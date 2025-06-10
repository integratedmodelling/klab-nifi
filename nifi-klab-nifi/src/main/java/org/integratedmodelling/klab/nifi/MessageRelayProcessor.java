package org.integratedmodelling.klab.nifi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.integratedmodelling.common.utils.Utils;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.runtime.Channel;
import org.integratedmodelling.klab.api.services.runtime.Message;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Receive messages from the currently configured scope and relay them as output. Should be
 * configurable with queue/message filters and scope IDs.
 */
@Tags({"k.LAB", "source", "event-driven"})
@CapabilityDescription("Generates FlowFiles when events are received from k.LAB Controller Service")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class MessageRelayProcessor extends AbstractProcessor {

  public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
      new PropertyDescriptor.Builder()
          .name("klab-controller-service")
          .displayName("k.LAB Controller Service")
          .description("The k.LAB Controller Service to receive events from")
          .required(true)
          .identifiesControllerService(KlabController.class)
          .build();

  public static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description("Successfully generated FlowFiles")
          .build();

  private static final List<PropertyDescriptor> DESCRIPTORS =
      Arrays.asList(KLAB_CONTROLLER_SERVICE);

  private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

  private final BlockingQueue<EventData> eventQueue = new LinkedBlockingQueue<>();
  private volatile boolean isRunning = false;

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return DESCRIPTORS;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    final KlabController controllerService =
        context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);

    isRunning = true;
    // Register as listener with the controller service
    controllerService.addEventListener(this::handleEvent);
  }

  @OnStopped
  public void onStopped(final ProcessContext context) {
    isRunning = false;
    final KlabController controllerService =
        context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);

    // Unregister listener
    controllerService.removeEventListener(this::handleEvent);
    eventQueue.clear();
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {

    if (!isRunning) {
      return;
    }

    // Poll for events with a timeout to avoid blocking indefinitely
    EventData eventData = eventQueue.poll();
    if (eventData == null) {
      // No events available, yield to other processors
      context.yield();
      return;
    }

    try {
      // Create FlowFile from event data
      FlowFile flowFile = session.create();
      flowFile =
          session.write(
              flowFile,
              out -> {
                // Write event data to FlowFile content
                writeEventToStream(eventData, out);
              });

      // Add attributes from event
      flowFile = session.putAllAttributes(flowFile, eventData.getAttributes());

      session.transfer(flowFile, REL_SUCCESS);
      session.commitAsync();

    } catch (Exception e) {
      getLogger().error("Failed to process event", e);
      session.rollback();
    }
  }

  private void handleEvent(EventData eventData) {
    if (isRunning) {
      try {
        eventQueue.offer(eventData, 1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        getLogger().warn("Interrupted while queuing event");
      }
    }
  }

  private void writeEventToStream(EventData eventData, OutputStream out) throws IOException {
    out.write(Utils.Json.printAsJson(eventData.getPayload()).getBytes());
  }
}

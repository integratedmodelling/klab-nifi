package org.integratedmodelling.klab.nifi;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.runtime.Channel;
import org.integratedmodelling.klab.api.services.runtime.Message;

import java.util.*;

/**
 * Receive messages from the currently configured scope and relay them as output. Should be
 * configurable with queue/message filters and scope IDs.
 */
@Tags({"k.LAB", "TODO"})
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("TODO")
public class MessageRelayProcessor extends AbstractProcessor {

  public static final PropertyDescriptor KLAB_SERVICE =
      new PropertyDescriptor.Builder()
          .name("k.LAB service controller")
          .description("k.LAB service controller")
          .identifiesControllerService(KlabController.class)
          .required(true)
          .build();

  public static final PropertyDescriptor SCOPE_ID =
      new PropertyDescriptor.Builder()
          .name("k.LAB scope ID")
          .description(
              "The ID of an existing and accessible scope. If not specified, the main user scope is used")
          .required(false)
          .build();

  public static final Relationship SUCCESS = new Relationship.Builder().name("success").build();

  public Set<Message.Queue> queues =
      EnumSet.of(Message.Queue.Events, Message.Queue.Errors, Message.Queue.Status);
  private String listenerId;
  private Scope scope;

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propDescs = new ArrayList<>();
    propDescs.add(KLAB_SERVICE);
    propDescs.add(SCOPE_ID);

    // TODO add properties to choose queues and filter messages

    return propDescs;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    var controller = context.getProperty(KLAB_SERVICE).asControllerService(KlabController.class);
    var scopeId = context.getProperty(SCOPE_ID).getValue();

    /*
    use the controller to find the scope from the configuration. If nothing is configured we use the main user scope.
     */
    Scope scope = controller.getScope(UserScope.class);
    if (scopeId != null) {
      // TODO find the scope
    }

    this.scope = scope;

    // TODO log, provenance

    /*
    install a listener to relay messages. Must be uninstalled on processor shutdown.
     */
    this.listenerId = scope.onMessage(this::processEvent, queues.toArray(new Message.Queue[0]));

    /*
    listener must filter messages based on configuration and output those that remain after filtering
     */

  }

  private void processEvent(Channel channel, Message message) {

  }

  @OnStopped
  public void onStopped(ProcessContext context) {
    // TODO see if we should set an AtomicBoolean flag to ignore all messages or remove the
    //  listener, to add it again when/if we are started again
  }

  @OnShutdown
  public void onShutdown() {
    /*
    remove the listener
     */
    this.scope.unregisterMessageListener(this.listenerId);
  }
}

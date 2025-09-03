package org.integratedmodelling.klab.nifi;

import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.integratedmodelling.klab.nifi.utils.KlabNifiInputRequest;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_URN;

/** An example processor that builds a valid flowfile. */
@Tags({"k.LAB", "source", "example"})
@CapabilityDescription("Generates FlowFiles when events are received from k.LAB Controller Service")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class KlabInputFromJavaExampleProcessor extends AbstractProcessor {

  public static final PropertyDescriptor KLAB_JAVA_INPUT_EXAMPLE =
      new PropertyDescriptor.Builder()
          .name("klab-java-input-example")
          .displayName("k.LAB Input example for Java")
          .description("This is an example on how to use the Java utilities.")
          .required(true)
          .identifiesControllerService(KlabController.class)
          .build();

  public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
      List.of(KLAB_JAVA_INPUT_EXAMPLE);

  public static final Relationship REL_FAILURE =
      new Relationship.Builder().description("Failed processing").name("failure").build();

  public static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description("Successfully generated FlowFiles")
          .build();

  private Set<Relationship> relationships;

  private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_FAILURE, REL_SUCCESS);

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return PROPERTY_DESCRIPTORS;
  }

  @Override
  public Set<Relationship> getRelationships() {
    return Set.of(REL_FAILURE, REL_SUCCESS);
    }

  @Override
  protected void init(final ProcessorInitializationContext context) {
    relationships = Set.of(REL_FAILURE, REL_SUCCESS);
  }

  private volatile boolean isRunning = false;

  @OnStopped
  public void onStopped(final ProcessContext context) {
    isRunning = false;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    final KlabController controllerService =
            context.getProperty(KLAB_JAVA_INPUT_EXAMPLE).asControllerService(KlabController.class);

    isRunning = true;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    if (!isRunning) {
      return;
    }

    var requestBuilder =
        new KlabNifiInputRequest.Builder()
            .setProjection("EPSG:4326")
            .setShape(
                "POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))")
            .setSgrid("1.km")
            .setTime(1325376000000L, 1356998400000L)
            .setScope(1.0, "year")
            .setName("testing")
            .setUrn("earth:Terrestrial earth:Region");
    try {
      FlowFile flowFile = session.create();
      flowFile =
          session.write(
              flowFile,
              out -> {
                // Write event data to FlowFile content
                out.write(new Gson().toJson(requestBuilder).getBytes());
              });

      // Add attributes from event
      session.putAttribute(flowFile, KLAB_URN, "earth:Terrestrial earth:Region");
      session.transfer(flowFile, REL_SUCCESS);
      session.commitAsync();

    } catch (Exception e) {
      getLogger().error("Failed to process event", e);
      session.rollback();
    }
  }
}

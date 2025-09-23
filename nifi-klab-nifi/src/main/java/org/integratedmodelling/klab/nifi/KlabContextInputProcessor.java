package org.integratedmodelling.klab.nifi;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_URN;

import com.google.gson.Gson;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.integratedmodelling.klab.nifi.utils.KlabNifiException;
import org.integratedmodelling.klab.nifi.utils.KlabObservationNifiRequest;

/** An example processor that builds a valid flowfile. */
@Tags({"k.LAB", "source", "example"})
@CapabilityDescription("Generates FlowFiles when events are received from k.LAB Controller Service")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class KlabContextInputProcessor extends AbstractProcessor {

  public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
      new PropertyDescriptor.Builder()
          .name("klab-controller-service")
          .displayName("k.LAB Controller Service")
          .description("This is an example on how to use the Java utilities.")
          .required(true)
          .identifiesControllerService(KlabController.class)
          .build();

  public static final PropertyDescriptor DIGITAL_TWIN_URL_PROPERTY =
          new PropertyDescriptor.Builder()
                  .name("URL")
                  .displayName("Digital Twin URL")
                  .description("The URL for the digital twin to connect to")
                  .required(false)
                  .addValidator(StandardValidators.URL_VALIDATOR)
                  .build();

  public static final PropertyDescriptor OBSERVATION_NAME =
          new PropertyDescriptor.Builder()
                  .name("observation-name")
                  .displayName("Name of the observation context.")
                  .description("The name of the observation context.")
                  .required(false)
                  .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                  .build();

  public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
      List.of(KLAB_CONTROLLER_SERVICE, DIGITAL_TWIN_URL_PROPERTY, OBSERVATION_NAME);

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
        context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);

    isRunning = true;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    if (!isRunning) {
      return;
    }

    KlabObservationNifiRequest.Builder requestBuilder = null;
    try {
    var builder = new KlabObservationNifiRequest.Builder();

    var space = new KlabObservationNifiRequest.Geometry.Space.Builder()
            .setProj("EPSG:4326")
            .setShape(
                    "POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))")
            .setGrid("1.km")
            .build();

    var time = new KlabObservationNifiRequest.Geometry.Time.Builder()
            .setTime(1325376000000L, 1356998400000L)
            .setTscope(1).setTunit("year")
            .build();

    var geometry = new KlabObservationNifiRequest.Geometry.Builder()
            .setSpace(space)
            .setTime(time)
            .build();

    String name = context.getProperty(OBSERVATION_NAME) == null || context.getProperty(OBSERVATION_NAME).getValue().isBlank()
            ? "testing-" + System.currentTimeMillis()
            : context.getProperty(OBSERVATION_NAME).getValue();

    requestBuilder = builder.setObservationName(name)
            .setObservationSemantics("earth:Terrestrial earth:Region")
            .setGeometry(geometry);

    if (!context.getProperty(DIGITAL_TWIN_URL_PROPERTY).getValue().isBlank()) {
      requestBuilder.setDigitalTwinUrl(context.getProperty(DIGITAL_TWIN_URL_PROPERTY).getValue());
    }

    } catch (KlabNifiException | MalformedURLException e) {
      throw new ProcessException(e);
    }

      try {
      FlowFile flowFile = session.create();
      KlabObservationNifiRequest request = requestBuilder.build();
      flowFile =
          session.write(
              flowFile,
              out -> {
                // Write event data to FlowFile content
                out.write(new Gson().toJson(request).getBytes());
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

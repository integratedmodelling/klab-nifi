package org.integratedmodelling.klab.nifi;

import com.google.gson.*;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.integratedmodelling.common.knowledge.ConceptImpl;
import org.integratedmodelling.common.knowledge.ObservableImpl;
import org.integratedmodelling.common.utils.Utils;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.collections.impl.MetadataImpl;
import org.integratedmodelling.klab.api.collections.impl.ParametersImpl;
import org.integratedmodelling.klab.api.data.Metadata;
import org.integratedmodelling.klab.api.geometry.Geometry;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.*;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.knowledge.observation.impl.ObservationImpl;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.services.runtime.Message;

/**
 * Submit observations (unresolved or resolved through adapter metadata) and output their
 * resolved/accepted version. Also output any related events through the `events` relationship.
 * Only works if the dataflow is tuned on a digital twin scope.
 *
 * <p>TODO configure to filter for observables, scope, geometry etc.
 */
@Tags({"k.LAB", "Observations"})
@CapabilityDescription(
    "Observation processor for the digital twin. Submitted unresolved observations "
        + "will be output as resolved; submitted resolved observations will be output as accepted, "
        + "or their already present observation will be output instead.")
public class ObservationRelayProcessor extends AbstractProcessor {

  public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
      new PropertyDescriptor.Builder()
          .name("klab-controller-service")
          .displayName("k.LAB Controller Service")
          .description("The k.LAB Controller Service providing the digital twin scope.")
          .required(true)
          .identifiesControllerService(KlabController.class)
          .build();

  public static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description("Successfully processed observations")
          .build();

  public static final Relationship REL_FAILURE =
      new Relationship.Builder()
          .name("failure")
          .description("Unresolved observations that failed validation or resolution")
          .build();

  public static final Relationship REL_EVENTS =
      new Relationship.Builder()
          .name("events")
          .description(
              "Events originating from the digital twin during observation resolution"
                  + " and contextualization")
          .build();

  public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
      List.of(KLAB_CONTROLLER_SERVICE);

  public static Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_EVENTS);

  private volatile KlabController klabController;
  private volatile ContextScope contextScope;
  private final Set<Consumer<EventData>> eventConsumers = new HashSet<>();
  private volatile boolean isRunning = false;

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return PROPERTY_DESCRIPTORS;
  }

  @OnScheduled
  /*
    The first call after the basic Validations are passed, it gets the Scope from the Controller Service
    it needs to interact and relay the observations as required
   */
  public void initializeScope(final ProcessContext context) {
    isRunning = true;
    klabController =
        context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);

    // Get the ContextScope from the controller
    contextScope = (ContextScope) klabController.getScope(ContextScope.class);
    if (contextScope == null) {
      getLogger().error("No ContextScope available from the KlabController");
    }
  }


  @OnStopped
  public void cleanup() {
    isRunning = false;
    eventConsumers.clear();
  }

  class ObservableTypeAdapter implements JsonDeserializer<Observable> {
    @Override
    public Observable deserialize(
        JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      return context.deserialize(jsonObject, ObservableImpl.class);
    }
  }

  class MetadataTypeAdapter implements JsonDeserializer<Metadata> {
    @Override
    public Metadata deserialize(
            JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      return context.deserialize(jsonObject, MetadataImpl.class);
    }
  }

  class ConceptTypeAdapter implements JsonDeserializer<Concept> {
    @Override
    public Concept deserialize(
            JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      return context.deserialize(jsonObject, ConceptImpl.class);
    }
  }

  class GeometryTypeAdapter implements JsonDeserializer<Geometry> {
    @Override
    public Geometry deserialize(
            JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      return context.deserialize(jsonObject, GeometryImpl.class);
    }
  }

  class ParametersTypeAdapter implements JsonDeserializer<Parameters> {
    @Override
    public Parameters deserialize(
            JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      return context.deserialize(jsonObject, ParametersImpl.class);
    }
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    if (!isRunning || contextScope == null) {
      context.yield();
      return;
    }

//      FlowFile flowFile = session.create();

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      getLogger().error("Flowfile is null.. :)))");
      return;
    }

    AtomicReference<ObservationImpl> observationRef = new AtomicReference<>();

    final GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Observable.class, new ObservableTypeAdapter());
    builder.registerTypeAdapter(Metadata.class, new MetadataTypeAdapter());
    builder.registerTypeAdapter(Concept.class, new ConceptTypeAdapter());
    builder.registerTypeAdapter(Geometry.class, new GeometryTypeAdapter());
    Gson gson = builder.create();
    try {
      session.read(flowFile, in -> {
        ObservationImpl obs = gson.fromJson(new InputStreamReader(in, StandardCharsets.UTF_8), ObservationImpl.class);
        observationRef.set(obs);
        getLogger().info("Read some observation...");
      });
    } catch (Exception e) {
      getLogger().error("Error reading observation from FlowFile", e);
      session.transfer(flowFile, REL_FAILURE);
      return;
    }

    Observation observation = observationRef.get();

    if (observation == null) {
      getLogger().error("Found the observation to be null...");
      session.transfer(flowFile, REL_FAILURE);
      return;
    }

    try {

            // Submit the observation to the context scope
            var future =
                contextScope
                    .submit(observation)
                    .exceptionally(
                        throwable -> {

                          // Transfer to failure if resolution returned null
                          getLogger().error(throwable.getMessage());
                          session.transfer(flowFile, REL_FAILURE);
                          // TODO remove listener
                          return observation;
                        })
                    .thenApply(
                        resolvedObservation -> {
                          FlowFile successFlowFile = session.create(flowFile);
                          successFlowFile =
                              session.write(
                                  successFlowFile,
                                  out -> {
                                    try {
                                      // Serialize the resolved observation to the output stream
                                      serializeObservation(resolvedObservation, out);
                                    } catch (IOException e) {
                                      getLogger()
                                          .error(
                                              "Failed to write resolved observation to FlowFile",
                                              e);
                                      throw new ProcessException("Failed to write observation", e);
                                    }
                                  });

                          // Add relevant attributes
                          Map<String, String> attributes = new HashMap<>();
                          // TODO
                          attributes.put("observation.id", resolvedObservation.getId() + "");
                          attributes.put(
                              "observation.type", resolvedObservation.getType().toString());
                          successFlowFile = session.putAllAttributes(successFlowFile, attributes);

                          getLogger().info("Success flowfile being sent");
                          // Transfer to success relationship
                          session.transfer(successFlowFile, REL_SUCCESS);
                          session.remove(flowFile);
                          // TODO remove listener
                          return resolvedObservation;
                        });
    } catch (Exception e) {
      getLogger().error("Error processing observation", e);
      session.transfer(flowFile, REL_FAILURE);
    }
  }

  private void handleEventData(EventData eventData, ProcessSession session) {

    if (!isRunning) {
      return;
    }

    // Check if this is a Message event that should be routed to REL_EVENTS
    if (eventData.getPayload() instanceof Message) {
      try {

        // Create a FlowFile for the event
        FlowFile eventFlowFile = session.create();
        eventFlowFile =
            session.write(
                eventFlowFile,
                out -> {
                  try {
                    // Serialize the event payload
                    serializeEventPayload(eventData.getPayload(), out);
                  } catch (IOException e) {
                    getLogger().error("Failed to write event to FlowFile", e);
                    throw new ProcessException("Failed to write event", e);
                  }
                });

        // Add attributes from the event
        eventFlowFile = session.putAllAttributes(eventFlowFile, eventData.getAttributes());

        // Transfer to events relationship
        session.transfer(eventFlowFile, REL_EVENTS);
        session.commit();
      } catch (Exception e) {
        getLogger().error("Error creating event FlowFile", e);
      }
    }
  }

  private void serializeObservation(Observation observation, OutputStream out) throws IOException {
    // Implementation would depend on your serialization library
    // This is a placeholder - you'd use a proper JSON/serialization library
    String json = Utils.Json.asString(observation);
    out.write(json.getBytes());
  }

  private void serializeEventPayload(Object payload, OutputStream out) throws IOException {
    // Implementation would depend on your serialization library
    // This is a placeholder - you'd use a proper JSON/serialization library
    String json = Utils.Json.asString(payload);
    out.write(json.getBytes());
  }
}

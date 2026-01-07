package org.integratedmodelling.klab.nifi;

import com.google.gson.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.integratedmodelling.common.utils.Utils;
import org.integratedmodelling.klab.api.collections.Parameters;
import org.integratedmodelling.klab.api.collections.impl.ParametersImpl;
import org.integratedmodelling.klab.api.digitaltwin.DigitalTwin;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.knowledge.observation.impl.ObservationImpl;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.Reasoner;
import org.integratedmodelling.klab.api.services.RuntimeService;
import org.integratedmodelling.klab.api.services.runtime.Message;
import org.integratedmodelling.klab.nifi.utils.KlabObservationNifiRequest;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_CONTEXTUALIZER_TYPE_KEY;

@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@InputRequirement(
    InputRequirement.Requirement.INPUT_REQUIRED) // This shouldn't be the first processor
@CapabilityDescription(
    "Parses incoming Flowfiles and creates an Observation Obj"
        + "Also is responsible for some pre validation steps"
        + "Observation processor for the digital twin. Submitted unresolved observations "
        + "will be output as resolved; submitted resolved observations will be output as accepted, "
        + "or their already present observation will be output instead."
        + "This would work with multiple Digital Twins in a Federation for Collaborative Working on a Digital Twin")
@WritesAttributes({
  @WritesAttribute(
      attribute = "observation.id",
      description = "Writes the Id of the Observation made"),
  @WritesAttribute(
      attribute = "observation.type",
      description = "Writes the type of the Observation made"),
  @WritesAttribute(
          attribute = "observation.urn",
          description = "Writes the URN of the observation once resolved by k.LAB"
  ),
  @WritesAttribute(
          attribute = "digital.twin.url",
          description = "Writes the URL of the Digital Twin to which the Resolved Observation" +
                  "is added after Resolution")
})
public class KlabObservationWithDT extends AbstractProcessor {

  public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
      new PropertyDescriptor.Builder()
          .name("klab-controller-service")
          .displayName("k.LAB Controller Service")
          .description(
              "The k.LAB Federation Controller Service for the User Scope at the Federation Level")
          .required(true)
          .identifiesControllerService(KlabController.class)
          .build();

  public static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description("Successfully Resolved Observation")
          .build();

  public static final Relationship REL_FAILURE =
      new Relationship.Builder()
          .name("failure")
          .description("Observation Resolution Failed")
          .build();

  private List<PropertyDescriptor> descriptors;
  private Set<Relationship> relationships;
  private volatile KlabController klabController;
  private volatile UserScope userScope;
  private volatile boolean isRunning = false;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    descriptors = List.of(KLAB_CONTROLLER_SERVICE);
    relationships = Set.of(REL_SUCCESS, REL_FAILURE);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    isRunning = true;
    klabController =
        context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);
    userScope = (UserScope) klabController.getScope(UserScope.class);
    if (userScope == null) {
      getLogger()
          .error("No UserScope available from the KlabController, Authentication failed possibly");
    }
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    FlowFile flowfile = session.get();
    if (flowfile == null) {
      getLogger().error("Incoming flowfile to the processor is null :(");
      return;
    }

    if (!isRunning || userScope == null) {
      getLogger().error("Problems in Authenticating with the Certificate");
      context.yield();
      session.transfer(flowfile, REL_FAILURE);
      return;
    }

    final GsonBuilder builder = new GsonBuilder();
    AtomicReference<KlabObservationNifiRequest> req = new AtomicReference<>();

    Gson gson = builder.create(); // Read JSON directly from FlowFile input stream
    session.read(
        flowfile,
        in -> {
          try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            req.set(gson.fromJson(reader, KlabObservationNifiRequest.class));

          } catch (Exception e) {
            getLogger().error("Error reading JSON", e);
          }
        });

    getLogger().info("Payload parsing done...");

    String dtURL = req.get().getDigitalTwin();
    if (dtURL.isEmpty()) {
      getLogger().error("The Flowfile doesn't have the DT URL");
      session.transfer(flowfile, REL_FAILURE);
      return;
    }

    if (req.get().getResetContext()) {
      getLogger().info("Resetting the Context, another Context Observation would be required to be made");
      klabController.removeScope(dtURL);
      session.transfer(flowfile, REL_SUCCESS);
      return;
    }

    ContextScope contextScope = (ContextScope) klabController.getScope(dtURL, ContextScope.class);
    if (contextScope == null) {
      getLogger().info("No ContextScope available from the KlabController for the DT " + dtURL);
      contextScope = userScope.connect(Utils.URLs.newURL(dtURL));
      if (contextScope == null) {
        getLogger().error("Unable to connect to the Digital Twin " + dtURL);
        session.transfer(flowfile, REL_FAILURE);
        return;
      }
    } else {
      getLogger().info("Found the Context Scope for the k.LAB Controller");
    }


    // The Observable from the Semantics URN with the Reasoner Client
    Observable observable =
        contextScope
            .getService(Reasoner.class)
            .resolveObservable(req.get().getObservationSemantics());

    Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();

    System.out.println(prettyGson.toJson(observable));
    System.out.println("Observable Generated..");

    ObservationImpl obs = null;

    if (req.get().getAsContext()) {
      /*
        Setting Name and Geometry (Time & Space) when it's a context observation,
        else just pass the semantics
       */
      var geometry =
              GeometryImpl.builder()
                      .space()
                      .shape(req.get().getGeometry().getSpace().getShape())
                      .resolution(req.get().getGeometry().getSpace().getSgrid())
                      .projection(req.get().getGeometry().getSpace().getProj())
                      .build()
                      .time()
                      .between(
                              req.get().getGeometry().getTime().getTstart(),
                              req.get().getGeometry().getTime().getTend())
                      .resolution(Time.Resolution.Type.YEAR, req.get().getGeometry().getTime().getTscope())
                      .build();

      obs = DigitalTwin.createObservation(contextScope, observable, geometry.build(), req.get().getObservationName());
    } else {
      obs = DigitalTwin.createObservation(contextScope, observable);
    }


    ObservationImpl.ContextualizationDataImpl ctxData = getContextualizationData(
            contextScope,
            req.get().getContextualizer());

    if (!req.get().getAsContext()) {
      obs.setContextualizationData(ctxData);
    }

    obs.setId(Observation.UNASSIGNED_ID);
    getLogger().info("Observation Payload Generation done, submitting the Observation");

    // Convert the object to a pretty-printed JSON string
    System.out.println(prettyGson.toJson(obs));

    AtomicReference<ObservationImpl> observationRef = new AtomicReference<>();
    observationRef.set(obs);
    Observation observation = observationRef.get();
    FlowFile successFlowFile = session.create();

    try {
      CompletableFuture<Observation> future = contextScope.submit(observation);
      Observation resolvedObservation = future.get();
      Map<String, String> attributes = new HashMap<>();
      attributes.put("observation.id", resolvedObservation.getId() + "");
      attributes.put("observation.type", resolvedObservation.getType().toString());
      attributes.put("observation.urn", resolvedObservation.getUrn());
      attributes.put("digital.twin.url", dtURL);

      System.out.println("Observation ID: " + resolvedObservation.getId() + "\n"
              + "Observation Name: " + resolvedObservation.getName() + "\n"
              + resolvedObservation.getObservable() + "\n"
              + "Observation URN: " + resolvedObservation.getUrn() + "\n"
              + "Observation Type: " + resolvedObservation.getType().toString());

      System.out.println(prettyGson.toJson(resolvedObservation));

      // If the ID is -1, the Resolution Process failed
      if (resolvedObservation.getId() == -1) {
        getLogger().info("The submitted Observation couldn't be resolved");
        contextScope.send(
                Message.MessageClass.DigitalTwin,
                Message.MessageType.Error,
                resolvedObservation);
        throw new Exception("The Submitted Observation couldn't be resolved");
      }
      /*
        If asContext parameter has been set, and the observation submitted has
        a Semantics of earth:Terrestrial earth:Region and should
         have a time and space (Validated in the client)
        then the context observation would be marked as context, and
        the future observations would be made in that context
      */
      ContextScope ctxS = null;
      if (req.get().getAsContext()){
        getLogger().info("Setting the Context Observation with id: "
                + resolvedObservation.getId()
                + " as the Context for future Observation(s)");
        ctxS = contextScope.within(resolvedObservation);
        klabController.addScope(dtURL, ctxS);

        contextScope.send(
                Message.MessageClass.DigitalTwin,
                Message.MessageType.ContextObservationResolved,
                resolvedObservation);
      }

      getLogger().info("Sending Messages of Observation Submission Finished for the Digital Twin..");
      contextScope.send(
              Message.MessageClass.DigitalTwin,
              Message.MessageType.ObservationSubmissionFinished,
              resolvedObservation
      );
      successFlowFile = session.putAllAttributes(successFlowFile, attributes);
      ContextScope ctxScope = (ContextScope) klabController.getScope(dtURL, ContextScope.class);
      getLogger().info("Success Flowfile being sent to Success Relation..");
      session.remove(flowfile);
      session.transfer(successFlowFile, REL_SUCCESS);
    } catch (Exception e) {
      getLogger().error("Error in processing Observation: ", e);
      getLogger().info("Routing Success Flowfile to Failure Rel");
      session.remove(successFlowFile);
      session.transfer(flowfile, REL_FAILURE);
    }
  }


  private static ObservationImpl.ContextualizationDataImpl getContextualizationData(ContextScope scope, Map<String, Object> params) {

    if (params == null) {
      return null;
    }
    var ctxData = new ObservationImpl.ContextualizationDataImpl();
    ctxData.setAdapterId((String) params.get(KLAB_CONTEXTUALIZER_TYPE_KEY));
    // Would always be castable, guaranteed by the client
    ctxData.setServiceId(scope.getService(RuntimeService.class).serviceId());
    ctxData.setServiceUrl(scope.getService(RuntimeService.class).getUrl());
    ParametersImpl<String> ctxParams = new ParametersImpl<>();
    for(Map.Entry<String, Object> entry : params.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (!(key.equals(KLAB_CONTEXTUALIZER_TYPE_KEY))) {
        if (value instanceof Number ) {
          ctxParams.put(key, ((Number) value).intValue());
        } else {
          ctxParams.put(key, String.valueOf(value));
        }
      }
    }
    ctxData.setParameters(ctxParams);
    return ctxData;
  }
}

package org.integratedmodelling.klab.nifi;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_UNRESOLVED_OBS_ID;

import com.google.gson.*;
import java.io.*;
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
import org.integratedmodelling.klab.api.digitaltwin.DigitalTwin;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.knowledge.observation.Observation;
import org.integratedmodelling.klab.api.knowledge.observation.impl.ObservationImpl;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.Reasoner;
import org.integratedmodelling.klab.nifi.utils.KlabObservationNifiRequest;

@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED) // This shouldn't be the first processor

@CapabilityDescription( "Parses incoming Flowfiles and creates an Observation Obj"
        + "Also is responsible for some pre validation steps"
        + "Observation processor for the digital twin. Submitted unresolved observations "
        + "will be output as resolved; submitted resolved observations will be output as accepted, "
        + "or their already present observation will be output instead.")

@WritesAttributes(
        {
                @WritesAttribute(attribute="observation.id", description="Writes the Id of the Observation made"),
                @WritesAttribute(attribute = "observation.type", description = "Writes the type of the Observation made") })


public class KlabObservationWithDT extends AbstractProcessor {

    public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =   new PropertyDescriptor.Builder()
            .name("klab-controller-service")
            .displayName("k.LAB Controller Service")
            .description("The k.LAB Controller Service to receive events from")
            .required(true)
            .identifiesControllerService(KlabFederationControllerService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully Resolved Observation")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
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
        klabController = context.getProperty(KLAB_CONTROLLER_SERVICE)
                .asControllerService(KlabFederationControllerService.class);
        userScope = (UserScope) klabController.getScope(UserScope.class);

        if (userScope == null) {
            getLogger().error("No UserScope available from the KlabController, Authentication failed possibly");
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        if (!isRunning || userScope == null) {
            getLogger().error("Problems in Authenticating with the Certificate");
            context.yield();
            return;
        }

        FlowFile flowfile = session.get();
        if (flowfile == null){
            getLogger().error("Incoming flowfile to the processor is null :(");
            return;
        }

        final GsonBuilder builder = new GsonBuilder();
        AtomicReference<KlabObservationNifiRequest> req = new AtomicReference<>();

        Gson gson = builder.create(); // Read JSON directly from FlowFile input stream
        session.read(flowfile, in -> {
            try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                req.set(gson.fromJson(reader, KlabObservationNifiRequest.class));

            } catch (Exception e) {
                getLogger().error("Error reading JSON", e);
            }
        });

        getLogger().info("Payload parsing done...");

        String dtURL = req.get().getDigitalTwinUrl();
        if (dtURL.isEmpty()) {
            getLogger().error("The Flowfile doesn't have the DT URL");
            session.transfer(flowfile, REL_FAILURE);
            return;
        }

        ContextScope contextScope = (ContextScope) klabController.getScope(dtURL, ContextScope.class);
        if (contextScope == null){
            getLogger().info("No ContextScope available from the KlabController for the DT " + dtURL);
            contextScope = userScope.connect(Utils.URLs.newURL(dtURL));
            if (contextScope == null){
                getLogger().error("Unable to connect to the Digital Twin " + dtURL);
                session.transfer(flowfile, REL_FAILURE);
                return;
            } 
            klabController.addScope(dtURL, contextScope); // Add the newly created ContextScope to the KlabController Map
            getLogger().info("Fetched Context Scope successfully from DT: " + dtURL);
        }

        // The Observable from the Semantics URN with the Reasoner Client
        Observable observable = contextScope.getService(Reasoner.class).resolveObservable(
                req.get().getObservationSemantics()
        );

        Gson prettyGson = new GsonBuilder()
                .setPrettyPrinting()
                .create();

        System.out.println(prettyGson.toJson(observable));
        System.out.println("Observable Generated..");

        var geometry = GeometryImpl.builder()
                .space()
                .shape(req.get().getGeometry().getSpace().getShape())
                .resolution(req.get().getGeometry().getSpace().getSgrid())
                .projection(req.get().getGeometry().getSpace().getProj())
                .build()
                .time()
                .between(req.get().getGeometry().getTime().getTstart(),
                        req.get().getGeometry().getTime().getTend())
                .resolution(Time.Resolution.Type.YEAR,
                        req.get().getGeometry().getTime().getTscope())
                .build();

        ObservationImpl obs = DigitalTwin.createObservation(contextScope, observable);
        obs.setGeometry(geometry.build());
        obs.setName(req.get().getObservationName());
        obs.setUrn(req.get().getObservationSemantics());
        obs.setId(KLAB_UNRESOLVED_OBS_ID); // Unresolved Observation ID is -1
        getLogger().info("Observation Payload Generation done, submitting the Observation");


        // Convert the object to a pretty-printed JSON string
        System.out.println(prettyGson.toJson(obs));

        AtomicReference<ObservationImpl> observationRef = new AtomicReference<>();
        observationRef.set(obs);
        Observation observation = observationRef.get();
        try {
            CompletableFuture<Observation> future = contextScope.submit(observation);
            Observation resolvedObservation = future.get();
            FlowFile successFlowFile = session.create();
            Map<String, String> attributes = new HashMap<>();
            attributes.put("observation.id", resolvedObservation.getId() + "");
            attributes.put("observation.type", resolvedObservation.getType().toString());
            successFlowFile = session.putAllAttributes(successFlowFile, attributes);
            getLogger().info("Success Flowfile being sent to Success Relation..");
            session.remove(flowfile);
            session.transfer(successFlowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Error in processing Observation: ", e);
            getLogger().info("Routing Success Flowfile to Failure Rel");
            session.transfer(flowfile, REL_FAILURE);
        }
    }
}

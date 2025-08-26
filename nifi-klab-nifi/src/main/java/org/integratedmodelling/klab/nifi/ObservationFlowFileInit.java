package org.integratedmodelling.klab.nifi;


import com.google.gson.Gson;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.integratedmodelling.common.knowledge.ObservableImpl;
import org.integratedmodelling.klab.api.digitaltwin.DigitalTwin;
import org.integratedmodelling.klab.api.knowledge.*;
import org.integratedmodelling.klab.api.knowledge.observation.impl.ObservationImpl;
import org.integratedmodelling.klab.api.scope.ContextScope;

@Tags({"k.LAB", "source", "event-driven"})
@CapabilityDescription("Generates FlowFiles for the Observation Relay Processor")

/*
    Just creates the flow file for the observations relay processor
 */

public class ObservationFlowFileInit extends AbstractProcessor {


    public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
            new PropertyDescriptor.Builder()
                    .name("klab-controller-service")
                    .displayName("k.LAB Controller Service")
                    .description("The k.LAB Controller Service providing the digital twin scope.")
                    .required(true)
                    .identifiesControllerService(KlabController.class)
                    .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("On Success from the Processor")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("On Failure from the Processor")
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            List.of(KLAB_CONTROLLER_SERVICE);

    public static Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private volatile KlabController klabController;
    private volatile ContextScope contextScope;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }


    @OnScheduled
  /*
    The first call after the basic Validations are passed, it gets the Scope from the Controller Service
    it needs to interact and relay the observations as required
   */
    public void initializeScope(final ProcessContext context) {
        klabController =
                context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);

        // Get the ContextScope from the controller
        contextScope = (ContextScope) klabController.getScope(ContextScope.class);
        if (contextScope == null) {
            getLogger().error("No ContextScope available from the KlabController");
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create(); // create a flow file..
        if (flowFile == null) {
            return;
        }

        if (contextScope == null) {
            getLogger().error("Context Scope from k.LAB Controller Service is null");
            return;
        }

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) {
                String content = "Hello from custom processor!";
                String jsonStr = "{\"status\":\"ok\", \"message\":\"Hello\"}";
                ObservationImpl obs = DigitalTwin.createObservation(
                        contextScope,
                        new ObservableImpl()
                );
                Gson gson = new Gson();
                String json = gson.toJson(obs);

                try {
                    out.write(json.getBytes(StandardCharsets.UTF_8));
                    getLogger().info("Wrote some stuff in the Nifi flowfile to push an observation...");
                } catch (Exception e) {
                    throw new ProcessException("Error writing content", e);
                }
            }
        });

        // Transfer FlowFile to success
        session.transfer(flowFile, REL_SUCCESS);
    }
}


package org.integratedmodelling.klab.nifi;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_SEMANTIC_TYPES;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.integratedmodelling.common.knowledge.ConceptImpl;
import org.integratedmodelling.klab.api.digitaltwin.DigitalTwin;
import org.integratedmodelling.klab.api.geometry.impl.GeometryImpl;
import org.integratedmodelling.klab.api.knowledge.*;
import org.integratedmodelling.klab.api.knowledge.observation.impl.ObservationImpl;
import org.integratedmodelling.klab.api.knowledge.observation.scale.time.Time;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.nifi.utils.KlabNifiInputRequest;

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

  public static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description("On Success from the Processor")
          .build();

  public static final Relationship REL_FAILURE =
      new Relationship.Builder()
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
    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    if (contextScope == null) {
      getLogger().error("Context Scope from k.LAB Controller Service is null");
      return;
    }

    KlabNifiInputRequest request = null;
    try (final InputStream in = session.read(flowFile);
        final InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
      var json = JsonParser.parseReader(reader);
      request =
          new Gson().fromJson(json.getAsJsonObject().getAsString(), KlabNifiInputRequest.class);
    } catch (final IOException e) {
      getLogger().error("Failed to read FlowFile content due to {}", new Object[] {e}, e);
    }

    // TODO get request and build valid JSON
    String semanticTypes = flowFile.getAttribute(KLAB_SEMANTIC_TYPES);
    getLogger().warn("SEMANTIC TYPES: " + semanticTypes);
    flowFile =
        session.write(
            flowFile,
            new OutputStreamCallback() {
              @Override
              public void process(OutputStream out) {

                ConceptImpl ccpt = new ConceptImpl();
                ccpt.setUrn("earth:Terrestrial earth:Region");
                ccpt.setId(-1);

                ObservationImpl obs =
                    //DigitalTwin.createObservation(contextScope, new ObservableImpl(ccpt));
                  DigitalTwin.createObservation(contextScope);

                var geom =
                    GeometryImpl.builder()
                        .space()
                        .shape(
                            "EPSG:4326 POLYGON((33.796 -7.086, 35.946 -7.086, 35.946 -9.41, 33.796 -9.41, 33.796 -7.086))")
                        .resolution("1.km")
                        .projection("EPSG:4326")
                        .build()
                        .time()
                        .between(1325376000000L, 1356998400000L)
                        .resolution(Time.Resolution.Type.YEAR, 1)
                        .build();

                obs.setGeometry(geom.build());
                obs.setUrn("earth:Terrestrial earth:Region");

                Gson gson = new Gson();
                String json = gson.toJson(obs);
                try {
                  out.write(json.getBytes(StandardCharsets.UTF_8));
                  getLogger()
                      .info("Wrote some stuff in the Nifi flowfile to push an observation...");
                } catch (Exception e) {
                  throw new ProcessException("Error writing content", e);
                }
              }
            });

    // Transfer FlowFile to success
    session.transfer(flowFile, REL_SUCCESS);
  }
}

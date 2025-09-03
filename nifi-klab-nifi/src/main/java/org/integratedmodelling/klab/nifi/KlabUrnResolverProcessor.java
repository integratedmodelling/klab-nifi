package org.integratedmodelling.klab.nifi;

import com.google.gson.*;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.integratedmodelling.common.knowledge.ObservableImpl;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.services.ResourcesService;
import org.integratedmodelling.klab.api.services.Reasoner;

@ReadsAttributes({
  @ReadsAttribute(
      attribute = "urn",
      description = "URN that references a valid k.LAB semantic concept.")
})
public class KlabUrnResolverProcessor extends AbstractProcessor {

  //  // TODO make it use a UserScope
  //  private volatile UserScope userScope;
  private volatile ContextScope contextScope;
  private volatile KlabController klabController;

  public static final PropertyDescriptor KLAB_CONTROLLER_SERVICE =
      new PropertyDescriptor.Builder()
          .name("klab-controller-service")
          .displayName("k.LAB Controller Service")
          .description("The k.LAB Controller Service providing the digital twin scope.")
          .required(true)
          .identifiesControllerService(KlabController.class)
          .build();

  public static final PropertyDescriptor PROPERTY_KLAB_URN =
      new PropertyDescriptor.Builder()
          .name("URN")
          .displayName("k.LAB URN")
          .description("URN that references a valid k.LAB semantic concept")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
      List.of(KLAB_CONTROLLER_SERVICE, PROPERTY_KLAB_URN);

  public static Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return PROPERTY_DESCRIPTORS;
  }

  @OnScheduled
  public void initializeScope(final ProcessContext context) {
    klabController =
        context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);

    // Get the ContextScope from the controller
    contextScope = (ContextScope) klabController.getScope(ContextScope.class);
    if (contextScope == null) {
      getLogger().error("No ContextScope available from the KlabController");
    }
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

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    String urn = context.getProperty(PROPERTY_KLAB_URN).getValue();
    getLogger().warn("URN" + urn);

    contextScope = (ContextScope) klabController.getScope(ContextScope.class);
    var solved = contextScope.getService(ResourcesService.class).resolve(urn, contextScope);
    var solved_reason = contextScope.getService(Reasoner.class).resolveConcept(urn);
    var solved_observable = contextScope.getService(Reasoner.class).resolveObservable(urn);

    getLogger().info("Solved Concept"  + solved_reason.getUrn());
    getLogger().info("Solved Observable" + solved_observable.getUrn());
    final GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Observable.class, new ObservableTypeAdapter());

    Gson gson = builder.create();
    getLogger().warn(solved.toString());
    var observable = solved.getResults().stream().findFirst().orElseThrow(() -> new ProcessException("Cannot resolve the provided URN."));

    FlowFile flowFile = session.create();


    flowFile =
        session.write(
            flowFile,
            new OutputStreamCallback() {
              @Override
              public void process(OutputStream out) {
                String asJson = gson.toJson(solved_observable);
                try {
                  out.write(asJson.getBytes(StandardCharsets.UTF_8));
                  getLogger().info("Writing observable to the flowfile.");
                } catch (Exception e) {
                  throw new ProcessException("Error writing content", e);
                }
              }
            });

    session.transfer(flowFile, REL_SUCCESS);
    session.commit();
  }
}

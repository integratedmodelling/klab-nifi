package org.integratedmodelling.klab.nifi;

import com.google.gson.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.integratedmodelling.common.knowledge.ObservableImpl;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.services.Reasoner;
import org.integratedmodelling.klab.api.services.ResourcesService;

@ReadsAttributes({
  @ReadsAttribute(
      attribute = "urn",
      description = "URN that references a valid k.LAB semantic concept.")
})
public class KlabUrnResolverProcessor extends AbstractProcessor {
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

  public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
          List.of(KLAB_CONTROLLER_SERVICE);

  @Override
  public Set<Relationship> getRelationships() {
    return relationships;
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return PROPERTY_DESCRIPTORS;
  }

  public static final Relationship REL_SUCCESS = new Relationship.Builder()
          .name("success")
          .description("Successfully Resolved k.LAB URN")
          .build();

  public static final Relationship REL_FAILURE = new Relationship.Builder()
          .name("failure")
          .description("Resolution of k.LAB URN Failed")
          .build();

  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    relationships = Set.of(REL_SUCCESS, REL_FAILURE);
  }

  @OnScheduled
  public void initializeScope(final ProcessContext context) {
    klabController = context.getProperty(KLAB_CONTROLLER_SERVICE).asControllerService(KlabController.class);

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
    String urn = null;
    var flowFile = session.get();

    JsonObject jsonObject = null;
    try (final InputStream in = session.read(flowFile);
        final InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
      final JsonElement root = JsonParser.parseReader(reader);

      if (root.isJsonObject()) {
        jsonObject = root.getAsJsonObject();
        if (jsonObject.has("urn")) {
          final JsonElement element = jsonObject.get("urn");
          urn = element.getAsString();
        } else {
          getLogger().warn("JSON key not found in FlowFile");
        }
      }
    } catch (final IOException e) {
      getLogger().error("Failed to read FlowFile content due to {}", new Object[] {e}, e);
    }

    if (urn == null) {
      // TODO thrown an exception
      return;
    }

    contextScope = (ContextScope) klabController.getScope(ContextScope.class);
    var solved = contextScope.getService(ResourcesService.class).resolve(urn, contextScope);
    var solved_reason = contextScope.getService(Reasoner.class).resolveConcept(urn);
    var solved_observable = contextScope.getService(Reasoner.class).resolveObservable(urn);

    getLogger().info("Solved Observable" + solved_observable.getUrn());
    final GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Observable.class, new ObservableTypeAdapter());

    Gson gson = builder.create();
    JsonObject finalJsonObject = jsonObject;
    flowFile =
        session.write(
            flowFile,
            out -> {
              finalJsonObject.add("observable", gson.toJsonTree(solved_observable));
              try {
                out.write(finalJsonObject.getAsString().getBytes(StandardCharsets.UTF_8));
                getLogger().info("Writing observable to the flowfile.");
              } catch (Exception e) {
                throw new ProcessException("Error writing content", e);
              }
            });

    session.transfer(flowFile, REL_SUCCESS);
    session.commit();
  }
}

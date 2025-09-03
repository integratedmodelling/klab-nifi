package org.integratedmodelling.klab.nifi;

import com.google.gson.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.integratedmodelling.common.knowledge.ObservableImpl;
import org.integratedmodelling.klab.api.knowledge.KlabAsset;
import org.integratedmodelling.klab.api.knowledge.Observable;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.services.ResourcesService;

import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_SEMANTIC_TYPES;
import static org.integratedmodelling.klab.nifi.utils.KlabAttributes.KLAB_URN;

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
    String urn = null;
    var flowFile = session.get();
    try (final InputStream in = session.read(flowFile);
        final InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
      final JsonElement root = JsonParser.parseReader(reader);

      if (root.isJsonObject()) {
        final JsonObject jsonObject = root.getAsJsonObject();
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

    var observable =
        solved.getResults().stream()
            .filter(
                res -> {
                  return res.getKnowledgeClass().equals(KlabAsset.KnowledgeClass.OBSERVABLE);
                })
            .findFirst()
            .orElseThrow(() -> new ProcessException("Cannot resolve the provided URN."));

    var concept = contextScope.getService(ResourcesService.class).retrieveConcept(urn);
    var semanticTypes = concept.getType();
    var attributes = Map.of(KLAB_SEMANTIC_TYPES, semanticTypes.toString());

    final GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Observable.class, new ObservableTypeAdapter());
    Gson gson = builder.create();
    // We send back the same flowfile plus the attributes
    session.putAllAttributes(flowFile, attributes);
    session.transfer(flowFile, REL_SUCCESS);
    session.commit();
  }
}

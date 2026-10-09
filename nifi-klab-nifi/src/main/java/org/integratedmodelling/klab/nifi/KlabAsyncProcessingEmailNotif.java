package org.integratedmodelling.klab.nifi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.integratedmodelling.common.authentication.KlabCertificateImpl;
import org.integratedmodelling.common.configuration.CommonConfiguration;
import org.integratedmodelling.common.services.client.engine.EngineImpl;
import org.integratedmodelling.klab.api.Klab;
import org.integratedmodelling.klab.api.scope.ContextScope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.nifi.utils.KlabAsyncProcessingEmailNotifRequest;
import org.integratedmodelling.klab.nifi.utils.KlabRDMTrainingPointRequest;
import org.integratedmodelling.klab.services.base.BaseService;
import org.integratedmodelling.klab.services.base.EmailManager;
import org.integratedmodelling.klab.services.scopes.ServiceUserScope;

import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;


@Tags({"k.LAB", "WEED", "AI", "Semantic Web", "Digital Twins"})
@InputRequirement(
        InputRequirement.Requirement.INPUT_REQUIRED)

public class KlabAsyncProcessingEmailNotif extends AbstractProcessor {

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
        final GsonBuilder builder = new GsonBuilder();

        AtomicReference<KlabAsyncProcessingEmailNotifRequest> req = new AtomicReference<>();

        Gson gson = builder.create(); // Read JSON directly from FlowFile input stream
        session.read(
                flowfile,
                in -> {
                    try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                        req.set(gson.fromJson(reader, KlabAsyncProcessingEmailNotifRequest.class));

                    } catch (Exception e) {
                        getLogger().error("Error reading JSON", e);
                    }
                });


        String recipient = req.get().getRecipient();
        String subject = req.get().getSubject();
        String body = req.get().getBody();
        String dtURL = req.get().getDTURL();

        ContextScope contextScope = (ContextScope) klabController.getScope(String.valueOf(dtURL), ContextScope.class);
        EmailManager manager = null;

        if (contextScope instanceof ServiceUserScope serviceScope
                && serviceScope.getService() instanceof BaseService service) {
            manager = service.getEmailManager();
        }

        if (manager == null) {
            getLogger().error("Couldn't send Email since Manager is null");
            session.transfer(flowfile, REL_FAILURE);
            return;
        }

        manager.send(recipient, subject, body, false);
    }
}

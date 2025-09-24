package org.integratedmodelling.klab.nifi;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.integratedmodelling.common.authentication.KlabCertificateImpl;
import org.integratedmodelling.common.services.client.engine.EngineImpl;
import org.integratedmodelling.klab.api.Klab;
import org.integratedmodelling.klab.api.engine.Engine;
import org.integratedmodelling.klab.api.exceptions.KlabAuthorizationException;
import org.integratedmodelling.klab.api.identities.Federation;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.KlabService;
import org.integratedmodelling.klab.api.services.runtime.Channel;
import org.integratedmodelling.klab.api.services.runtime.Message;

public class KlabFederationControllerService extends AbstractControllerService
    implements KlabController {

  public static final PropertyDescriptor CERTIFICATE_PROPERTY =
      new PropertyDescriptor.Builder()
          .name("Certificate")
          .displayName("k.LAB Certificate")
          .description("The URL for the k.LAB certificate to use for authentication")
          .required(false)
          .addValidator(StandardValidators.URL_VALIDATOR)
          .build();

  public static final PropertyDescriptor DEFAULT_QUEUES =
      new PropertyDescriptor.Builder()
          .name("default-queues")
          .displayName("Default queues")
          .description(
              "A comma-separated list of queue types that will provide a default for the connected scopes unless otherwise specified."
                  + "Values must be one or more of Events, Errors, Status, Info, Warning, Debug, UI. The default is Events, Errors, Status.")
          .required(false)
          // .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  private static final List<PropertyDescriptor> properties =
      List.of(CERTIFICATE_PROPERTY, DEFAULT_QUEUES);

  private Map<String, Scope> scopeMap;
  private String certificatePath;
  private Engine engine;
  private UserScope userScope;
  private Scope configuredScope;
  private Federation federation;
  private Set<Message.Queue> queues =
      EnumSet.of(Message.Queue.Events, Message.Queue.Errors, Message.Queue.Status);

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }

  private void updateServiceStatus(KlabService service, KlabService.ServiceStatus serviceStatus) {
    // TODO keep tabs on all available services and their status
  }

  private void updateEngineStatus(Engine.Status status) {
    // TODO update and log what needed
  }

  @Override
  public Scope getScope(Class<? extends Scope> scopeClass) {
    return this.configuredScope;
  }

  @Override
  public Scope getScope(String dtURL, Class<? extends Scope> scopeClass) {
    return this.scopeMap.get(dtURL);
  }

  /*
  TODO update info
  Adds ContextScopes to the Scope Map if the relevant Scope
  corresponding to the DT URL if absent
   */
  @Override
  public void addScope(String dtURL, Scope scope) throws KlabAuthorizationException {
    if (this.scopeMap.containsKey(dtURL)) {
      return; // TODO check if we need to override the current one
    }
    try {
      this.userScope = authenticateUserScope();
    } catch (URISyntaxException | InitializationException e) {
      throw new KlabAuthorizationException(e);
    }
    this.scopeMap.put(dtURL, scope);

    this.engine.boot();
    this.configuredScope =
            this.userScope; // getScope would return the Userscope (and not the context scope) here

    // Set up a message listener for the configured scope
    if (this.configuredScope != null) {
      setupMessageListener();
    }

    getLogger()
            .info("Initialization done for the UserScope from the Certificate for the federation");
  }

  @Override
  public Scope createScope(String dtURL) throws KlabAuthorizationException {
    if (scopeMap.containsKey(dtURL)) {
      getLogger().info("Scope already exists.");
      return scopeMap.get(dtURL);
    }
    try {
      this.userScope = authenticateUserScope();
    } catch (URISyntaxException | InitializationException e) {
      throw new KlabAuthorizationException(e);
    }
    this.scopeMap.put(dtURL, userScope);

    this.engine.boot();
    this.configuredScope =
            this.userScope; // getScope would return the Userscope (and not the context scope) here

    return userScope;
  }

  @Override
  public boolean hasScope(String dtUrl) {
    return scopeMap.containsKey(dtUrl);
  }

  @Override
  public void addEventListener(Consumer<EventData> listener) {}

  @Override
  public void removeEventListener(Consumer<EventData> listener) {}

  private void setupMessageListener() {
    // This would depend on your k.LAB API for message listening
    configuredScope.onMessage(this::handleKlabMessage, queues.toArray(new Message.Queue[0]));
  }

  private final Set<Consumer<EventData>> eventListeners = ConcurrentHashMap.newKeySet();
  private final ExecutorService eventExecutor = Executors.newCachedThreadPool();

  /*
  Make the Auth Call based on the Certificate,
  And get the user scope, and make the connect call with that
   */

  @OnEnabled
  public void onEnabled(final ConfigurationContext context) {
    this.engine = new EngineImpl(this::updateEngineStatus, this::updateServiceStatus);
    this.scopeMap = new ConcurrentHashMap<>();
    this.certificatePath = context.getProperty(CERTIFICATE_PROPERTY).getValue();
  }

  private UserScope authenticateUserScope() throws URISyntaxException, InitializationException {
    final boolean useDefaultPath = this.certificatePath == null;
    getLogger().info("Processing Certificate");
    if (useDefaultPath) {
      return this.engine.authenticate();
    } else {
      var certificateUri = new URI(this.certificatePath);
      var certificateFile = Paths.get(certificateUri).toAbsolutePath().toFile();
      if (!certificateFile.exists()) {
        throw new InitializationException("Cannot find a certificate at: " + this.certificatePath);
      }
      var certificate = KlabCertificateImpl.createFromFile(certificateFile);
      if (!certificate.isValid()) {
        throw new InitializationException(
            "Certificate is not valid: " + certificate.getInvalidityCause());
      }
      var ret = this.engine.authenticate(certificate);
      if (ret == null || ret.getUser().isAnonymous()) {
        throw new KlabAuthorizationException(
                "Unable to authenticate to k.LAB. Authentication is required for operation.");
      }

      this.federation =
              ret == null ? null : Klab.INSTANCE.getFederationData(ret.getUser());
      if (this.federation == null) {
        getLogger()
                .warn(
                        "User {} is not federated: messaging features disabled.",
                        ret.getUser().getUsername());
      }
      return ret;
    }
  }

  @OnDisabled
  public void shutdown() {
    scopeMap.clear();
    eventListeners.clear();
    eventExecutor.shutdown();
    try {
      if (!eventExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        eventExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      eventExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private void handleKlabMessage(Channel scope, Message message) {

    // Convert k.LAB message to EventData
    EventData eventData = convertMessageToEventData(message);

    // Notify all registered listeners asynchronously
    eventListeners.forEach(
        listener ->
            eventExecutor.submit(
                () -> {
                  try {
                    listener.accept(eventData);
                  } catch (Exception e) {
                    getLogger().error("Error notifying event listener", e);
                  }
                }));
  }

  private EventData convertMessageToEventData(Message message) {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("message.type", message.getMessageType().toString());
    attributes.put("message.class", message.getMessageClass().toString());
    attributes.put("message.queue", message.getQueue().toString());
    attributes.put("message.timestamp", String.valueOf(message.getTimestamp()));
    // Add other relevant attributes

    return new EventData(message.getPayload(message.getMessageType().payloadClass), attributes);
  }
}

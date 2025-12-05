/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.integratedmodelling.klab.nifi;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
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

/**
 * The <code>KlabControllerService</code> is required by all k.LAB processors and is responsible for
 * establishing connection to k.LAB and setting up the scope of interest, which can be a {@link
 * UserScope}, a {@link org.integratedmodelling.klab.api.scope.SessionScope}, or a {@link
 * org.integratedmodelling.klab.api.scope.ContextScope} according to configuration.
 *
 * <p>The controller logs a federated user into k.LAB upon creation, sets up the target scope based
 * on configuration and translates events from the scope into NiFi-consumable {@link
 * org.apache.nifi.flowfile.FlowFile}s, which other processors can consume.
 */
public class KlabControllerWithDTService extends AbstractControllerService implements KlabController {

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
                  //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                  .build();

  private static final List<PropertyDescriptor> properties =
          List.of(CERTIFICATE_PROPERTY, DEFAULT_QUEUES);

  private Engine engine;
  private Map<String, Scope> scopeMap;
  private UserScope userScope;
  private Scope configuredScope;
  private Federation federation;
  private String certificatePath;
  private final BlockingQueue<EventData> eventQueue = new LinkedBlockingQueue<>();

  private Set<Message.Queue> queues =
          EnumSet.of(Message.Queue.Events,Message.Queue.Errors, Message.Queue.Status);

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

  // Add listener management
  private final Set<Consumer<EventData>> eventListeners = ConcurrentHashMap.newKeySet();
  private final ExecutorService eventExecutor = Executors.newCachedThreadPool();

  @OnEnabled
  public void onEnabled(final ConfigurationContext context)
          throws InitializationException, URISyntaxException {
    this.engine = new EngineImpl(this::updateEngineStatus, this::updateServiceStatus);
    this.scopeMap = new ConcurrentHashMap<>();
    this.certificatePath = context.getProperty(CERTIFICATE_PROPERTY).getValue();

    final boolean useDefaultPath = this.certificatePath == null || this.certificatePath.isBlank();
    if (useDefaultPath) {
      getLogger().debug("Using default certificate path");
      this.userScope = engine.authenticate();
      getLogger().debug("User scope: " + this.userScope);
    } else {
      getLogger().debug("Using certificate path: " + this.certificatePath);

      var certificateUri = new URI(this.certificatePath);
      var exists = Files.exists(Paths.get(certificateUri));

      var certificateFile = Paths.get(certificateUri).toAbsolutePath().toFile();
      if (!certificateFile.exists()) {
        throw new InitializationException("Cannot find a certificate at: " + this.certificatePath);
      }
      var certificate = KlabCertificateImpl.createFromFile(certificateFile);
      if (!certificate.isValid()) {
        throw new InitializationException(
                "Certificate is not valid: " + certificate.getInvalidityCause());
      }
      this.userScope = engine.authenticate(certificate);
    }

    if (this.userScope == null || this.userScope.getUser().isAnonymous()) {
      throw new InitializationException(
              "Unable to authenticate to k.LAB. Authentication is required for operation.");
    }
    this.federation =
            this.userScope == null
                    ? null
                    : Klab.INSTANCE.getFederationData(this.userScope.getUser());
    if (federation == null) {
      getLogger()
              .warn(
                      "User {} is not federated: messaging features disabled.",
                      userScope.getUser().getUsername());
    }
    this.engine.boot();
    this.configuredScope = this.userScope;

    // Register as listener with the controller service
    addEventListener(this::handleEvent);
  }

  @OnDisabled
  public void shutdown() {
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

  private void handleEvent(EventData eventData) {
    try {
      eventQueue.offer(eventData, 1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      getLogger().warn("Interrupted while queuing event");
    }
  }

  @Override
  public void addEventListener(Consumer<EventData> listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeEventListener(Consumer<EventData> listener) {
    eventListeners.remove(listener);
  }

  private void setupMessageListener() {
    // This would depend on your k.LAB API for message listening
    configuredScope.onMessage(this::handleKlabMessage, queues.toArray(new Message.Queue[0]));
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

  @Override
  public Scope getScope(String dtURL, Class<? extends Scope> scopeClass) {
    return this.scopeMap.get(dtURL);
  }

  @Override
  public Scope getScope(Class<? extends Scope> scopeClass) {
    // TODO check if scope is configured for this user. If not, a default session scope and context
    //  scope can be created when the correspondent classes are requested.
    return this.userScope;
  }

  @Override
  public void addScope(String dtURL, Scope scope) throws KlabAuthorizationException {
    // A method to add a scope to a DT URL, ALWAYS would update the Scope
    // corresponding to a DT URL, and add one if not existing from before
    this.scopeMap.put(dtURL, scope);
  }
}
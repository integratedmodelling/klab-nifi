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

import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.integratedmodelling.common.authentication.Authentication;
import org.integratedmodelling.common.services.client.engine.EngineImpl;
import org.integratedmodelling.klab.api.engine.Engine;
import org.integratedmodelling.klab.api.identities.Federation;
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.scope.UserScope;
import org.integratedmodelling.klab.api.services.KlabService;

/** Logs a federated user into k.LAB and maintains a set of scopes for that user. */
@Tags({"k.LAB"})
@CapabilityDescription(
    "Controller service providing a k.LAB user scope and federation-wide messaging facilities")
public class KlabControllerService extends AbstractControllerService implements KlabController {

  public static final PropertyDescriptor CERTIFICATE_PROPERTY =
      new PropertyDescriptor.Builder()
          .name("Certificate")
          .displayName("k.LAB Certificate")
          .description("The URL for the k.LAB certificate to use for authentication")
          .required(false)
          .addValidator(StandardValidators.URL_VALIDATOR)
          .build();

  private static final List<PropertyDescriptor> properties = List.of(CERTIFICATE_PROPERTY);

  private Engine engine;
  private UserScope userScope;
  private Scope configuredScope;
  private Federation federation;

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }

  /**
   * @param context the configuration context
   * @throws InitializationException if unable to create a k.LAB connection
   */
  @OnEnabled
  public void onEnabled(final ConfigurationContext context) throws InitializationException {
    this.engine = new EngineImpl(this::updateEngineStatus, this::updateServiceStatus);
    // TODO find certificate through properties
    this.userScope = engine.authenticate();
    if (this.userScope == null || this.userScope.getUser().isAnonymous()) {
      throw new InitializationException(
          "Unable to authenticate to k.LAB. Authentication is required for operation.");
    }
    this.federation =
        this.userScope == null
            ? null
            : Authentication.INSTANCE.getFederationData(this.userScope.getUser());
    if (federation == null) {
      getLogger()
          .warn(
              "User {} is not federated: messaging features disabled.",
              userScope.getUser().getUsername());
    }
    this.engine.boot();
    this.configuredScope = this.userScope;
    // TODO check properties for a DT URL or ID
    // TODO install overall message router as a listener
  }

  private void updateServiceStatus(KlabService service, KlabService.ServiceStatus serviceStatus) {
    // TODO keep tabs on all available services and their status
  }

  private void updateEngineStatus(Engine.Status status) {
    // TODO update and log what needed
  }

  @OnDisabled
  public void shutdown() {
    // TODO remove all listeners
  }

  @Override
  public Scope getScope(Class<? extends Scope> scopeClass) {
    // TODO check if scope is configured for this user. If not, a default session scope and context
    //  scope can be created when the correspondent classes are requested.
    return this.configuredScope;
  }
}

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
import org.integratedmodelling.klab.api.scope.Scope;
import org.integratedmodelling.klab.api.scope.UserScope;

@Tags({"example"})
@CapabilityDescription("Controller service providing access to the k.LAB network.")
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
    this.engine = new EngineImpl();
    // TODO find certificate through properties
    this.userScope = engine.authenticate();
    var federationData =
        this.userScope == null
            ? null
            : Authentication.INSTANCE.getFederationData(this.userScope.getUser());
    if (federationData == null) {
      throw new InitializationException(
          "Unable to authenticate to k.LAB or the authenticating certificate is not in a federation.");
    }
    // TODO check properties for a DT URL or ID
  }

  @OnDisabled
  public void shutdown() {}

  @Override
  public Scope getScope(Class<? extends Scope> scopeClass) {
    return this.configuredScope;
  }
}

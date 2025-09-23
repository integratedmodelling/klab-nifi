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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.integratedmodelling.klab.api.exceptions.KlabAuthorizationException;
import org.integratedmodelling.klab.api.exceptions.KlabException;
import org.integratedmodelling.klab.api.scope.Scope;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Tags({"k.LAB", "Semantic Web"})
@CapabilityDescription("k.LAB Service API.")
public interface KlabController extends ControllerService {

  /**
   * Return the k.LAB scope configured in the processor, whose specific class depends on
   * configuration.
   *
   * @return the scope requested. A scope appropriate for the request will be created if not
   *     configured in advance.
   */
  Scope getScope(Class<? extends Scope> scopeClass);

  Scope getScope(String dtURL, Class<? extends Scope> scopeClass);

  void addEventListener(Consumer<EventData> listener);

  void removeEventListener(Consumer<EventData> listener);

  void addScope(String dtURL, Scope scope) throws KlabAuthorizationException;
}

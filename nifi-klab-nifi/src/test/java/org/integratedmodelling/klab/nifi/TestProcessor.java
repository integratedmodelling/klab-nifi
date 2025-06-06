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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestProcessor {

  private TestRunner testRunner;
  @Mock private KlabController mockKlabController;
  private AutoCloseable mockitoContext;

  @BeforeEach
  public void setUp() throws InitializationException {
    mockitoContext = MockitoAnnotations.openMocks(this);
    testRunner = TestRunners.newTestRunner(MessageRelayProcessor.class);
    testRunner.addControllerService("klab-controller", mockKlabController);
    testRunner.enableControllerService(mockKlabController);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mockitoContext != null) {
      mockitoContext.close();
    }
  }

  @Test
  public void testProcessorConfiguration() {
    testRunner.assertValid();
  }
}

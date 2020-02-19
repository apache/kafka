/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.support.metrics.validate;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class MetricsValidationTest {
  @Test
  public void testValidKsqlDeploymentMode() {
    List<String> validDeploymentModes = Arrays.asList(
        "LOCAL_CLI", "REMOTE_CLI", "SERVER", "EMBEDDED", "CLI"
    );
    for (String mode: validDeploymentModes) {
      assertTrue("Expected KSQL deployment mode '" + mode + "' to be valid",
                 MetricsValidation.isValidKsqlModuleType(mode));
    }
  }

  @Test
  public void testInvalidKsqlDeploymentMode() {
    List<String> invalidDeploymentModes = Arrays.asList(
        "local_CLI", "REMOTECLI", "servER", "random", "ANOTHER_CLI"
    );
    for (String mode: invalidDeploymentModes) {
      assertFalse("Expected KSQL deployment mode '" + mode + "' to be invalid",
                 MetricsValidation.isValidKsqlModuleType(mode));
    }
  }
}

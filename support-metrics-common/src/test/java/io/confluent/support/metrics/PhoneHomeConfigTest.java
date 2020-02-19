/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.support.metrics;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class PhoneHomeConfigTest {

  private static final String EXPECTED_SECURE_ENDPOINT = "https://version-check.confluent.io";
  private static final String EXPECTED_INSECURE_ENDPOINT = "http://version-check.confluent.io";

  @Test
  public void testProductionEndpoints() {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, "c11");
    overrideProps.getProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "topic");

    BaseSupportConfig config = new PhoneHomeConfig(overrideProps, "TestComponent");
    assertEquals("", config.getKafkaTopic());
    assertEquals(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT, config.getCustomerId());
    assertEquals(EXPECTED_SECURE_ENDPOINT + "/TestComponent/anon",
                 config.getEndpointHTTPS());
    assertEquals(EXPECTED_INSECURE_ENDPOINT + "/TestComponent/anon",
                 config.getEndpointHTTP());
  }

  @Test
  public void testTestEndpoints() {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
                              BaseSupportConfig.CONFLUENT_SUPPORT_TEST_ID_DEFAULT);

    BaseSupportConfig config = new PhoneHomeConfig(overrideProps, "TestComponent");
    assertEquals("", config.getKafkaTopic());
    assertEquals(BaseSupportConfig.CONFLUENT_SUPPORT_TEST_ID_DEFAULT, config.getCustomerId());
    assertEquals(EXPECTED_SECURE_ENDPOINT + "/TestComponent/test",
                 config.getEndpointHTTPS());
    assertEquals(EXPECTED_INSECURE_ENDPOINT + "/TestComponent/test",
                 config.getEndpointHTTP());
  }

  @Test
  public void testInsecureEndpointDisabled() {
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");

    BaseSupportConfig config = new PhoneHomeConfig(overrideProps, "TestComponent");
    assertEquals(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT, config.getCustomerId());
    assertEquals(EXPECTED_SECURE_ENDPOINT + "/TestComponent/anon",
                 config.getEndpointHTTPS());
    assertEquals("",
                 config.getEndpointHTTP());
  }

}

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

package io.confluent.support.metrics;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SupportedServerStartableTest {

  private Properties defaultBrokerConfiguration() throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(SupportedServerStartableTest.class.getResourceAsStream("/default-server.properties"));
    return brokerConfiguration;
  }

  @Test
  public void testProactiveSupportEnabled() throws IOException {
    Properties brokerConfiguration = defaultBrokerConfiguration();
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(brokerConfiguration);

    assertTrue(supportedServerStartable.isProactiveSupportActiveAtRuntime());
    assertNotNull(supportedServerStartable.getMetricsReporter());
    assertTrue(supportedServerStartable.getMetricsReporter().reportingEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled());
  }

  @Test
  public void testProactiveSupportDisabled() throws IOException {
    Properties brokerConfiguration = defaultBrokerConfiguration();
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(brokerConfiguration);

    assertFalse(supportedServerStartable.isProactiveSupportActiveAtRuntime());
    assertTrue(supportedServerStartable.getMetricsReporter() == null);
  }

  @Test
  public void testProactiveSupportEnabledKafkaOnly() throws IOException {
    Properties brokerConfiguration = defaultBrokerConfiguration();
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(brokerConfiguration);

    assertTrue(supportedServerStartable.isProactiveSupportActiveAtRuntime());
    assertTrue(supportedServerStartable.getMetricsReporter().reportingEnabled());
    assertFalse(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled());
  }

  @Test
  public void testProactiveSupportEnabledKafkaAndConfluentHTTPOnly() throws IOException {
    Properties brokerConfiguration = defaultBrokerConfiguration();
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(brokerConfiguration);

    assertTrue(supportedServerStartable.isProactiveSupportActiveAtRuntime());
    assertTrue(supportedServerStartable.getMetricsReporter().reportingEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled());
  }

  @Test
  public void testProactiveSupportEnabledKafkaAndConfluentHTTPSOnly() throws IOException {
    Properties brokerConfiguration = defaultBrokerConfiguration();
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(brokerConfiguration);

    assertTrue(supportedServerStartable.isProactiveSupportActiveAtRuntime());
    assertTrue(supportedServerStartable.getMetricsReporter().reportingEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled());
  }

  @Test
  public void testProactiveSupportEnabledConfluentHTTPSAndHTTPOnly() throws IOException {
    Properties brokerConfiguration = defaultBrokerConfiguration();
    brokerConfiguration.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "");
    SupportedServerStartable supportedServerStartable = new SupportedServerStartable(brokerConfiguration);

    assertTrue(supportedServerStartable.isProactiveSupportActiveAtRuntime());
    assertTrue(supportedServerStartable.getMetricsReporter().reportingEnabled());
    assertTrue(supportedServerStartable.getMetricsReporter().sendToConfluentEnabled());
    assertFalse(supportedServerStartable.getMetricsReporter().sendToKafkaEnabled());
  }
}

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

import kafka.zk.KafkaZkClient;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricsReporterTest {
  private static KafkaServer mockServer;

  @BeforeClass
  public static void startCluster() {
    KafkaZkClient mockZkClient = mock(KafkaZkClient.class);
    KafkaConfig mockConfig = mock(KafkaConfig.class);
    when(mockConfig.advertisedHostName()).thenReturn("anyHostname");
    when(mockConfig.advertisedPort()).thenReturn(12345);
    mockServer = mock(KafkaServer.class);
    when(mockServer.zkClient()).thenReturn(mockZkClient);
    when(mockServer.config()).thenReturn(mockConfig);
  }

  @Test
  public void testInvalidArgumentsForConstructorNullServer() {
    // Given
    Properties emptyProperties = new Properties();
    Runtime serverRuntime = Runtime.getRuntime();
    KafkaSupportConfig kafkaSupportConfig = new KafkaSupportConfig(emptyProperties);
    // When/Then
    try {
      new MetricsReporter("testThread", false, null, kafkaSupportConfig, serverRuntime);
      fail("NullPointerException expected because server is null");
    } catch (NullPointerException e) {
      assertTrue(e.getMessage().contains("Kafka Server can't be null"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullProperties() {
    // Given
    Runtime serverRuntime = Runtime.getRuntime();

    // When/Then
    try {
      new MetricsReporter("testThread", false, mockServer, null, serverRuntime);
      fail("NullPointerException expected because props is null");
    } catch (NullPointerException e) {
      assertTrue(e.getMessage().contains("supportConfig can't be null"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullRuntime() {
    // Given
    Properties emptyProperties = new Properties();
    KafkaSupportConfig kafkaSupportConfig = new KafkaSupportConfig(emptyProperties);

    // When/Then
    try {
      new MetricsReporter("testThread", false, mockServer, kafkaSupportConfig, null);
      fail("NullPointerException expected because serverRuntime is null");
    } catch (NullPointerException e) {
      assertTrue(e.getMessage().contains("serverRuntime can't be null"));
    }
  }


  @Test
  public void testValidConstructorTopicOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    Runtime serverRuntime = Runtime.getRuntime();
    KafkaSupportConfig kafkaSupportConfig = new KafkaSupportConfig(serverProperties);
    // When
    MetricsReporter reporter = new MetricsReporter("testThread", false, mockServer, kafkaSupportConfig, serverRuntime);
    reporter.init();
    // Then
    assertTrue(reporter.reportingEnabled());
    assertTrue(reporter.sendToKafkaEnabled());
    assertTrue(reporter.sendToConfluentEnabled());
  }

  @Test
  public void testValidConstructorHTTPOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "http://example.com/");
    Runtime serverRuntime = Runtime.getRuntime();
    KafkaSupportConfig kafkaSupportConfig = new KafkaSupportConfig(serverProperties);
    // When
    MetricsReporter reporter = new MetricsReporter("testThread", false, mockServer, kafkaSupportConfig, serverRuntime);
    reporter.init();
    // Then
    assertTrue(reporter.reportingEnabled());
    assertTrue(reporter.sendToKafkaEnabled());
    assertTrue(reporter.sendToConfluentEnabled());
  }

  @Test
  public void testValidConstructorHTTPSOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(KafkaSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG, "https://example.com/");
    Runtime serverRuntime = Runtime.getRuntime();
    KafkaSupportConfig kafkaSupportConfig = new KafkaSupportConfig(serverProperties);
    // When
    MetricsReporter reporter = new MetricsReporter("testThread", false, mockServer, kafkaSupportConfig, serverRuntime);
    reporter.init();
    // Then
    assertTrue(reporter.reportingEnabled());
    assertTrue(reporter.sendToKafkaEnabled());
    assertTrue(reporter.sendToConfluentEnabled());
  }
}

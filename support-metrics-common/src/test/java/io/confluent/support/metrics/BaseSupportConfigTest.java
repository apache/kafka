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
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

import io.confluent.support.metrics.utils.CustomerIdExamples;
import kafka.server.KafkaConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BaseSupportConfigTest {

  @Test
  public void testValidCustomer() {
    for (String validId : CustomerIdExamples.VALID_CUSTOMER_IDS) {
      assertTrue(validId + " is an invalid customer identifier",
          BaseSupportConfig.isConfluentCustomer(validId));
    }
  }

  @Test
  public void testValidNewCustomer() {
    String[] validNewCustomerIds = Stream.concat(
        Arrays.stream(CustomerIdExamples.VALID_CASE_SENSISTIVE_NEW_CUSTOMER_IDS),
        Arrays.stream(CustomerIdExamples.VALID_CASE_INSENSISTIVE_NEW_CUSTOMER_IDS)).
        toArray(String[]::new);
    for (String validId : validNewCustomerIds) {
      assertTrue(validId + " is an invalid new customer identifier",
                 BaseSupportConfig.isConfluentCustomer(validId));
    }
  }

  @Test
  public void testInvalidCustomer() {
    String[] invalidIds = Stream.concat(
        Arrays.stream(CustomerIdExamples.INVALID_CUSTOMER_IDS),
        Arrays.stream(CustomerIdExamples.VALID_ANONYMOUS_IDS)).
        toArray(String[]::new);
    for (String invalidCustomerId : invalidIds) {
      assertFalse(invalidCustomerId + " is a valid customer identifier",
          BaseSupportConfig.isConfluentCustomer(invalidCustomerId));
    }
  }

  @Test
  public void testValidAnonymousUser() {
    for (String validId : CustomerIdExamples.VALID_ANONYMOUS_IDS) {
      assertTrue(validId + " is an invalid anonymous user identifier",
          BaseSupportConfig.isAnonymousUser(validId));
    }
  }

  @Test
  public void testInvalidAnonymousUser() {
    String[] invalidIds = Stream.concat(
        Arrays.stream(CustomerIdExamples.INVALID_ANONYMOUS_IDS),
        Arrays.stream(CustomerIdExamples.VALID_CUSTOMER_IDS)).
        toArray(String[]::new);
    for (String invalidId : invalidIds) {
      assertFalse(invalidId + " is a valid anonymous user identifier",
          BaseSupportConfig.isAnonymousUser(invalidId));
    }
  }

  @Test
  public void testCustomerIdValidSettings() {
    String[] validValues = Stream.concat(
        Arrays.stream(CustomerIdExamples.VALID_ANONYMOUS_IDS),
        Arrays.stream(CustomerIdExamples.VALID_CUSTOMER_IDS)).
        toArray(String[]::new);
    for (String validValue : validValues) {
      assertTrue(validValue + " is an invalid value for " +
          BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          BaseSupportConfig.isSyntacticallyCorrectCustomerId(validValue));
      // old customer Ids are all case-insensitive
      assertFalse(validValue + " is case-sensitive customer ID.",
                 BaseSupportConfig.isCaseSensitiveCustomerId(validValue));
    }
  }

  @Test
  public void testCustomerIdInvalidSettings() {
    String[] invalidValues = Stream.concat(
        Arrays.stream(CustomerIdExamples.INVALID_ANONYMOUS_IDS),
        Arrays.stream(CustomerIdExamples.INVALID_CUSTOMER_IDS)).
        toArray(String[]::new);
    for (String invalidValue : invalidValues) {
      assertFalse(invalidValue + " is a valid value for " +
          BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          BaseSupportConfig.isSyntacticallyCorrectCustomerId(invalidValue));
    }
  }

  @Test
  public void testCaseInsensitiveNewCustomerIds() {
    for (String validValue : CustomerIdExamples.VALID_CASE_INSENSISTIVE_NEW_CUSTOMER_IDS) {
      assertFalse(validValue + " is case-sensitive customer ID.",
                 BaseSupportConfig.isCaseSensitiveCustomerId(validValue));
    }
  }

  @Test
  public void testCaseSensitiveNewCustomerIds() {
    for (String validValue : CustomerIdExamples.VALID_CASE_SENSISTIVE_NEW_CUSTOMER_IDS) {
      assertTrue(validValue + " is case-insensitive customer ID.",
                 BaseSupportConfig.isCaseSensitiveCustomerId(validValue));
    }
  }

  @Test
  public void proactiveSupportConfigIsValidKafkaConfig() throws IOException {
    // Given
    Properties brokerConfiguration = defaultBrokerConfiguration();

    // When
    KafkaConfig cfg = KafkaConfig.fromProps(brokerConfiguration);

    // Then
    assertEquals(0, cfg.brokerId());
    assertTrue(cfg.zkConnect().startsWith("localhost:"));
  }

  private Properties defaultBrokerConfiguration() throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(BaseSupportConfigTest.class.getResourceAsStream("/default-server"
                                                                             + ".properties"));
    return brokerConfiguration;
  }

  @Test
  public void canParseProactiveSupportConfiguration() throws IOException {
    // Given
    Properties brokerConfiguration = defaultBrokerConfiguration();

    BaseSupportConfig supportConfig = new TestSupportConfig(brokerConfiguration);
    // When/Then
    assertTrue(supportConfig.getMetricsEnabled());
    assertEquals("c0", supportConfig.getCustomerId());
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }


  @Test
  public void testGetDefaultProps() {
    // Given
    Properties overrideProps = new Properties();
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertTrue(supportConfig.getMetricsEnabled());
    assertEquals("anonymous", supportConfig.getCustomerId());
    assertEquals(24 * 60 * 60 * 1000, supportConfig.getReportIntervalMs());
    assertEquals("__confluent.support.metrics", supportConfig.getKafkaTopic());
    assertTrue(supportConfig.isHttpEnabled());
    assertTrue(supportConfig.isHttpsEnabled());
    assertTrue(supportConfig.isProactiveSupportEnabled());
    assertEquals("", supportConfig.getProxy());
    assertEquals("http://support-metrics.confluent.io/anon", supportConfig.getEndpointHTTP());
    assertEquals("https://support-metrics.confluent.io/anon", supportConfig.getEndpointHTTPS());
  }

  @Test
  public void testMergeAndValidatePropsFilterDisallowedKeys() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG,
        "anyValue"
    );
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG,
        "anyValue"
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertEquals("http://support-metrics.confluent.io/anon", supportConfig.getEndpointHTTP());
    assertEquals("https://support-metrics.confluent.io/anon", supportConfig.getEndpointHTTPS());
  }

  @Test
  public void testMergeAndValidatePropsDisableEndpoints() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertTrue(supportConfig.getEndpointHTTP().isEmpty());
    assertTrue(supportConfig.getEndpointHTTPS().isEmpty());
  }

  @Test
  public void testOverrideReportInterval() {
    // Given
    Properties overrideProps = new Properties();
    int reportIntervalHours = 1;
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
        String.valueOf(reportIntervalHours)
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertEquals(reportIntervalHours * 60 * 60 * 1000, supportConfig.getReportIntervalMs());
  }

  @Test
  public void testOverrideTopic() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG,
        "__another_example_topic"
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertEquals("__another_example_topic", supportConfig.getKafkaTopic());
  }

  @Test
  public void isProactiveSupportEnabledFull() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "true");

    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportDisabledFull() {
    // Given
    Properties serverProperties = new Properties();

    serverProperties
        .setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    serverProperties
        .setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        "true"
    );
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG,
        "true"
    );
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertFalse(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportEnabledTopicOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties
        .setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportEnabledHTTPOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        "true"
    );
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void isProactiveSupportEnabledHTTPSOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "true");
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  @Test
  public void proactiveSupportIsDisabledByDefaultWhenBrokerConfigurationIsEmpty() {
    // Given
    Properties serverProperties = new Properties();
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertTrue(supportConfig.isProactiveSupportEnabled());
  }

  public class TestSupportConfig extends BaseSupportConfig {

    public TestSupportConfig(Properties originals) {
      super(originals);
    }

    @Override
    protected String getAnonymousEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/anon";
      } else {
        return "https://support-metrics.confluent.io/anon";
      }
    }

    @Override
    protected String getTestEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/test";
      } else {
        return "https://support-metrics.confluent.io/test";
      }
    }

    @Override
    protected String getCustomerEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/submit";
      } else {
        return "https://support-metrics.confluent.io/submit";
      }
    }
  }

}

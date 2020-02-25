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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Use this config for component-specific phone-home clients. It disables writing metrics to
 * Kafka topic and ensures that non-test customer Ids are "anonymous", even if the client sets
 * support topic and customer id in the config.
 * TODO(apovzner) this class will probably be extended for standard phone-home libraries (the
 * difference is a different URI format).
 */
public class PhoneHomeConfig extends BaseSupportConfig {

  private static final Logger log = LoggerFactory.getLogger(PhoneHomeConfig.class);

  private static final boolean CONFLUENT_SUPPORT_METRICS_TOPIC_ENABLED_DEFAULT = false;
  private static final boolean CONFLUENT_SUPPORT_CUSTOMER_ID_ENABLED_DEFAULT = false;

  /**
   * Will make sure that writing to support metrics topic is disabled and there is no customer
   * endpoint.
   */
  public PhoneHomeConfig(Properties originals, String componentId) {
    this(originals, componentId,
         CONFLUENT_SUPPORT_METRICS_TOPIC_ENABLED_DEFAULT,
         CONFLUENT_SUPPORT_CUSTOMER_ID_ENABLED_DEFAULT);
  }

  private PhoneHomeConfig(Properties originals, String componentId,
                          boolean supportMetricsTopicEnabled, boolean supportCustomerIdEnabled) {
    super(setupProperties(originals, supportMetricsTopicEnabled, supportCustomerIdEnabled),
          getEndpointPath(componentId));
  }

  private static Properties setupProperties(Properties originals,
                                            boolean supportMetricsTopicEnabled,
                                            boolean supportCustomerIdEnabled) {
    if (!supportCustomerIdEnabled || !supportMetricsTopicEnabled) {
      //disable publish to topic
      if (originals == null) {
        originals = new Properties();
      }
      if (!supportMetricsTopicEnabled) {
        log.warn("Writing to metrics Kafka topic will be disabled");
        originals.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "");
      }
      if (!supportCustomerIdEnabled) {
        if (!isTestUser(getCustomerId(originals))) {
          log.warn("Enforcing customer ID '{}'", CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
          originals.setProperty(CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
                                CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
        }
      }
    }
    return originals;
  }


  /**
   * The resulting secure endpoint will be:
   * BaseSupportConfig.CONFLUENT_PHONE_HOME_ENDPOINT_BASE_SECURE/getEndpointPath()/{anon|test}
   * The resulting insecure endpoint will be:
   * BaseSupportConfig.CONFLUENT_PHONE_HOME_ENDPOINT_BASE_INSECURE/getEndpointPath()/{anon|test}
   */
  private static String getEndpointPath(String componentId) {
    return componentId;
  }

}

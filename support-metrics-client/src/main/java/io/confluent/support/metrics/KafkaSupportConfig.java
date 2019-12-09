/**
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
 * Configuration for the Confluent Support options.
 */
public class KafkaSupportConfig extends BaseSupportConfig {

  private static final String
      PROPRIETARY_PACKAGE_NAME =
      "io.confluent.support.metrics.collectors.FullCollector";
  private static final Logger log = LoggerFactory.getLogger(KafkaSupportConfig.class);

  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_DEFAULT =
      "http://support-metrics.confluent.io/anon";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CUSTOMER_DEFAULT =
      "http://support-metrics.confluent.io/submit";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_TEST_DEFAULT =
      "http://support-metrics.confluent.io/test";

  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_DEFAULT =
      "https://support-metrics.confluent.io/anon";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CUSTOMER_DEFAULT =
      "https://support-metrics.confluent.io/submit";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_TEST_DEFAULT =
      "https://support-metrics.confluent.io/test";

  public KafkaSupportConfig(Properties originals) {
    super(setupProperties(originals));
  }

  @Override
  protected String getAnonymousEndpoint(boolean secure) {
    if (secure) {
      return CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_DEFAULT;
    } else {
      return CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_DEFAULT;
    }
  }

  @Override
  protected String getTestEndpoint(boolean secure) {
    if (secure) {
      return CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_TEST_DEFAULT;
    } else {
      return CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_TEST_DEFAULT;
    }
  }

  @Override
  protected String getCustomerEndpoint(boolean secure) {
    if (secure) {
      return CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CUSTOMER_DEFAULT;
    } else {
      return CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CUSTOMER_DEFAULT;
    }
  }

  private static Properties setupProperties(Properties originals) {
    try {
      Class.forName(PROPRIETARY_PACKAGE_NAME);
    } catch (ClassNotFoundException e) {
      originals.setProperty(
          KafkaSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          KafkaSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT
      );
      log.warn(warningIfFullCollectorPackageMissing());
    }

    return originals;
  }

  private static String warningIfFullCollectorPackageMissing() {
    return "The package "
           + PROPRIETARY_PACKAGE_NAME
           + " for collecting the full set of support metrics could not be loaded, so we are "
           + "reverting to anonymous, basic metric collection. If you are a Confluent customer, "
           + "please refer to the Confluent Platform documentation, section Proactive Support, "
           + "on how to activate full metrics collection.";
  }
}

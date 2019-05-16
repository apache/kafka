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

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Configuration for the Confluent Support options.
 */
public abstract class BaseSupportConfig {

  private static final Logger log = LoggerFactory.getLogger(BaseSupportConfig.class);

  static final String CONFLUENT_PHONE_HOME_ENDPOINT_BASE_SECURE =
      "https://version-check.confluent.io";
  static final String CONFLUENT_PHONE_HOME_ENDPOINT_BASE_INSECURE =
      "http://version-check.confluent.io";
  public static final String CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_ANON = "anon";
  public static final String CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_TEST = "test";
  public static final String CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_CUSTOMER = "submit";

  /**
   * <code>confluent.support.metrics.enable</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG =
      "confluent.support.metrics.enable";
  private static final String CONFLUENT_SUPPORT_METRICS_ENABLE_DOC =
      "False to disable metric collection, true otherwise.";
  public static final String CONFLUENT_SUPPORT_METRICS_ENABLE_DEFAULT = "true";

  /**
   * <code>confluent.support.customer.id</code>
   */
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG = "confluent.support.customer.id";
  private static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DOC =
      "Customer ID assigned by Confluent";
  public static final String CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT = "anonymous";
  public static final String CONFLUENT_SUPPORT_TEST_ID_DEFAULT = "c0";

  /**
   * <code>confluent.support.metrics.report.interval.hours</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG =
      "confluent.support.metrics.report.interval.hours";
  private static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DOC =
      "Frequency of reporting in hours, e.g., 24 would indicate every day ";
  public static final String CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT = "24";

  /**
   * <code>confluent.support.metrics.topic</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG =
      "confluent.support.metrics.topic";
  private static final String CONFLUENT_SUPPORT_METRICS_TOPIC_DOC =
      "Internal topic used for metric collection. If missing, metrics will not be collected in a "
      + "Kafka topic ";
  public static final String CONFLUENT_SUPPORT_METRICS_TOPIC_DEFAULT =
      "__confluent.support.metrics";

  /**
   * <code>confluent.support.metrics.endpoint.insecure.enable</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG =
      "confluent.support.metrics.endpoint.insecure.enable";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DOC =
      "False to disable reporting over HTTP, true otherwise";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DEFAULT = "true";

  /**
   * <code>confluent.support.metrics.endpoint.secure.enable</code>
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG =
      "confluent.support.metrics.endpoint.secure.enable";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DOC =
      "False to disable reporting over HTTPS, true otherwise";
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DEFAULT = "true";

  /**
   * <code>confluent.support.proxy</code>
   */
  public static final String CONFLUENT_SUPPORT_PROXY_CONFIG = "confluent.support.proxy";
  public static final String CONFLUENT_SUPPORT_PROXY_DOC =
      "HTTP forward proxy used to support metrics to Confluent";
  public static final String CONFLUENT_SUPPORT_PROXY_DEFAULT = "";


  /**
   * Confluent endpoints. These are internal properties that cannot be set from a config file
   * but that are added to the original config file at startup time
   */
  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG =
      "confluent.support.metrics.endpoint.insecure";

  public static final String CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG =
      "confluent.support.metrics.endpoint.secure";


  private static final Pattern CUSTOMER_PATTERN = Pattern.compile("c\\d{1,30}");
  private static final Pattern NEW_CUSTOMER_CASE_SENSISTIVE_PATTERN = Pattern.compile(
      "[a-zA-Z0-9]{15}"
  );
  private static final Pattern NEW_CUSTOMER_CASE_INSENSISTIVE_PATTERN = Pattern.compile(
      "[a-zA-Z0-9]{18}"
  );

  public Properties getProperties() {
    return properties;
  }

  private Properties properties;


  /**
   * Returns the default Proactive Support properties
   */
  protected Properties getDefaultProps() {
    Properties props = new Properties();
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG,
        CONFLUENT_SUPPORT_METRICS_ENABLE_DEFAULT
    );
    props.setProperty(CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG, CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
        CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT
    );
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG,
        CONFLUENT_SUPPORT_METRICS_TOPIC_DEFAULT
    );
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DEFAULT
    );
    props.setProperty(
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG,
        CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DEFAULT
    );
    props.setProperty(CONFLUENT_SUPPORT_PROXY_CONFIG, CONFLUENT_SUPPORT_PROXY_DEFAULT);
    return props;
  }


  // TODO(apovzner) remove this after all derived classes use constructor with endpointPath
  public BaseSupportConfig(Properties originals) {
    mergeAndValidateWithDefaultProperties(originals, null);
  }

  public BaseSupportConfig(Properties originals, String endpointPath) {
    mergeAndValidateWithDefaultProperties(originals, endpointPath);
  }

  /**
   * Takes default properties from getDefaultProps() and a set of override properties and
   * returns a merged properties object, where the defaults are overridden.
   * Sanitizes and validates the returned properties
   *
   * @param overrides Parameters that override the default properties
   */
  private void mergeAndValidateWithDefaultProperties(Properties overrides, String endpointPath) {
    Properties defaults = getDefaultProps();
    Properties props;

    if (overrides == null) {
      props = defaults;
    } else {
      props = new Properties();
      props.putAll(defaults);
      props.putAll(overrides);
    }
    this.properties = props;

    // make sure users are not setting internal properties in the config file
    // sanitize props just in case
    props.remove(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG);
    props.remove(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG);

    String customerId = getCustomerId();

    // new way to set endpoint
    if (endpointPath != null) {
      // derived class provided URI
      if (isHttpEnabled()) {
        setEndpointHTTP(getEndpoint(false, customerId, endpointPath));
      }
      if (isHttpsEnabled()) {
        setEndpointHTTPS(getEndpoint(true, customerId, endpointPath));
      }
    } else {
      // TODO(apovzner) once all existing clients provide endpointPath, remove this code
      // set the correct customer id/endpoint pair
      setEndpointsOldWay(customerId);
    }
  }

  private void setEndpointsOldWay(String customerId) {
    if (isAnonymousUser(customerId)) {
      if (isHttpEnabled()) {
        setEndpointHTTP(getAnonymousEndpoint(false));
      }
      if (isHttpsEnabled()) {
        setEndpointHTTPS(getAnonymousEndpoint(true));
      }
    } else if (isTestUser(customerId)) {
      if (isHttpEnabled()) {
        setEndpointHTTP(getTestEndpoint(false));
      }
      if (isHttpsEnabled()) {
        setEndpointHTTPS(getTestEndpoint(true));
      }
    } else {
      if (isHttpEnabled()) {
        setEndpointHTTP(getCustomerEndpoint(false));
      }
      if (isHttpsEnabled()) {
        setEndpointHTTPS(getCustomerEndpoint(true));
      }
    }
  }

  // TODO(apovzner) remove this after all derived classes use constructor with endpointPath
  protected String getAnonymousEndpoint(boolean secure) {
    return null;
  }

  // TODO(apovzner) remove this after all derived classes use constructor with endpointPath
  protected String getTestEndpoint(boolean secure) {
    return null;
  }

  // TODO(apovzner) remove this after all derived classes use constructor with endpointPath
  protected String getCustomerEndpoint(boolean secure) {
    return null;
  }

  public static String getEndpoint(boolean secure, String customerId, String endpointPath) {
    String base = secure ? CONFLUENT_PHONE_HOME_ENDPOINT_BASE_SECURE
                         : CONFLUENT_PHONE_HOME_ENDPOINT_BASE_INSECURE;
    return base + "/" + endpointPath + "/" + getEndpointSuffix(customerId);
  }

  private static String getEndpointSuffix(String customerId) {
    if (isAnonymousUser(customerId)) {
      return CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_ANON;
    } else if (isTestUser(customerId)) {
      return CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_TEST;
    } else {
      return CONFLUENT_PHONE_HOME_ENDPOINT_SUFFIX_USER_CUSTOMER;
    }
  }

  /**
   * A check on whether Proactive Support (PS) is enabled or not. PS is disabled when
   * CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG is false or is not defined
   *
   * @return false if PS is not enabled, true if PS is enabled
   */
  public boolean isProactiveSupportEnabled() {
    if (properties == null) {
      return false;
    }
    return getMetricsEnabled();
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the setting we use to denote anonymous users.
   */
  public static boolean isAnonymousUser(String customerId) {
    return customerId != null && customerId.toLowerCase(Locale.ROOT).equals(
        CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the setting we use to denote internal testing.
   */
  public static boolean isTestUser(String customerId) {
    return customerId != null && customerId.toLowerCase(Locale.ROOT).equals(CONFLUENT_SUPPORT_TEST_ID_DEFAULT);
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value matches the pattern of Confluent's internal customer ids.
   */
  public static boolean isConfluentCustomer(String customerId) {
    return customerId != null
           && (CUSTOMER_PATTERN.matcher(customerId.toLowerCase(Locale.ROOT)).matches()
               || NEW_CUSTOMER_CASE_INSENSISTIVE_PATTERN.matcher(customerId).matches()
               || NEW_CUSTOMER_CASE_SENSISTIVE_PATTERN.matcher(customerId).matches());
  }

  /**
   * @param customerId The value of "confluent.support.customer.id".
   * @return True if the value is syntactically correct.
   */
  public static boolean isSyntacticallyCorrectCustomerId(String customerId) {
    return isAnonymousUser(customerId) || isConfluentCustomer(customerId);
  }

  /**
   * The 15-character alpha-numeric customer IDs are case-sensitive, others are case-insensitive.
   * The old-style customer ID "c\\d{1,30}" maybe look the same as a 15-character alpha-numeric ID,
   * if its length is also 15 characters. In that case, this method will return true.
   * @param customerId The value of "confluent.support.customer.id".
   * @return true if customer Id is case sensitive
   */
  public static boolean isCaseSensitiveCustomerId(String customerId) {
    return NEW_CUSTOMER_CASE_SENSISTIVE_PATTERN.matcher(customerId).matches();
  }

  public String getCustomerId() {
    return getCustomerId(properties);
  }

  public static String getCustomerId(Properties props) {
    String fallbackId = BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;
    String id = props.getProperty(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG);
    if (id == null || id.isEmpty()) {
      log.info("No customer ID configured -- falling back to id '{}'", fallbackId);
      id = fallbackId;
    }
    if (!isSyntacticallyCorrectCustomerId(id)) {
      log.error(
          "'{}' is not a valid Confluent customer ID -- falling back to id '{}'",
          id,
          fallbackId
      );
      id = fallbackId;
    }
    return id;
  }

  public long getReportIntervalMs() {
    String intervalString =
        properties.getProperty(
            BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG
        );
    if (intervalString == null || intervalString.isEmpty()) {
      intervalString = BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_DEFAULT;
    }
    try {
      long intervalHours = Long.parseLong(intervalString);
      if (intervalHours < 1) {
        throw new ConfigException(
            BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
            intervalString,
            "Interval must be >= 1"
        );
      }
      return intervalHours * 60 * 60 * 1000;
    } catch (NumberFormatException e) {
      throw new ConfigException(
          BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
          intervalString,
          "Interval is not an integer number"
      );
    }
  }

  public String getKafkaTopic() {
    return properties.getProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "");
  }

  public boolean getMetricsEnabled() {
    String enableString = properties
        .getProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    return Boolean.parseBoolean(enableString);
  }

  public boolean isHttpEnabled() {
    String enableHTTP =
        properties.getProperty(
            BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
            BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_DEFAULT
        );
    return Boolean.parseBoolean(enableHTTP);
  }

  public String getEndpointHTTP() {
    return properties
        .getProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG, "");
  }

  public void setEndpointHTTP(String endpointHTTP) {

    properties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG,
        endpointHTTP
    );
  }

  public boolean isHttpsEnabled() {
    String enableHTTPS =
        properties.getProperty(
            BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG,
            BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_DEFAULT
        );
    return Boolean.parseBoolean(enableHTTPS);
  }

  public String getEndpointHTTPS() {
    return properties.getProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG,
        ""
    );
  }

  public void setEndpointHTTPS(String endpointHTTPS) {
    properties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG,
        endpointHTTPS
    );
  }

  public String getProxy() {
    return properties.getProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_CONFIG,
        BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_DEFAULT
    );
  }
}

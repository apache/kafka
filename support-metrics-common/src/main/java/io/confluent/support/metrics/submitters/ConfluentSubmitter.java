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

package io.confluent.support.metrics.submitters;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import io.confluent.support.metrics.BaseSupportConfig;
import io.confluent.support.metrics.utils.StringUtils;
import io.confluent.support.metrics.utils.WebClient;

public class ConfluentSubmitter implements Submitter {

  private static final Logger log = LoggerFactory.getLogger(ConfluentSubmitter.class);

  private final String customerId;
  private final String endpointHTTP;
  private final String endpointHTTPS;
  private HttpHost proxy;
  private ResponseHandler responseHandler;

  public String getProxy() {
    return proxy == null ? null : proxy.toString();
  }

  public void setProxy(String name, int port, String scheme) {
    this.proxy = new HttpHost(name, port, scheme);
  }

  /**
   * Class that decides how to send data to Confluent.
   *
   * @param endpointHTTP HTTP endpoint for the Confluent support service. Can be null.
   * @param endpointHTTPS HTTPS endpoint for the Confluent support service. Can be null.
   */
  public ConfluentSubmitter(String customerId, String endpointHTTP, String endpointHTTPS) {
    this(customerId, endpointHTTP, endpointHTTPS, null, null);
  }

  /**
   * Constructor for phone-home clients which use ConfluentSubmitter directly instead of
   * BaseMetricsReporter and, as a result, don't need PhoneHomeConfig.
   * Sets customerId = "anonymous"
   */
  public ConfluentSubmitter(String componentId, ResponseHandler responseHandler) {
    this(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT, componentId, responseHandler);
  }

  /**
   * Also constructor for phone-home clients which use ConfluentSubmitter directly, but lets set
   * customer ID.
   * Common use-case is customerId = "anonymous" (BaseSupportConfig
   * .CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT) for production code, and "c0"
   * (BaseSupportConfig.CONFLUENT_SUPPORT_TEST_ID_DEFAULT) for internal testing. E.g. if we don't
   * want to include any internal testing data to aggregations and analysis we do on S3 data that
   * the server stores.
   * If customerID is "anonymous", then the endpoint path ends in "/anon", and if customerId is
   * "c0", then the endpoint path ends in "/test". CustomerId is also included in the body of
   * the phone-home ping.
   */
  public ConfluentSubmitter(
      String customerId, String componentId, ResponseHandler responseHandler
  ) {
    this(customerId,
         BaseSupportConfig.getEndpoint(false, customerId, componentId),
         BaseSupportConfig.getEndpoint(true, customerId, componentId),
         BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_DEFAULT,
         responseHandler);
  }

  public ConfluentSubmitter(
      String customerId,
      String endpointHTTP,
      String endpointHTTPS,
      String proxyURIString,
      ResponseHandler responseHandler
  ) {

    if (StringUtils.isNullOrEmpty(endpointHTTP) && StringUtils.isNullOrEmpty(endpointHTTPS)) {
      throw new IllegalArgumentException("must specify endpoints");
    }
    if (!StringUtils.isNullOrEmpty(endpointHTTP)) {
      if (!endpointHTTP.startsWith("http://")) {
        throw new IllegalArgumentException("invalid HTTP endpoint " + endpointHTTP);
      }
    }
    if (!StringUtils.isNullOrEmpty(endpointHTTPS)) {
      if (!endpointHTTPS.startsWith("https://")) {
        throw new IllegalArgumentException("invalid HTTPS endpoint " + endpointHTTPS);
      }
    }
    if (!BaseSupportConfig.isSyntacticallyCorrectCustomerId(customerId)) {
      throw new IllegalArgumentException("invalid customer ID "  + customerId);
    }
    this.endpointHTTP = endpointHTTP;
    this.endpointHTTPS = endpointHTTPS;
    this.customerId = customerId;
    this.responseHandler = responseHandler;

    if (!StringUtils.isNullOrEmpty(proxyURIString)) {
      URI proxyURI = URI.create(proxyURIString);
      this.setProxy(proxyURI.getHost(), proxyURI.getPort(), proxyURI.getScheme());
    }
  }

  /**
   * Submits metrics to Confluent via the Internet.  Ignores null or empty inputs.
   */
  @Override
  public void submit(byte[] bytes) {
    if (bytes != null && bytes.length > 0) {
      if (isSecureEndpointEnabled()) {
        if (!submittedSuccessfully(sendSecurely(bytes))) {
          if (isInsecureEndpointEnabled()) {
            log.error(
                "Failed to submit metrics via secure endpoint, falling back to insecure endpoint"
            );
            submitToInsecureEndpoint(bytes);
          } else {
            log.error(
                "Failed to submit metrics via secure endpoint={} -- giving up",
                endpointHTTPS
            );
          }
        } else {
          log.info("Successfully submitted metrics to Confluent via secure endpoint");
        }
      } else {
        if (isInsecureEndpointEnabled()) {
          submitToInsecureEndpoint(bytes);
        } else {
          log.error("Metrics will not be submitted because all endpoints are disabled");
        }
      }
    } else {
      log.error("Could not submit metrics to Confluent (metrics data missing)");
    }
  }

  private void submitToInsecureEndpoint(byte[] encodedMetricsRecord) {
    int statusCode = sendInsecurely(encodedMetricsRecord);
    if (submittedSuccessfully(statusCode)) {
      log.info("Successfully submitted metrics to Confluent via insecure endpoint");
    } else {
      log.error(
          "Failed to submit metrics to Confluent via insecure endpoint={} -- giving up",
          endpointHTTP
      );
    }
  }

  private boolean isSecureEndpointEnabled() {
    return !endpointHTTPS.isEmpty();
  }

  private boolean isInsecureEndpointEnabled() {
    return !endpointHTTP.isEmpty();
  }

  /**
   * Getters for testing
   */
  String getEndpointHTTP() {
    return endpointHTTP;
  }

  String getEndpointHTTPS() {
    return endpointHTTPS;
  }

  private boolean submittedSuccessfully(int statusCode) {
    return statusCode == HttpStatus.SC_OK;
  }

  private int sendSecurely(byte[] encodedMetricsRecord) {
    return send(encodedMetricsRecord, endpointHTTPS);
  }

  private int sendInsecurely(byte[] encodedMetricsRecord) {
    return send(encodedMetricsRecord, endpointHTTP);
  }

  private int send(byte[] encodedMetricsRecord, String endpoint) {
    return WebClient
        .send(customerId, encodedMetricsRecord, new HttpPost(endpoint), proxy, responseHandler);
  }
}

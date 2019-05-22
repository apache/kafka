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

package io.confluent.support.metrics.utils;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.support.metrics.submitters.ResponseHandler;

public class WebClient {

  private static final Logger log = LoggerFactory.getLogger(WebClient.class);
  private static final int REQUEST_TIMEOUT_MS = 2000;
  public static final int DEFAULT_STATUS_CODE = HttpStatus.SC_BAD_GATEWAY;

  /**
   * Sends a POST request to a web server
   * This method requires a pre-configured http client instance
   *
   * @param customerId customer Id on behalf of which the request is sent
   * @param bytes request payload
   * @param httpPost A POST request structure
   * @param proxy a http (passive) proxy
   * @param httpClient http client instance configured by caller
   * @return an HTTP Status code
   * @see #send(String, byte[], HttpPost, ResponseHandler)
   */
  protected static int send(
      String customerId, byte[] bytes, HttpPost httpPost, HttpHost proxy,
      CloseableHttpClient httpClient, ResponseHandler responseHandler
  ) {
    int statusCode = DEFAULT_STATUS_CODE;
    if (bytes != null && bytes.length > 0 && httpPost != null && customerId != null) {

      // add the body to the request
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
      builder.addTextBody("cid", customerId);
      builder.addBinaryBody("file", bytes, ContentType.DEFAULT_BINARY, "filename");
      httpPost.setEntity(builder.build());
      httpPost.addHeader("api-version", "phone-home-v1");

      // set the HTTP config
      RequestConfig config = RequestConfig.custom()
          .setConnectTimeout(REQUEST_TIMEOUT_MS)
          .setConnectionRequestTimeout(REQUEST_TIMEOUT_MS)
          .setSocketTimeout(REQUEST_TIMEOUT_MS)
          .build();

      CloseableHttpResponse response = null;

      try {
        if (proxy != null) {
          log.debug("setting proxy to {}", proxy);
          config = RequestConfig.copy(config).setProxy(proxy).build();
          httpPost.setConfig(config);
          DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(proxy);
          if (httpClient == null) {
            httpClient = HttpClientBuilder
                .create()
                .setRoutePlanner(routePlanner)
                .setDefaultRequestConfig(config)
                .build();
          }
        } else {
          if (httpClient == null) {
            httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
          }
        }

        response = httpClient.execute(httpPost);
        if (responseHandler != null) {
          responseHandler.handle(response);
        }

        // send request
        log.debug("POST request returned {}", response.getStatusLine().toString());
        statusCode = response.getStatusLine().getStatusCode();
      } catch (IOException e) {
        log.error("Could not submit metrics to Confluent: {}", e.getMessage());
      } finally {
        if (httpClient != null) {
          try {
            httpClient.close();
          } catch (IOException e) {
            log.warn("could not close http client", e);
          }
        }
        if (response != null) {
          try {
            response.close();
          } catch (IOException e) {
            log.warn("could not close http response", e);
          }
        }
      }
    } else {
      statusCode = HttpStatus.SC_BAD_REQUEST;
    }
    return statusCode;
  }

  /**
   * Sends a POST request to a web server via a HTTP proxy
   *
   * @param customerId customer Id on behalf of which the request is sent
   * @param bytes request payload
   * @param httpPost A POST request structure
   * @param proxy a http (passive) proxy
   * @return an HTTP Status code
   */
  public static int send(
      String customerId,
      byte[] bytes,
      HttpPost httpPost,
      HttpHost proxy,
      ResponseHandler responseHandler
  ) {
    return send(customerId, bytes, httpPost, proxy, null, responseHandler);
  }

  /**
   * Sends a POST request to a web server
   *
   * @param customerId customer Id on behalf of which the request is sent
   * @param bytes request payload
   * @param httpPost A POST request structure
   * @return an HTTP Status code
   */
  public static int send(
      String customerId,
      byte[] bytes,
      HttpPost httpPost,
      ResponseHandler responseHandler
  ) {
    return send(customerId, bytes, httpPost, null, responseHandler);
  }

}

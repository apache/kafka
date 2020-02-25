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

import io.confluent.support.metrics.BaseSupportConfig;
import io.confluent.support.metrics.utils.CustomerIdExamples;
import io.confluent.support.metrics.utils.StringUtils;
import org.apache.http.HttpResponse;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfluentSubmitterTest {

  private String customerId = BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT;


  @Test
  public void testInvalidArgumentsForConstructorNullEndpoints() {
    // Given
    String httpEndpoint = null;
    String httpsEndpoint = null;

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must specify endpoints"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorEmptyEndpoints() {
    // Given
    String httpEndpoint = "";
    String httpsEndpoint = "";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are empty");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must specify endpoints"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorNullEmptyEndpoints() {
    // Given
    String httpEndpoint = null;
    String httpsEndpoint = "";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are null/empty");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must specify endpoints"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorEmptyNullEndpoints() {
    // Given
    String httpEndpoint = "";
    String httpsEndpoint = null;

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are empty/null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must specify endpoints"));
    }
  }

  @Test
  public void testValidArgumentsForConstructor() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
  }

  @Test
  public void testValidArgumentsForPhoneHomeConstructorWithoutCustomerId() {
    ConfluentSubmitter submitter = new ConfluentSubmitter("test-component", null);
    assertTrue(StringUtils.isNullOrEmpty(submitter.getProxy()));
    assertEquals(BaseSupportConfig.getEndpoint(
        false,
        BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT,
        "test-component"),
        submitter.getEndpointHTTP());
    assertEquals(BaseSupportConfig.getEndpoint(
        true,
        BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT,
        "test-component"),
        submitter.getEndpointHTTPS());
  }

  @Test
  public void testValidArgumentsForPhoneHomeConstructorWithCustomerId() {
    final String customerId = BaseSupportConfig.CONFLUENT_SUPPORT_TEST_ID_DEFAULT;
    final ResponseHandler responseHandler = new ResponseHandler() {
      @Override
      public void handle(HttpResponse response) {
        //
      }
    };

    ConfluentSubmitter submitter =
        new ConfluentSubmitter(customerId, "test-component", responseHandler);
    assertTrue(StringUtils.isNullOrEmpty(submitter.getProxy()));
    assertEquals(BaseSupportConfig.getEndpoint(false, customerId, "test-component"),
        submitter.getEndpointHTTP());
    assertEquals(BaseSupportConfig.getEndpoint(true, customerId, "test-component"),
        submitter.getEndpointHTTPS());
  }

  @Test
  public void testInvalidArgumentsForConstructorInvalidHttpEndpoint() {
    // Given
    String httpEndpoint = "invalid URL";
    String httpsEndpoint = "https://example.com";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are invalid");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("invalid HTTP endpoint"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorInvalidHttpsEndpoint() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "invalid URL";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint);
      fail("IllegalArgumentException expected because endpoints are invalid");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("invalid HTTPS endpoint"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorMismatchedEndpoints() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    try {
      new ConfluentSubmitter(customerId, httpsEndpoint, httpEndpoint);
      fail("IllegalArgumentException expected because endpoints were provided in the wrong order");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().startsWith("invalid HTTP endpoint"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorCustomerIdInvalid() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    for (String invalidCustomerId : CustomerIdExamples.INVALID_CUSTOMER_IDS) {
      try {
        new ConfluentSubmitter(invalidCustomerId, httpEndpoint, httpsEndpoint);
        fail("IllegalArgumentException expected because customer ID is invalid");
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().startsWith("invalid customer ID"));
      }
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorAnonymousIdInvalid() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";

    // When/Then
    for (String invalidCustomerId : CustomerIdExamples.INVALID_ANONYMOUS_IDS) {
      try {
        new ConfluentSubmitter(invalidCustomerId, httpEndpoint, httpsEndpoint);
        fail("IllegalArgumentException expected because customer ID is invalid");
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().startsWith("invalid customer ID"));
      }
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorWithProxy() {
    // Given
    String httpEndpoint = "http://example.com";
    String httpsEndpoint = "https://example.com";
    String proxyURI = null;

    // When/Then
    ConfluentSubmitter submitter = new ConfluentSubmitter(customerId, httpEndpoint,
        httpsEndpoint, proxyURI, null);
    assertTrue(submitter.getProxy() == null);

    proxyURI = "https://proxy.example.com";
    submitter = new ConfluentSubmitter(customerId, httpEndpoint, httpsEndpoint, proxyURI, null);
    assertEquals(proxyURI, submitter.getProxy());
  }
}

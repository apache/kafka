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

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.concurrent.ThreadLocalRandom;

public class WebClientTest {
  private String customerId = CustomerIdExamples.VALID_CUSTOMER_IDS[0];
  private static final String SECURE_LIVE_TEST_ENDPOINT = "https://support-metrics.confluent.io/test";

  @Test
  public void testSubmitIgnoresNullInput() {
    // Given
    HttpPost p = mock(HttpPost.class);
    byte[] nullData = null;

    // When
    WebClient.send(customerId, nullData, p, null);

    // Then
    verifyZeroInteractions(p);
  }

  @Test
  public void testSubmitIgnoresEmptyInput() {
    // Given
    HttpPost p = mock(HttpPost.class);
    byte[] emptyData = new byte[0];

    // When
    WebClient.send(customerId, emptyData, p, null);

    // Then
    verifyZeroInteractions(p);
  }

  @Test
  public void testSubmitInvalidCustomer() {
    // Given
    HttpPost p = new HttpPost(SECURE_LIVE_TEST_ENDPOINT);
    byte[] anyData = "anyData".getBytes();
    int randomIndex = ThreadLocalRandom.current().nextInt(CustomerIdExamples.INVALID_CUSTOMER_IDS.length);
    String invalidCustomerId = CustomerIdExamples.INVALID_CUSTOMER_IDS[randomIndex];

    // When/Then
    assertNotEquals("customerId=" + invalidCustomerId,
                    HttpStatus.SC_OK, WebClient.send(invalidCustomerId, anyData, p, null));
  }

  @Test
  public void testSubmitInvalidAnonymousUser() {
    // Given
    HttpPost p = new HttpPost(SECURE_LIVE_TEST_ENDPOINT);
    byte[] anyData = "anyData".getBytes();
    int randomIndex = ThreadLocalRandom.current().nextInt(CustomerIdExamples.INVALID_ANONYMOUS_IDS.length);
    String invalidCustomerId = CustomerIdExamples.INVALID_ANONYMOUS_IDS[randomIndex];

    // When/Then
    assertNotEquals("customerId=" + invalidCustomerId,
                    HttpStatus.SC_OK, WebClient.send(invalidCustomerId, anyData, p, null));
  }

  @Test
  public void testSubmitValidCustomer() {
    // Given
    HttpPost p = new HttpPost(SECURE_LIVE_TEST_ENDPOINT);
    byte[] anyData = "anyData".getBytes();
    int randomIndex = ThreadLocalRandom.current().nextInt(CustomerIdExamples.VALID_CUSTOMER_IDS.length);
    String validCustomerId = CustomerIdExamples.VALID_CUSTOMER_IDS[randomIndex];

    // When/Then
    int status = WebClient.send(validCustomerId, anyData, p, null);
    // if we are not connected to the internet this test should still pass
    assertTrue("customerId=" + validCustomerId,
               status == HttpStatus.SC_OK || status == HttpStatus.SC_BAD_GATEWAY);
  }

  @Test
  public void testSubmitValidAnonymousUser() {
    // Given
    HttpPost p = new HttpPost(SECURE_LIVE_TEST_ENDPOINT);
    byte[] anyData = "anyData".getBytes();
    int randomIndex = ThreadLocalRandom.current().nextInt(CustomerIdExamples.VALID_ANONYMOUS_IDS.length);
    String validCustomerId = CustomerIdExamples.VALID_ANONYMOUS_IDS[randomIndex];

    // When/Then
    int status = WebClient.send(validCustomerId, anyData, p, null);
    // if we are not connected to the internet this test should still pass
    assertTrue("customerId=" + validCustomerId,
               status == HttpStatus.SC_OK || status == HttpStatus.SC_BAD_GATEWAY);
  }

}

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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.support.metrics.common.kafka.ZkClientProvider;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class KafkaSubmitterTest {

  private static ZkClientProvider zkClientProvider;

  @BeforeClass
  public static void startCluster() {
    zkClientProvider = mock(ZkClientProvider.class);
  }

  @Test
  public void testInvalidArgumentsForConstructorNullServer() {
    // Given
    ZkClientProvider nullServer = null;
    String anyTopic = "valueNotRelevant";

    // When/Then
    try {
      new KafkaSubmitter(nullServer, anyTopic);
      fail("IllegalArgumentException expected because server is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must specify zkClientProvider"));
    }
  }


  @Test
  public void testInvalidArgumentsForConstructorNullTopic() {
    // Given
    String nullTopic = null;

    // When/Then
    try {
      new KafkaSubmitter(zkClientProvider, nullTopic);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must specify topic"));
    }
  }

  @Test
  public void testInvalidArgumentsForConstructorEmptyTopic() {
    // Given
    String emptyTopic = "";

    // When/Then
    try {
      new KafkaSubmitter(zkClientProvider, emptyTopic);
      fail("IllegalArgumentException expected because topic is the empty string");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must specify topic"));
    }
  }

  @Test
  public void testSubmitIgnoresNullInput() {
    // Given
    String anyTopic = "valueNotRelevant";
    KafkaSubmitter k = new KafkaSubmitter(zkClientProvider, anyTopic);
    Producer<byte[], byte[]> producer = mock(Producer.class);
    byte[] nullData = null;

    // When
    k.submit(nullData, producer);

    // Then
    verifyZeroInteractions(producer);
  }

  @Test
  public void testSubmitIgnoresEmptyInput() {
    // Given
    String anyTopic = "valueNotRelevant";
    KafkaSubmitter k = new KafkaSubmitter(zkClientProvider, anyTopic);
    Producer<byte[], byte[]> producer = mock(Producer.class);
    byte[] emptyData = new byte[0];

    // When
    k.submit(emptyData, producer);

    // Then
    verifyZeroInteractions(producer);
  }

  @Test
  public void testSubmit() {
    // Given
    String anyTopic = "valueNotRelevant";
    KafkaSubmitter k = new KafkaSubmitter(zkClientProvider, anyTopic);
    Producer<byte[], byte[]> producer = mock(Producer.class);
    byte[] anyData = "anyData".getBytes();

    // When
    k.submit(anyData, producer);

    // Then
    verify(producer).send(any(ProducerRecord.class));
  }

}

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
package io.confluent.support.metrics.common.kafka;

import kafka.zk.KafkaZkClient;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import kafka.cluster.Broker;
import kafka.server.KafkaServer;
import scala.collection.JavaConversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The tests in this class should not be run in parallel.  This limitation is caused by how the
 * current implementation of EmbeddedKafkaCLuster works.
 */
public class KafkaUtilitiesTest {
  private static final KafkaZkClient MOCK_ZK_CLIENT = mock(KafkaZkClient.class);
  private static final int ANY_MAX_NUM_SERVERS = 2;
  private static final String ANY_TOPIC = "valueNotRelevant";
  private static final int ANY_PARTITIONS = 1;
  private static final int ANY_REPLICATION = 1;
  private static final long ANY_RETENTION_MS = 1000L;
  private static final long ONE_YEAR_RETENTION = 365 * 24 * 60 * 60 * 1000L;
  private static final String[] EXAMPLE_TOPICS = {"__confluent.support.metrics", "ANY_TOPIC", "basketball"};

  @Test
  public void getNumTopicsThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.getNumTopics(null);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }

  @Test
  public void getNumTopicsReturnsMinusOneOnError() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    when(zkClient.getAllTopicsInCluster()).thenThrow(new RuntimeException("exception intentionally thrown by test"));

    // When/Then
    assertEquals(-1L, kUtil.getNumTopics(zkClient));
  }

  @Test
  public void getNumTopicsReturnsZeroWhenThereAreNoTopics() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    List<String> zeroTopics = new ArrayList<>();
    when(zkClient.getAllTopicsInCluster()).thenReturn(JavaConversions.asScalaBuffer(zeroTopics).toList());

    // When/Then
    assertEquals(0L, kUtil.getNumTopics(zkClient));
  }

  @Test
  public void getNumTopicsReturnsCorrectNumber() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    List<String> topics = new ArrayList<>();
    topics.add("topic1");
    topics.add("topic2");
    when(zkClient.getAllTopicsInCluster()).thenReturn(JavaConversions.asScalaBuffer(topics).toList());

    // When/Then
    assertEquals(topics.size(), kUtil.getNumTopics(zkClient));
  }

  @Test
  public void getBootstrapServersThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.getBootstrapServers(null, ANY_MAX_NUM_SERVERS);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }

  @Test
  public void getBootstrapServersThrowsIAEWhenMaxNumServersIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroMaxNumServers = 0;

    // When/Then
    try {
      kUtil.getBootstrapServers(MOCK_ZK_CLIENT, zeroMaxNumServers);
      fail("IllegalArgumentException expected because max number of servers is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void getBootstrapServersReturnsEmptyListWhenThereAreNoLiveBrokers() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    List<Broker> empty = new ArrayList<>();
    when(zkClient.getAllBrokersInCluster()).thenReturn(JavaConversions.asScalaBuffer(empty).toList());

    // When/Then
    assertTrue(kUtil.getBootstrapServers(zkClient, ANY_MAX_NUM_SERVERS).isEmpty());
  }

  @Test
  public void createTopicThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.createAndVerifyTopic(null, ANY_TOPIC, ANY_PARTITIONS, ANY_REPLICATION, ANY_RETENTION_MS);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenTopicIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(MOCK_ZK_CLIENT, nullTopic, ANY_PARTITIONS, ANY_REPLICATION, ANY_RETENTION_MS);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenTopicIsEmpty() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";

    // When/Then
    try {
      kUtil.createAndVerifyTopic(MOCK_ZK_CLIENT, emptyTopic, ANY_PARTITIONS, ANY_REPLICATION, ANY_RETENTION_MS);
      fail("IllegalArgumentException expected because topic is empty");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroPartitions = 0;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(MOCK_ZK_CLIENT, ANY_TOPIC, zeroPartitions, ANY_REPLICATION, ANY_RETENTION_MS);
      fail("IllegalArgumentException expected because number of partitions is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenReplicationFactorIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroReplication = 0;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(MOCK_ZK_CLIENT, ANY_TOPIC, ANY_PARTITIONS, zeroReplication, ANY_RETENTION_MS);
      fail("IllegalArgumentException expected because replication factor is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenRetentionMsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    long zeroRetentionMs = 0;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(MOCK_ZK_CLIENT, ANY_TOPIC, ANY_PARTITIONS, ANY_REPLICATION, zeroRetentionMs);
      fail("IllegalArgumentException expected because retention.ms is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.verifySupportTopic(null, ANY_TOPIC, ANY_PARTITIONS, ANY_REPLICATION);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }


  @Test
  public void verifySupportTopicThrowsIAEWhenTopicIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;

    // When/Then
    try {
      kUtil.verifySupportTopic(MOCK_ZK_CLIENT, nullTopic, ANY_PARTITIONS, ANY_REPLICATION);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenTopicIsEmpty() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";

    // When/Then
    try {
      kUtil.verifySupportTopic(MOCK_ZK_CLIENT, emptyTopic, ANY_PARTITIONS, ANY_REPLICATION);
      fail("IllegalArgumentException expected because topic is empty");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroPartitions = 0;

    // When/Then
    try {
      kUtil.verifySupportTopic(MOCK_ZK_CLIENT, ANY_TOPIC, zeroPartitions, ANY_REPLICATION);
      fail("IllegalArgumentException expected because number of partitions is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenReplicationFactorIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroReplication = 0;

    // When/Then
    try {
      kUtil.verifySupportTopic(MOCK_ZK_CLIENT, ANY_TOPIC, ANY_PARTITIONS, zeroReplication);
      fail("IllegalArgumentException expected because replication factor is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void underreplicatedTopicsCanBeCreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    int partitions = numBrokers + 1;
    int replication = numBrokers + 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient zkClient = broker.zkClient();

    // When/Then
    for (String topic : EXAMPLE_TOPICS) {
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, ONE_YEAR_RETENTION));
      // Only one broker is up, so the actual number of replicas will be only 1.
      assertEquals(KafkaUtilities.VerifyTopicState.Less,
          kUtil.verifySupportTopic(zkClient, topic, partitions, replication));
    }
    assertEquals(EXAMPLE_TOPICS.length, kUtil.getNumTopics(zkClient));

    // Cleanup
    cluster.stopCluster();
    zkClient.close();
  }


  @Test
  public void underreplicatedTopicsCanBeRecreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    int partitions = numBrokers + 1;
    int replication = numBrokers + 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient zkClient = broker.zkClient();

    // When/Then
    for (String topic : EXAMPLE_TOPICS) {
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, ONE_YEAR_RETENTION));
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, ONE_YEAR_RETENTION));
      assertEquals(KafkaUtilities.VerifyTopicState.Less,
          kUtil.verifySupportTopic(zkClient, topic, partitions, replication));
    }
    assertEquals(EXAMPLE_TOPICS.length, kUtil.getNumTopics(zkClient));

    // Cleanup
    cluster.stopCluster();
  }

  @Test
  public void createTopicFailsWhenThereAreNoLiveBrokers() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    // Provide us with a realistic but, once the cluster is stopped, defunct instance of zkutils.
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    cluster.startCluster(1);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient defunctZkClient = broker.zkClient();
    cluster.stopCluster();

    // When/Then
    assertFalse(kUtil.createAndVerifyTopic(defunctZkClient, ANY_TOPIC, ANY_PARTITIONS, ANY_REPLICATION, ANY_RETENTION_MS));
  }

  @Test
  public void replicatedTopicsCanBeCreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 3;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient zkClient = broker.zkClient();
    Random random = new Random();
    int replication = numBrokers;

    // When/Then
    for (String topic : EXAMPLE_TOPICS) {
      int morePartitionsThanBrokers = random.nextInt(10) + numBrokers + 1;
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, morePartitionsThanBrokers, replication, ONE_YEAR_RETENTION));
      assertEquals(KafkaUtilities.VerifyTopicState.Exactly,
          kUtil.verifySupportTopic(zkClient, topic, morePartitionsThanBrokers, replication));
    }

    // Cleanup
    cluster.stopCluster();
  }

  @Test
  public void leaderIsElectedAfterCreateTopicReturns() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 3;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient zkClient = broker.zkClient();
    int replication = numBrokers;

    assertTrue(kUtil.createAndVerifyTopic(zkClient, ANY_TOPIC, ANY_PARTITIONS, replication,
                                          ONE_YEAR_RETENTION));
    assertTrue(zkClient.getLeaderForPartition(
        new TopicPartition(ANY_TOPIC, 0)).isDefined());
  }

}

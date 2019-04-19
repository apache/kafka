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
  *//**
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
package io.confluent.support.metrics.common.kafka

import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import java.util
import java.util.Random
import kafka.cluster.Broker
import scala.collection.JavaConversions
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.mockito.Mockito.mock
import org.mockito.Mockito.when

/**
  * The tests in this class should not be run in parallel.  This limitation is caused by how the
  * current implementation of EmbeddedKafkaCLuster works.
  */
object KafkaUtilitiesTest {
  private val mockZkClient = mock(classOf[KafkaZkClient])
  private val anyMaxNumServers = 2
  private val anyTopic = "valueNotRelevant"
  private val anyPartitions = 1
  private val anyReplication = 1
  private val anyRetentionMs = 1000L
  private val oneYearRetention = 365 * 24 * 60 * 60 * 1000L
  private val exampleTopics = Array("__confluent.support.metrics", "anyTopic", "basketball")
}

class KafkaUtilitiesTest {
  @Test def getNumTopicsThrowsIAEWhenKafkaZkClientIsNull(): Unit = { // Given
    val kUtil = new KafkaUtilities
    // When/Then
    try {
      kUtil.getNumTopics(null)
      fail("IllegalArgumentException expected because zkClient is null")
    } catch {
      case e: NullPointerException =>

      // ignore
    }
  }

  @Test def getNumTopicsReturnsMinusOneOnError(): Unit = {
    val kUtil = new KafkaUtilities
    val zkClient = mock(classOf[KafkaZkClient])
    when(zkClient.getAllTopicsInCluster).thenThrow(new RuntimeException("exception intentionally thrown by test"))
    assertEquals(-1L, kUtil.getNumTopics(zkClient))
  }

  @Test def getNumTopicsReturnsZeroWhenThereAreNoTopics(): Unit = {
    val kUtil = new KafkaUtilities
    val zkClient = mock(classOf[KafkaZkClient])
    val zeroTopics = new util.ArrayList[String]
    when(zkClient.getAllTopicsInCluster).thenReturn(JavaConversions.asScalaBuffer(zeroTopics).toList)
    assertEquals(0L, kUtil.getNumTopics(zkClient))
  }

  @Test def getNumTopicsReturnsCorrectNumber(): Unit = {
    val kUtil = new KafkaUtilities
    val zkClient = mock(classOf[KafkaZkClient])
    val topics = new util.ArrayList[String]
    topics.add("topic1")
    topics.add("topic2")
    when(zkClient.getAllTopicsInCluster).thenReturn(JavaConversions.asScalaBuffer(topics).toList)
    assertEquals(topics.size, kUtil.getNumTopics(zkClient))
  }

  @Test def getBootstrapServersThrowsIAEWhenKafkaZkClientIsNull(): Unit = {
    val kUtil = new KafkaUtilities
    try {
      kUtil.getBootstrapServers(null, KafkaUtilitiesTest.anyMaxNumServers)
      fail("IllegalArgumentException expected because zkClient is null")
    } catch {
      case e: NullPointerException =>
    }
  }

  @Test def getBootstrapServersThrowsIAEWhenMaxNumServersIsZero(): Unit = {
    val kUtil = new KafkaUtilities
    val zeroMaxNumServers = 0
    try {
      kUtil.getBootstrapServers(KafkaUtilitiesTest.mockZkClient, zeroMaxNumServers)
      fail("IllegalArgumentException expected because max number of servers is zero")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def getBootstrapServersReturnsEmptyListWhenThereAreNoLiveBrokers(): Unit = {
    val kUtil = new KafkaUtilities
    val zkClient = mock(classOf[KafkaZkClient])
    val empty = new util.ArrayList[Broker]
    when(zkClient.getAllBrokersInCluster).thenReturn(JavaConversions.asScalaBuffer(empty).toList)
    assertTrue(kUtil.getBootstrapServers(zkClient, KafkaUtilitiesTest.anyMaxNumServers).isEmpty)
  }

  @Test def createTopicThrowsIAEWhenKafkaZkClientIsNull(): Unit = {
    val kUtil = new KafkaUtilities
    try {
      kUtil.createAndVerifyTopic(null, KafkaUtilitiesTest.anyTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication, KafkaUtilitiesTest.anyRetentionMs)
      fail("IllegalArgumentException expected because zkClient is null")
    } catch {
      case e: NullPointerException =>
    }
  }

  @Test def createTopicThrowsIAEWhenTopicIsNull(): Unit = {
    val kUtil = new KafkaUtilities
    val nullTopic = null
    try {
      kUtil.createAndVerifyTopic(KafkaUtilitiesTest.mockZkClient, nullTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication, KafkaUtilitiesTest.anyRetentionMs)
      fail("IllegalArgumentException expected because topic is null")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def createTopicThrowsIAEWhenTopicIsEmpty(): Unit = {
    val kUtil = new KafkaUtilities
    val emptyTopic = ""
    try {
      kUtil.createAndVerifyTopic(KafkaUtilitiesTest.mockZkClient, emptyTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication, KafkaUtilitiesTest.anyRetentionMs)
      fail("IllegalArgumentException expected because topic is empty")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def createTopicThrowsIAEWhenNumberOfPartitionsIsZero(): Unit = {
    val kUtil = new KafkaUtilities
    val zeroPartitions = 0
    try {
      kUtil.createAndVerifyTopic(KafkaUtilitiesTest.mockZkClient, KafkaUtilitiesTest.anyTopic, zeroPartitions, KafkaUtilitiesTest.anyReplication, KafkaUtilitiesTest.anyRetentionMs)
      fail("IllegalArgumentException expected because number of partitions is zero")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def createTopicThrowsIAEWhenReplicationFactorIsZero(): Unit = {
    val kUtil = new KafkaUtilities
    val zeroReplication = 0
    try {
      kUtil.createAndVerifyTopic(KafkaUtilitiesTest.mockZkClient, KafkaUtilitiesTest.anyTopic, KafkaUtilitiesTest.anyPartitions, zeroReplication, KafkaUtilitiesTest.anyRetentionMs)
      fail("IllegalArgumentException expected because replication factor is zero")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def createTopicThrowsIAEWhenRetentionMsIsZero(): Unit = {
    val kUtil = new KafkaUtilities
    val zeroRetentionMs = 0
    try {
      kUtil.createAndVerifyTopic(KafkaUtilitiesTest.mockZkClient, KafkaUtilitiesTest.anyTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication, zeroRetentionMs)
      fail("IllegalArgumentException expected because retention.ms is zero")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def verifySupportTopicThrowsIAEWhenKafkaZkClientIsNull(): Unit = {
    val kUtil = new KafkaUtilities
    try {
      kUtil.verifySupportTopic(null, KafkaUtilitiesTest.anyTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication)
      fail("IllegalArgumentException expected because zkClient is null")
    } catch {
      case e: NullPointerException =>
    }
  }

  @Test def verifySupportTopicThrowsIAEWhenTopicIsNull(): Unit = {
    val kUtil = new KafkaUtilities
    val nullTopic = null
    try {
      kUtil.verifySupportTopic(KafkaUtilitiesTest.mockZkClient, nullTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication)
      fail("IllegalArgumentException expected because topic is null")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def verifySupportTopicThrowsIAEWhenTopicIsEmpty(): Unit = {
    val kUtil = new KafkaUtilities
    val emptyTopic = ""
    try {
      kUtil.verifySupportTopic(KafkaUtilitiesTest.mockZkClient, emptyTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication)
      fail("IllegalArgumentException expected because topic is empty")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def verifySupportTopicThrowsIAEWhenNumberOfPartitionsIsZero(): Unit = {
    val kUtil = new KafkaUtilities
    val zeroPartitions = 0
    try {
      kUtil.verifySupportTopic(KafkaUtilitiesTest.mockZkClient, KafkaUtilitiesTest.anyTopic, zeroPartitions, KafkaUtilitiesTest.anyReplication)
      fail("IllegalArgumentException expected because number of partitions is zero")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def verifySupportTopicThrowsIAEWhenReplicationFactorIsZero(): Unit = {
    val kUtil = new KafkaUtilities
    val zeroReplication = 0
    try {
      kUtil.verifySupportTopic(KafkaUtilitiesTest.mockZkClient, KafkaUtilitiesTest.anyTopic, KafkaUtilitiesTest.anyPartitions, zeroReplication)
      fail("IllegalArgumentException expected because replication factor is zero")
    } catch {
      case e: IllegalArgumentException =>
    }
  }

  @Test def underreplicatedTopicsCanBeCreatedAndVerified(): Unit = {
    val kUtil = new KafkaUtilities
    val cluster = new EmbeddedKafkaCluster
    val numBrokers = 1
    val partitions = numBrokers + 1
    val replication = numBrokers + 1
    cluster.startCluster(numBrokers)
    val broker = cluster.getBroker(0)
    val zkClient = broker.zkClient
    for (topic <- KafkaUtilitiesTest.exampleTopics) {
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, KafkaUtilitiesTest.oneYearRetention))
      // Only one broker is up, so the actual number of replicas will be only 1.
      assertEquals(KafkaUtilities.VerifyTopicState.Less, kUtil.verifySupportTopic(zkClient, topic, partitions, replication))
    }
    assertEquals(KafkaUtilitiesTest.exampleTopics.length, kUtil.getNumTopics(zkClient))
    // Cleanup
    cluster.stopCluster()
    zkClient.close()
  }

  @Test def underreplicatedTopicsCanBeRecreatedAndVerified(): Unit = {
    val kUtil = new KafkaUtilities
    val cluster = new EmbeddedKafkaCluster
    val numBrokers = 1
    val partitions = numBrokers + 1
    val replication = numBrokers + 1
    cluster.startCluster(numBrokers)
    val broker = cluster.getBroker(0)
    val zkClient = broker.zkClient
    for (topic <- KafkaUtilitiesTest.exampleTopics) {
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, KafkaUtilitiesTest.oneYearRetention))
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, KafkaUtilitiesTest.oneYearRetention))
      assertEquals(KafkaUtilities.VerifyTopicState.Less, kUtil.verifySupportTopic(zkClient, topic, partitions, replication))
    }
    assertEquals(KafkaUtilitiesTest.exampleTopics.length, kUtil.getNumTopics(zkClient))
    cluster.stopCluster()
  }

  @Test def createTopicFailsWhenThereAreNoLiveBrokers(): Unit = {
    val kUtil = new KafkaUtilities
    // Provide us with a realistic but, once the cluster is stopped, defunct instance of zkutils.
    val cluster = new EmbeddedKafkaCluster
    cluster.startCluster(1)
    val broker = cluster.getBroker(0)
    val defunctZkClient = broker.zkClient
    cluster.stopCluster()
    assertFalse(kUtil.createAndVerifyTopic(defunctZkClient, KafkaUtilitiesTest.anyTopic, KafkaUtilitiesTest.anyPartitions, KafkaUtilitiesTest.anyReplication, KafkaUtilitiesTest.anyRetentionMs))
  }

  @Test def replicatedTopicsCanBeCreatedAndVerified(): Unit = {
    val kUtil = new KafkaUtilities
    val cluster = new EmbeddedKafkaCluster
    val numBrokers = 3
    cluster.startCluster(numBrokers)
    val broker = cluster.getBroker(0)
    val zkClient = broker.zkClient
    val random = new Random
    val replication = numBrokers
    for (topic <- KafkaUtilitiesTest.exampleTopics) {
      val morePartitionsThanBrokers = random.nextInt(10) + numBrokers + 1
      assertTrue(kUtil.createAndVerifyTopic(zkClient, topic, morePartitionsThanBrokers, replication, KafkaUtilitiesTest.oneYearRetention))
      assertEquals(KafkaUtilities.VerifyTopicState.Exactly, kUtil.verifySupportTopic(zkClient, topic, morePartitionsThanBrokers, replication))
    }
    cluster.stopCluster()
  }

  @Test def leaderIsElectedAfterCreateTopicReturns(): Unit = {
    val kUtil = new KafkaUtilities
    val cluster = new EmbeddedKafkaCluster
    val numBrokers = 3
    cluster.startCluster(numBrokers)
    val broker = cluster.getBroker(0)
    val zkClient = broker.zkClient
    val replication = numBrokers
    assertTrue(kUtil.createAndVerifyTopic(zkClient, KafkaUtilitiesTest.anyTopic, KafkaUtilitiesTest.anyPartitions, replication, KafkaUtilitiesTest.oneYearRetention))
    assertTrue(zkClient.getLeaderForPartition(new TopicPartition(KafkaUtilitiesTest.anyTopic, 0)).isDefined)
  }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import java.util.Properties

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Meter, MetricPredicate}
import org.junit.Test
import org.junit.Assert._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.serializer._
import kafka.utils._
import kafka.admin.AdminUtils
import kafka.utils.TestUtils._

import scala.collection._
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import kafka.consumer.{ConsumerConfig, ZookeeperConsumerConnector}
import kafka.log.LogConfig
import org.apache.kafka.common.TopicPartition

class MetricsTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2

  val overridingProps = new Properties
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  def generateConfigs =
    TestUtils.createBrokerConfigs(numNodes, zkConnect, enableDeleteTopic=true).map(KafkaConfig.fromProps(_, overridingProps))

  val nMessages = 2

  @Test
  @deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
  def testMetricsLeak() {
    val topic = "test-metrics-leak"
    // create topic topic1 with 1 partition on broker 0
    createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = servers)
    // force creation not client's specific metrics.
    createAndShutdownStep(topic, "group0", "consumer0", "producer0")

    //this assertion is only used for creating the metrics for DelayedFetchMetrics, it should never fail, but should not be removed
    assertNotNull(DelayedFetchMetrics)

    val countOfStaticMetrics = Metrics.defaultRegistry.allMetrics.keySet.size

    for (i <- 0 to 5) {
      createAndShutdownStep(topic, "group" + i % 3, "consumer" + i % 2, "producer" + i % 2)
      assertEquals(countOfStaticMetrics, Metrics.defaultRegistry.allMetrics.keySet.size)
    }
  }

  @Test
  def testMetricsReporterAfterDeletingTopic() {
    val topic = "test-topic-metric"
    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetricGroups(topic))
  }

  @Test
  def testBrokerTopicMetricsUnregisteredAfterDeletingTopic() {
    val topic = "test-broker-topic-metric"
    AdminUtils.createTopic(zkUtils, topic, 2, 1)
    // Produce a few messages to create the metrics
    // Don't consume messages as it may cause metrics to be re-created causing the test to fail, see KAFKA-5238
    TestUtils.produceMessages(servers, topic, nMessages)
    assertTrue("Topic metrics don't exist", topicMetricGroups(topic).nonEmpty)
    servers.foreach(s => assertNotNull(s.brokerTopicStats.topicStats(topic)))
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetricGroups(topic))
  }

  @Test
  def testClusterIdMetric(): Unit = {
    // Check if clusterId metric exists.
    val metrics = Metrics.defaultRegistry.allMetrics
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=KafkaServer,name=ClusterId"), 1)
  }

  @deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
  def createAndShutdownStep(topic: String, group: String, consumerId: String, producerId: String): Unit = {
    sendMessages(servers, topic, nMessages)
    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumerId))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder, new StringDecoder)
    getMessages(topicMessageStreams1, nMessages)

    zkConsumerConnector1.shutdown()
  }

  @Test
  def testBrokerTopicMetricsBytesInOut(): Unit = {
    val topic = "test-bytes-in-out"
    val replicationBytesIn = BrokerTopicStats.ReplicationBytesInPerSec
    val replicationBytesOut = BrokerTopicStats.ReplicationBytesOutPerSec
    val bytesIn = s"${BrokerTopicStats.BytesInPerSec},topic=$topic"
    val bytesOut = s"${BrokerTopicStats.BytesOutPerSec},topic=$topic"

    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, "2")
    createTopic(zkUtils, topic, 1, numNodes, servers, topicConfig)
    // Produce a few messages to create the metrics
    TestUtils.produceMessages(servers, topic, nMessages)

    // Check the log size for each broker so that we can distinguish between failures caused by replication issues
    // versus failures caused by the metrics
    val topicPartition = new TopicPartition(topic, 0)
    servers.foreach { server =>
      val log = server.logManager.logsByTopicPartition.get(new TopicPartition(topic, 0))
      val brokerId = server.config.brokerId
      val logSize = log.map(_.size)
      assertTrue(s"Expected broker $brokerId to have a Log for $topicPartition with positive size, actual: $logSize",
        logSize.map(_ > 0).getOrElse(false))
    }

    val initialReplicationBytesIn = meterCount(replicationBytesIn)
    val initialReplicationBytesOut = meterCount(replicationBytesOut)
    val initialBytesIn = meterCount(bytesIn)
    val initialBytesOut = meterCount(bytesOut)

    // Produce a few messages to make the metrics tick
    TestUtils.produceMessages(servers, topic, nMessages)

    assertTrue(meterCount(replicationBytesIn) > initialReplicationBytesIn)
    assertTrue(meterCount(replicationBytesOut) > initialReplicationBytesOut)
    assertTrue(meterCount(bytesIn) > initialBytesIn)
    // BytesOut doesn't include replication, so it shouldn't have changed
    assertEquals(initialBytesOut, meterCount(bytesOut))

    // Consume messages to make bytesOut tick
    TestUtils.consumeTopicRecords(servers, topic, nMessages * 2)

    assertTrue(meterCount(bytesOut) > initialBytesOut)
  }

  @Test
  def testControllerMetrics(): Unit = {
    val metrics = Metrics.defaultRegistry.allMetrics
    
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ActiveControllerCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=GlobalTopicCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=GlobalPartitionCount"), 1)
  }

  private def meterCount(metricName: String): Long = {
    Metrics.defaultRegistry.allMetrics.asScala
      .filterKeys(_.getMBeanName.endsWith(metricName))
      .values
      .headOption
      .getOrElse(fail(s"Unable to find metric $metricName"))
      .asInstanceOf[Meter]
      .count
  }

  private def topicMetricGroups(topic: String): Set[String] = {
    val topicMetricRegex = new Regex(".*BrokerTopicMetrics.*("+topic+")$")
    val metricGroups = Metrics.defaultRegistry.groupedMetrics(MetricPredicate.ALL).keySet.asScala
    metricGroups.filter(topicMetricRegex.pattern.matcher(_).matches)
  }
}

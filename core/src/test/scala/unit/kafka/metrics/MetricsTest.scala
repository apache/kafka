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
import javax.management.ObjectName

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Meter, MetricPredicate}
import org.junit.Test
import org.junit.Assert._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils._

import scala.collection._
import scala.collection.JavaConverters._
import scala.util.matching.Regex
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
  def testMetricsReporterAfterDeletingTopic() {
    val topic = "test-topic-metric"
    adminZkClient.createTopic(topic, 1, 1)
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetricGroups(topic))
  }

  @Test
  def testBrokerTopicMetricsUnregisteredAfterDeletingTopic() {
    val topic = "test-broker-topic-metric"
    adminZkClient.createTopic(topic, 2, 1)
    // Produce a few messages to create the metrics
    // Don't consume messages as it may cause metrics to be re-created causing the test to fail, see KAFKA-5238
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)
    assertTrue("Topic metrics don't exist", topicMetricGroups(topic).nonEmpty)
    servers.foreach(s => assertNotNull(s.brokerTopicStats.topicStats(topic)))
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetricGroups(topic))
  }

  @Test
  def testClusterIdMetric(): Unit = {
    // Check if clusterId metric exists.
    val metrics = Metrics.defaultRegistry.allMetrics
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=KafkaServer,name=ClusterId"), 1)
  }

  @Test
  def testWindowsStyleTagNames(): Unit = {
    val path = "C:\\windows-path\\kafka-logs"
    val tags = Map("dir" -> path)
    val expectedMBeanName = Set(tags.keySet.head, ObjectName.quote(path)).mkString("=")
    val metric = KafkaMetricsGroup.metricName("test-metric", tags)
    assert(metric.getMBeanName.endsWith(expectedMBeanName))
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
    createTopic(topic, 1, numNodes, topicConfig)
    // Produce a few messages to create the metrics
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)

    // Check the log size for each broker so that we can distinguish between failures caused by replication issues
    // versus failures caused by the metrics
    val topicPartition = new TopicPartition(topic, 0)
    servers.foreach { server =>
      val log = server.logManager.getLog(new TopicPartition(topic, 0))
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
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)

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

  /**
   * Test that the metrics are created with the right name, testZooKeeperStateChangeRateMetrics
   * and testZooKeeperSessionStateMetric in ZooKeeperClientTest test the metrics behaviour.
   */
  @Test
  def testSessionExpireListenerMetrics(): Unit = {
    val metrics = Metrics.defaultRegistry.allMetrics

    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=SessionState"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec"), 1)
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

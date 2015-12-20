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

package kafka.consumer

import java.util.Properties

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.MetricPredicate
import org.junit.{After, Test}
import org.junit.Assert._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.serializer._
import kafka.utils._
import kafka.admin.AdminUtils
import kafka.utils.TestUtils._
import scala.collection._
import scala.collection.JavaConversions._
import scala.util.matching.Regex

class MetricsTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  def generateConfigs() =
    TestUtils.createBrokerConfigs(numNodes, zkConnect, enableDeleteTopic=true).map(KafkaConfig.fromProps(_, overridingProps))

  val nMessages = 2

  @After
  override def tearDown() {
    super.tearDown()
  }

  @Test
  def testMetricsLeak() {
    // create topic topic1 with 1 partition on broker 0
    createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = servers)
    // force creation not client's specific metrics.
    createAndShutdownStep("group0", "consumer0", "producer0")

    //this assertion is only used for creating the metrics for DelayedFetchMetrics, it should never fail, but should not be removed
    assertNotNull(DelayedFetchMetrics)

    val countOfStaticMetrics = Metrics.defaultRegistry().allMetrics().keySet().size

    for (i <- 0 to 5) {
      createAndShutdownStep("group" + i % 3, "consumer" + i % 2, "producer" + i % 2)
      assertEquals(countOfStaticMetrics, Metrics.defaultRegistry().allMetrics().keySet().size)
    }
  }

  @Test
  def testMetricsReporterAfterDeletingTopic() {
    val topic = "test-topic-metric"
    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    assertFalse("Topic metrics exists after deleteTopic", checkTopicMetricsExists(topic))
  }

  def createAndShutdownStep(group: String, consumerId: String, producerId: String): Unit = {
    val sentMessages1 = sendMessages(servers, topic, nMessages)
    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumerId))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    val receivedMessages1 = getMessages(topicMessageStreams1, nMessages)

    zkConsumerConnector1.shutdown()
  }

  private def checkTopicMetricsExists(topic: String): Boolean = {
    val topicMetricRegex = new Regex(".*("+topic+")$")
    val metricGroups = Metrics.defaultRegistry().groupedMetrics(MetricPredicate.ALL).entrySet()
    for(metricGroup <- metricGroups) {
      if (topicMetricRegex.pattern.matcher(metricGroup.getKey()).matches)
        return true
    }
    false
  }
}

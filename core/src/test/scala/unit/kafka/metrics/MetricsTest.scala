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

import com.yammer.metrics.Metrics
import junit.framework.Assert._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import scala.collection._
import org.scalatest.junit.JUnit3Suite
import kafka.message._
import kafka.serializer._
import kafka.utils._
import kafka.utils.TestUtils._

class MetricsTest extends JUnit3Suite with KafkaServerTestHarness with Logging {
  val zookeeperConnect = TestZKUtils.zookeeperConnect
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"
  val configs =
    for (props <- TestUtils.createBrokerConfigs(numNodes))
    yield new KafkaConfig(props) {
      override val zkConnect = zookeeperConnect
      override val numPartitions = numParts
    }
  val nMessages = 2

  override def tearDown() {
    super.tearDown()
  }

  def testMetricsLeak() {
    // create topic topic1 with 1 partition on broker 0
    createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 1, servers = servers)
    // force creation not client's specific metrics.
    createAndShutdownStep("group0", "consumer0", "producer0")

    val countOfStaticMetrics = Metrics.defaultRegistry().allMetrics().keySet().size

    for (i <- 0 to 5) {
      createAndShutdownStep("group" + i % 3, "consumer" + i % 2, "producer" + i % 2)
      assertEquals(countOfStaticMetrics, Metrics.defaultRegistry().allMetrics().keySet().size)
    }
  }

  def createAndShutdownStep(group: String, consumerId: String, producerId: String): Unit = {
    val sentMessages1 = sendMessages(configs, topic, producerId, nMessages, "batch1", NoCompressionCodec, 1)
    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumerId))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    val receivedMessages1 = getMessages(nMessages, topicMessageStreams1)

    zkConsumerConnector1.shutdown()
  }
}
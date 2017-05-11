/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.integration

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.{After, Before, Test}
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Meter
import scala.collection.JavaConverters._

class BrokerTopicMetricsTest extends KafkaServerTestHarness {

  private val serverCount = 2
  override def generateConfigs() = TestUtils.createBrokerConfigs(serverCount, zkConnect).map(KafkaConfig.fromProps)

  private val topic = "testtopic"
  private val replicationBytesIn = "ReplicationBytesInPerSec"
  private val replicationBytesOut = "ReplicationBytesOutPerSec"
  private val bytesIn = "BytesInPerSec,topic=" + topic
  private val bytesOut = "BytesOutPerSec,topic=" + topic
  private val testedMetrics = List(replicationBytesIn, replicationBytesOut, bytesIn, bytesOut)

  private val numMessages = 5

  @Before
  override def setUp() {
    // Do some Metrics Registry cleanup by removing the metrics that this test checks.
    // This is a test workaround to the issue that prior harness runs may have left a populated registry.
    // see https://issues.apache.org/jira/browse/KAFKA-4605
    for (m <- testedMetrics) {
      Metrics.defaultRegistry.allMetrics.asScala
        .filterKeys(k => k.getMBeanName.endsWith(m))
        .headOption match {
          case Some(e) => Metrics.defaultRegistry.removeMetric(e._1)
          case None =>
        }
    }
    super.setUp

    val leaders = TestUtils.createTopic(zkUtils, topic, 1, serverCount, servers)
    assertTrue("Leader of all partitions of the topic should exist", leaders.values.forall(leader => leader.isDefined))
  }

  @Test
  def testReplicationMetricsAreUpdated() {
    // Topic metric can take a moment to be created, so wait for them
    TestUtils.waitUntilTrue(
        () => Metrics.defaultRegistry.allMetrics.asScala.filterKeys(k => k.getMBeanName.endsWith(topic)).size > 1,
        "Timed out while waiting for the BrokerTopicMetrics to be created",
        500L)

    assertEquals(0L, getMeterCount(replicationBytesIn))
    assertEquals(0L, getMeterCount(replicationBytesOut))
    assertEquals(0L, getMeterCount(bytesIn))
    assertEquals(0L, getMeterCount(bytesOut))

    // Produce a few messages to make the BrokerTopicMetrics tick
    TestUtils.produceMessages(servers, topic, numMessages)

    assertTrue(getMeterCount(replicationBytesIn) > 0L)
    assertTrue(getMeterCount(replicationBytesOut) > 0L)
    assertTrue(getMeterCount(bytesIn) > 0L)
    // BytesOut doesn't include replication, so 0 until we consume
    assertEquals(0L, getMeterCount(bytesOut))

    // Consume messages to make bytesOut tick
    TestUtils.consumeMessages(servers, topic, numMessages)

    assertTrue(getMeterCount(bytesOut) > 0L)
  }

  private def getMeterCount(metricName: String): Long = {
    Metrics.defaultRegistry.allMetrics.asScala
      .filterKeys(k => k.getMBeanName.endsWith(metricName))
      .headOption
      .getOrElse { fail("Unable to find metric " + metricName) }
      ._2.asInstanceOf[Meter]
      .count
  }
}
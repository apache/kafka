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
package kafka.api

import java.util.Properties

import kafka.admin.{RackAwareMode, AdminUtils, RackAwareTest}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{ZkUtils, TestUtils}
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import scala.collection.Map

class RackAwareAutoTopicCreationTest extends KafkaServerTestHarness with RackAwareTest {
  val numServers = 4

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, 8.toString)
  overridingProps.put(KafkaConfig.DefaultReplicationFactorProp, 2.toString)

  def generateConfigs() =
    (0 until numServers) map { node =>
      TestUtils.createBrokerConfig(node, zkConnect, false, rack = (node / 2).toString)
    } map (KafkaConfig.fromProps(_, overridingProps))

  private val topic = "topic"

  @Test
  def testAutoCreateTopic() {
    var producer = TestUtils.createNewProducer(brokerList, retries = 5)

    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, "key".getBytes, "value".getBytes)
      assertEquals("Should have offset 0", 0L, producer.send(record).get.offset)

      // double check that the topic is created with leader elected
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
      val assignment = zkUtils.getReplicaAssignmentForTopics(Seq(topic))
                              .map(p => p._1.partition -> p._2)
      val brokerRackMap = AdminUtils.getBrokersAndRackInfo(zkUtils, RackAwareMode.Enforced)._2
      val expectedMap = Map(0 -> "0", 1 -> "0", 2 -> "1", 3 -> "1");
      assertEquals(expectedMap, brokerRackMap)
      ensureRackAwareAndEvenDistribution(assignment, brokerRackMap, 4, 8, 2)
    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }
}


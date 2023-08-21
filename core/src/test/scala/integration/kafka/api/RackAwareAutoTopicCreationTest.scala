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

import java.util.{Collections, Properties}
import kafka.admin.RackAwareTest
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.Map
import scala.jdk.CollectionConverters._

class RackAwareAutoTopicCreationTest extends KafkaServerTestHarness with RackAwareTest {
  val numServers = 4
  val numPartitions = 8
  val replicationFactor = 2
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numPartitions.toString)
  overridingProps.put(KafkaConfig.DefaultReplicationFactorProp, replicationFactor.toString)


  def generateConfigs =
    (0 until numServers) map { node =>
      TestUtils.createBrokerConfig(node, zkConnectOrNull, enableControlledShutdown = false, rack = Some((node / 2).toString))
    } map (KafkaConfig.fromProps(_, overridingProps))

  private val topic = "topic"

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk")) // TODO Partition leader is not evenly distributed in kraft mode, see KAFKA-15354
  def testAutoCreateTopic(quorum: String): Unit = {
    val producer = TestUtils.createProducer(bootstrapServers())
    val admin =  TestUtils.createAdminClient(brokers, listenerName, new Properties)
    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
      assertEquals(0L, producer.send(record).get.offset, "Should have offset 0")

      // double check that the topic is created with leader elected
      TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(admin, topic, 0)
      val assignment = admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get().asScala.flatMap(_._2.partitions().asScala).map { info =>
        info.partition -> info.replicas().asScala.toSeq.map(_.id())
      }.toMap
      val brokerMetadatas = admin.describeCluster().nodes().get().asScala.toSeq.sortBy(_.id)
      val expectedMap = Map(0 -> "0", 1 -> "0", 2 -> "1", 3 -> "1")
      assertEquals(expectedMap, brokerMetadatas.map(b => b.id -> b.rack).toMap)
      checkReplicaDistribution(assignment, expectedMap, numServers, numPartitions, replicationFactor)
    } finally {
      producer.close()
      admin.close()
    }
  }
}


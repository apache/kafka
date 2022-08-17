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
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, TopicDescription}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.ExecutionException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters.ListHasAsScala

class RackAwareAutoTopicCreationTest extends KafkaServerTestHarness with RackAwareTest {
  val numServers = 4
  val numPartitions = 8
  val replicationFactor = 2
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numPartitions.toString)
  overridingProps.put(KafkaConfig.DefaultReplicationFactorProp, replicationFactor.toString)

  def generateConfigs =
    (0 until numServers) map { node =>
      TestUtils.createBrokerConfig(node, zkConnectOrNull, enableControlledShutdown = false, rack = Some((node % 2).toString))
    } map (KafkaConfig.fromProps(_, overridingProps))

  private val topic = "topic"

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAutoCreateTopic(quorum: String): Unit = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())

    TestUtils.waitUntilTrue(
      () => brokers.head.metadataCache.getAliveBrokers().size == numServers,
      "Timed out waiting for all brokers to become active")

    // Send a message to auto-create the topic
    val record = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
    assertEquals(
      0L,
      TestUtils.resource(TestUtils.createProducer(bootstrapServers())) { producer =>
        producer.send(record).get.offset
      },
      "Should have offset 0")

    var topicDescription : TopicDescription = null
    try {
      topicDescription = TestUtils.resource(Admin.create(props)) { adminClient =>
        adminClient.describeTopics(Collections.singleton(topic)).topicNameValues().get(topic).get()
      }
    } catch {
      case e: ExecutionException => fail(e.getCause)
    }
    assertTrue(topicDescription != null, "Topic 'topic' should exist")

    val partition = topicDescription.partitions().stream().filter(_.partition == 0).findAny()
    assertTrue(partition.isPresent, "Partition [topic,0] should exist")
    assertFalse(partition.get().leader().isEmpty, "Leader should exist for partition [topic,0]")

    val assignment = topicDescription.partitions.asScala.map { info =>
      (info.partition, info.replicas.asScala.map(_.id))
    }.toMap

    val brokerMetadatas = brokers.head.metadataCache.getAliveBrokers().toList
    val expectedRacks = Map(0 -> "0", 1 -> "1", 2 -> "0", 3 -> "1")
    assertEquals(expectedRacks, brokerMetadatas.map(b => b.id -> b.rack.get).toMap)
    checkReplicaDistribution(assignment, expectedRacks, numServers, numPartitions, replicationFactor,
      verifyLeaderDistribution = false)
  }
}

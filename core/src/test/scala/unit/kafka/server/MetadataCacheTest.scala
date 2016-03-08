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
package kafka.server

import java.util
import util.Arrays.asList

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.UpdateMetadataRequest
import org.apache.kafka.common.requests.UpdateMetadataRequest.{PartitionState, Broker, EndPoint}
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._

class MetadataCacheTest {

  private def asSet[T](elems: T*): util.Set[T] = new util.HashSet(elems.asJava)

  @Test
  def getTopicMetadataNonExistingTopics() {
    val topic = "topic"
    val cache = new MetadataCache(1)
    val topicMetadata = cache.getTopicMetadata(Set(topic), SecurityProtocol.PLAINTEXT)
    assertTrue(topicMetadata.isEmpty)
  }

  @Test
  def getTopicMetadata() {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val brokers = Set(
      new Broker(0, Map(SecurityProtocol.PLAINTEXT -> new EndPoint("foo", 9092)).asJava),
      new Broker(1, Map(SecurityProtocol.PLAINTEXT -> new EndPoint("foo", 9092)).asJava),
      new Broker(2, Map(SecurityProtocol.PLAINTEXT -> new EndPoint("foo", 9092)).asJava))

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, 0, 0, asList(0), zkVersion, asSet(0)),
      new TopicPartition(topic, 1) -> new PartitionState(controllerEpoch, 1, 1, asList(1), zkVersion, asSet(1)),
      new TopicPartition(topic, 2) -> new PartitionState(controllerEpoch, 2, 2, asList(2), zkVersion, asSet(2)))

    val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava)
    cache.updateCache(15, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), SecurityProtocol.PLAINTEXT)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE, topicMetadata.error)
    assertEquals(topic, topicMetadata.topic)

    val partitionMetadatas = topicMetadata.partitionMetadata.asScala.sortBy(_.partition)
    assertEquals(3, partitionMetadatas.size)

    for (i <- 0 to 2) {
      val partitionMetadata = partitionMetadatas(i)
      assertEquals(Errors.NONE, partitionMetadata.error)
      assertEquals(i, partitionMetadata.partition)
      assertEquals(i, partitionMetadata.leader.id)
      assertEquals(List(i), partitionMetadata.isr.asScala.map(_.id))
      assertEquals(List(i), partitionMetadata.replicas.asScala.map(_.id))
    }
  }

  @Test
  def getTopicMetadataPartitionLeaderNotAvailable() {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val brokers = Set(new Broker(0, Map(SecurityProtocol.PLAINTEXT -> new EndPoint("foo", 9092)).asJava))

    val leader = 1
    val leaderEpoch = 1
    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, asList(0), zkVersion, asSet(0)))

    val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava)
    cache.updateCache(15, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), SecurityProtocol.PLAINTEXT)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE, topicMetadata.error)

    val partitionMetadatas = topicMetadata.partitionMetadata
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partition)
    assertEquals(Errors.LEADER_NOT_AVAILABLE, partitionMetadata.error)
    assertTrue(partitionMetadata.isr.isEmpty)
    assertEquals(1, partitionMetadata.replicas.size)
    assertEquals(0, partitionMetadata.replicas.get(0).id)
  }

  @Test
  def getTopicMetadataReplicaNotAvailable() {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val brokers = Set(new Broker(0, Map(SecurityProtocol.PLAINTEXT -> new EndPoint("foo", 9092)).asJava))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asSet[Integer](0, 1)
    val isr = asList[Integer](0)

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas))

    val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava)
    cache.updateCache(15, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), SecurityProtocol.PLAINTEXT)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE, topicMetadata.error)

    val partitionMetadatas = topicMetadata.partitionMetadata
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partition)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE, partitionMetadata.error)
    assertEquals(Set(0), partitionMetadata.replicas.asScala.map(_.id).toSet)
    assertEquals(Set(0), partitionMetadata.isr.asScala.map(_.id).toSet)
  }

  @Test
  def getTopicMetadataIsrNotAvailable() {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val brokers = Set(new Broker(0, Map(SecurityProtocol.PLAINTEXT -> new EndPoint("foo", 9092)).asJava))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asSet[Integer](0)
    val isr = asList[Integer](0, 1)

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas))

    val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava)
    cache.updateCache(15, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), SecurityProtocol.PLAINTEXT)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE, topicMetadata.error)

    val partitionMetadatas = topicMetadata.partitionMetadata
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partition)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE, partitionMetadata.error)
    assertEquals(Set(0), partitionMetadata.replicas.asScala.map(_.id).toSet)
    assertEquals(Set(0), partitionMetadata.isr.asScala.map(_.id).toSet)
  }

}

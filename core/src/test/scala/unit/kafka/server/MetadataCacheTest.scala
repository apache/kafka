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

import kafka.common.BrokerEndPointNotAvailableException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{PartitionState, UpdateMetadataRequest}
import org.apache.kafka.common.requests.UpdateMetadataRequest.{Broker, EndPoint}
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._

class MetadataCacheTest {

  private def asSet[T](elems: T*): util.Set[T] = new util.HashSet(elems.asJava)

  @Test
  def getTopicMetadataNonExistingTopics() {
    val topic = "topic"
    val cache = new MetadataCache(1)
    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assertTrue(topicMetadata.isEmpty)
  }

  @Test
  def getTopicMetadata() {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1

    def endPoints(brokerId: Int): Seq[EndPoint] = {
      val host = s"foo-$brokerId"
      Seq(
        new EndPoint(host, 9092, SecurityProtocol.PLAINTEXT, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)),
        new EndPoint(host, 9093, SecurityProtocol.SSL, ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
      )
    }

    val brokers = (0 to 2).map { brokerId =>
      new Broker(brokerId, endPoints(brokerId).asJava, "rack1")
    }.toSet

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, 0, 0, asList(0), zkVersion, asSet(0)),
      new TopicPartition(topic, 1) -> new PartitionState(controllerEpoch, 1, 1, asList(1), zkVersion, asSet(1)),
      new TopicPartition(topic, 2) -> new PartitionState(controllerEpoch, 2, 2, asList(2), zkVersion, asSet(2)))

    val updateMetadataRequest = new UpdateMetadataRequest.Builder(
      controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava).build()
    cache.updateCache(15, updateMetadataRequest)

    for (securityProtocol <- Seq(SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL)) {
      val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
      val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName)
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
        val leader = partitionMetadata.leader
        assertEquals(i, leader.id)
        val endPoint = endPoints(partitionMetadata.leader.id).find(_.listenerName == listenerName).get
        assertEquals(endPoint.host, leader.host)
        assertEquals(endPoint.port, leader.port)
        assertEquals(List(i), partitionMetadata.isr.asScala.map(_.id))
        assertEquals(List(i), partitionMetadata.replicas.asScala.map(_.id))
      }

    }

  }

  @Test
  def getTopicMetadataPartitionLeaderNotAvailable() {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Set(new Broker(0, Seq(new EndPoint("foo", 9092, securityProtocol, listenerName)).asJava, null))

    val leader = 1
    val leaderEpoch = 1
    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, asList(0), zkVersion, asSet(0)))

    val updateMetadataRequest = new UpdateMetadataRequest.Builder(
      controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava).build()
    cache.updateCache(15, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName)
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
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Set(new Broker(0, Seq(new EndPoint("foo", 9092, securityProtocol, listenerName)).asJava, null))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asSet[Integer](0, 1)
    val isr = asList[Integer](0)

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas))

    val updateMetadataRequest = new UpdateMetadataRequest.Builder(
      controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava).build()
    cache.updateCache(15, updateMetadataRequest)

    // Validate errorUnavailableEndpoints = false
    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = false)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE, topicMetadata.error)

    val partitionMetadatas = topicMetadata.partitionMetadata
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partition)
    assertEquals(Errors.NONE, partitionMetadata.error)
    assertEquals(Set(0, 1), partitionMetadata.replicas.asScala.map(_.id).toSet)
    assertEquals(Set(0), partitionMetadata.isr.asScala.map(_.id).toSet)

    // Validate errorUnavailableEndpoints = true
    val topicMetadatasWithError = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = true)
    assertEquals(1, topicMetadatasWithError.size)

    val topicMetadataWithError = topicMetadatasWithError.head
    assertEquals(Errors.NONE, topicMetadataWithError.error)

    val partitionMetadatasWithError = topicMetadataWithError.partitionMetadata
    assertEquals(1, partitionMetadatasWithError.size)

    val partitionMetadataWithError = partitionMetadatasWithError.get(0)
    assertEquals(0, partitionMetadataWithError.partition)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE, partitionMetadataWithError.error)
    assertEquals(Set(0), partitionMetadataWithError.replicas.asScala.map(_.id).toSet)
    assertEquals(Set(0), partitionMetadataWithError.isr.asScala.map(_.id).toSet)
  }

  @Test
  def getTopicMetadataIsrNotAvailable() {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Set(new Broker(0, Seq(new EndPoint("foo", 9092, securityProtocol, listenerName)).asJava, "rack1"))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asSet[Integer](0)
    val isr = asList[Integer](0, 1)

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas))

    val updateMetadataRequest = new UpdateMetadataRequest.Builder(
      controllerId, controllerEpoch, partitionStates.asJava, brokers.asJava).build()
    cache.updateCache(15, updateMetadataRequest)

    // Validate errorUnavailableEndpoints = false
    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = false)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE, topicMetadata.error)

    val partitionMetadatas = topicMetadata.partitionMetadata
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partition)
    assertEquals(Errors.NONE, partitionMetadata.error)
    assertEquals(Set(0), partitionMetadata.replicas.asScala.map(_.id).toSet)
    assertEquals(Set(0, 1), partitionMetadata.isr.asScala.map(_.id).toSet)

    // Validate errorUnavailableEndpoints = true
    val topicMetadatasWithError = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = true)
    assertEquals(1, topicMetadatasWithError.size)

    val topicMetadataWithError = topicMetadatasWithError.head
    assertEquals(Errors.NONE, topicMetadataWithError.error)

    val partitionMetadatasWithError = topicMetadataWithError.partitionMetadata
    assertEquals(1, partitionMetadatasWithError.size)

    val partitionMetadataWithError = partitionMetadatasWithError.get(0)
    assertEquals(0, partitionMetadataWithError.partition)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE, partitionMetadataWithError.error)
    assertEquals(Set(0), partitionMetadataWithError.replicas.asScala.map(_.id).toSet)
    assertEquals(Set(0), partitionMetadataWithError.isr.asScala.map(_.id).toSet)
  }

  @Test
  def getTopicMetadataWithNonSupportedSecurityProtocol() {
    val topic = "topic"
    val cache = new MetadataCache(1)
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val brokers = Set(new Broker(0,
      Seq(new EndPoint("foo", 9092, securityProtocol, ListenerName.forSecurityProtocol(securityProtocol))).asJava, ""))
    val controllerEpoch = 1
    val leader = 0
    val leaderEpoch = 0
    val replicas = asSet[Integer](0)
    val isr = asList[Integer](0, 1)
    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, isr, 3, replicas))
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(
      2, controllerEpoch, partitionStates.asJava, brokers.asJava).build()
    cache.updateCache(15, updateMetadataRequest)

    try {
      val result = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
      fail(s"Exception should be thrown by `getTopicMetadata` with non-supported SecurityProtocol, $result was returned instead")
    }
    catch {
      case _: BrokerEndPointNotAvailableException => //expected
    }

  }

  @Test
  def getAliveBrokersShouldNotBeMutatedByUpdateCache() {
    val topic = "topic"
    val cache = new MetadataCache(1)

    def updateCache(brokerIds: Set[Int]) {
      val brokers = brokerIds.map { brokerId =>
        val securityProtocol = SecurityProtocol.PLAINTEXT
        new Broker(brokerId, Seq(
          new EndPoint("foo", 9092, securityProtocol, ListenerName.forSecurityProtocol(securityProtocol))).asJava, "")
      }
      val controllerEpoch = 1
      val leader = 0
      val leaderEpoch = 0
      val replicas = asSet[Integer](0)
      val isr = asList[Integer](0, 1)
      val partitionStates = Map(
        new TopicPartition(topic, 0) -> new PartitionState(controllerEpoch, leader, leaderEpoch, isr, 3, replicas))
      val updateMetadataRequest = new UpdateMetadataRequest.Builder(
        2, controllerEpoch, partitionStates.asJava, brokers.asJava).build()
      cache.updateCache(15, updateMetadataRequest)
    }

    val initialBrokerIds = (0 to 2).toSet
    updateCache(initialBrokerIds)
    val aliveBrokersFromCache = cache.getAliveBrokers
    // This should not change `aliveBrokersFromCache`
    updateCache((0 to 3).toSet)
    assertEquals(initialBrokerIds, aliveBrokersFromCache.map(_.id).toSet)
  }

}

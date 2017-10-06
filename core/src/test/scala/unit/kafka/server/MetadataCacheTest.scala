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
import kafka.log.LogConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.UpdateMetadataRequest
import org.apache.kafka.common.requests.UpdateMetadataRequest.{Broker, EndPoint}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._
import scala.collection.mutable

class MetadataCacheTest {

  @Test
  def getTopicMetadataNonExistingTopics() {
    val topic = "topic"
    val cache = new MetadataCache(1, Map.empty[String, LogConfig])
    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assertTrue(topicMetadata.isEmpty)
  }

  @Test
  def getTopicMetadata() {
    val topic0 = "topic-0"
    val topic1 = "topic-1"

    val cache = new MetadataCache(1, createLogConfigs(Seq(topic0, topic1), "0.11.0", 110))

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

    val brokers = (0 to 4).map { brokerId =>
      new Broker(brokerId, endPoints(brokerId).asJava, "rack1")
    }.toSet

    val partitionStates = Map(
      new TopicPartition(topic0, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, 0, 0, asList(0, 1, 3), zkVersion, asList(0, 1, 3), asList()),
      new TopicPartition(topic0, 1) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, 1, 1, asList(1, 0), zkVersion, asList(1, 2, 0, 4), asList()),
      new TopicPartition(topic1, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, 2, 2, asList(2, 1), zkVersion, asList(2, 1, 3), asList()))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch,
      partitionStates.asJava, brokers.asJava).build()
    cache.updateCache(15, updateMetadataRequest)

    for (securityProtocol <- Seq(SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL)) {
      val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

      def checkTopicMetadata(topic: String): Unit = {
        val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName)
        assertEquals(1, topicMetadatas.size)

        val topicMetadata = topicMetadatas.head
        assertEquals(Errors.NONE, topicMetadata.error)
        assertEquals(topic, topicMetadata.topic)
        assertEquals(2, topicMetadata.messageFormatVersion())
        assertEquals(110, topicMetadata.messageMaxBytes())

        val topicPartitionStates = partitionStates.filter { case (tp, _) => tp.topic ==  topic }
        val partitionMetadatas = topicMetadata.partitionMetadata.asScala.sortBy(_.partition)
        assertEquals(s"Unexpected partition count for topic $topic", topicPartitionStates.size, partitionMetadatas.size)

        partitionMetadatas.zipWithIndex.foreach { case (partitionMetadata, partitionId) =>
          assertEquals(Errors.NONE, partitionMetadata.error)
          assertEquals(partitionId, partitionMetadata.partition)
          val leader = partitionMetadata.leader
          val partitionState = topicPartitionStates(new TopicPartition(topic, partitionId))
          assertEquals(partitionState.basePartitionState.leader, leader.id)
          assertEquals(partitionState.basePartitionState.isr, partitionMetadata.isr.asScala.map(_.id).asJava)
          assertEquals(partitionState.basePartitionState.replicas, partitionMetadata.replicas.asScala.map(_.id).asJava)
          val endPoint = endPoints(partitionMetadata.leader.id).find(_.listenerName == listenerName).get
          assertEquals(endPoint.host, leader.host)
          assertEquals(endPoint.port, leader.port)
        }
      }

      checkTopicMetadata(topic0)
      checkTopicMetadata(topic1)
    }

  }

  @Test
  def getTopicMetadataPartitionLeaderNotAvailable() {
    val topic = "topic"

    val cache = new MetadataCache(1, createLogConfigs(Seq(topic)))

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Set(new Broker(0, Seq(new EndPoint("foo", 9092, securityProtocol, listenerName)).asJava, null))

    val leader = 1
    val leaderEpoch = 1
    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, leader, leaderEpoch, asList(0), zkVersion, asList(0), asList()))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch,
      partitionStates.asJava, brokers.asJava).build()
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

    val cache = new MetadataCache(1, createLogConfigs(Seq(topic)))

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Set(new Broker(0, Seq(new EndPoint("foo", 9092, securityProtocol, listenerName)).asJava, null))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0, 1)
    val isr = asList[Integer](0)

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas, asList()))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch,
      partitionStates.asJava, brokers.asJava).build()
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

    val cache = new MetadataCache(1, createLogConfigs(Seq(topic)))

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Set(new Broker(0, Seq(new EndPoint("foo", 9092, securityProtocol, listenerName)).asJava, "rack1"))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)

    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas, asList()))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch,
      partitionStates.asJava, brokers.asJava).build()
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
    val cache = new MetadataCache(1, createLogConfigs(Seq(topic)))
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val brokers = Set(new Broker(0,
      Seq(new EndPoint("foo", 9092, securityProtocol, ListenerName.forSecurityProtocol(securityProtocol))).asJava, ""))
    val controllerEpoch = 1
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)
    val partitionStates = Map(
      new TopicPartition(topic, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, 3, replicas, asList()))
    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, 2, controllerEpoch, partitionStates.asJava,
      brokers.asJava).build()
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
    val cache = new MetadataCache(1, createLogConfigs(Seq(topic)))

    def updateCache(brokerIds: Set[Int]) {
      val brokers = brokerIds.map { brokerId =>
        val securityProtocol = SecurityProtocol.PLAINTEXT
        new Broker(brokerId, Seq(
          new EndPoint("foo", 9092, securityProtocol, ListenerName.forSecurityProtocol(securityProtocol))).asJava, "")
      }
      val controllerEpoch = 1
      val leader = 0
      val leaderEpoch = 0
      val replicas = asList[Integer](0)
      val isr = asList[Integer](0, 1)
      val partitionStates = Map(
        new TopicPartition(topic, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, 3, replicas, asList()))
      val version = ApiKeys.UPDATE_METADATA.latestVersion
      val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, 2, controllerEpoch, partitionStates.asJava,
        brokers.asJava).build()
      cache.updateCache(15, updateMetadataRequest)
    }

    val initialBrokerIds = (0 to 2).toSet
    updateCache(initialBrokerIds)
    val aliveBrokersFromCache = cache.getAliveBrokers
    // This should not change `aliveBrokersFromCache`
    updateCache((0 to 3).toSet)
    assertEquals(initialBrokerIds, aliveBrokersFromCache.map(_.id).toSet)
  }

  @Test
  def testPropagationOfTopicConfigs(): Unit = {
    val existingTopic : String = "existing"
    val newTopic = "newtopic"
    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1

    def endPoints(brokerId: Int): Seq[EndPoint] = {
      val host = s"foo-$brokerId"
      Seq(
        new EndPoint(host, 9092, SecurityProtocol.PLAINTEXT, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
      )
    }

    val brokers = (0 to 4).map { brokerId =>
      new Broker(brokerId, endPoints(brokerId).asJava, "rack1")
    }.toSet

    val partitionStates = Map(
      new TopicPartition(newTopic, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, 0, 0, asList(0, 1, 3), zkVersion, asList(0, 1, 3), asList()),
      new TopicPartition(existingTopic, 0) -> new UpdateMetadataRequest.PartitionState(controllerEpoch, 0, 0, asList(0, 1, 3), zkVersion, asList(0, 1, 3), asList()))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch,
      partitionStates.asJava, brokers.asJava).build()

    val cache = new MetadataCache(1, createLogConfigs(Seq(existingTopic), "0.10.2"))
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    cache.updateCache(15, updateMetadataRequest)

    val retrievedMetadata = cache.getTopicMetadata(Set(existingTopic), listenerName, true)
    assertEquals(1, retrievedMetadata.size)
    assertEquals(1, retrievedMetadata.head.messageFormatVersion())

    val updatedConfig = createLogConfigs(Seq(existingTopic), "0.11.0")
    cache.updateTopicMetadata(existingTopic, updatedConfig(existingTopic))

    val newConfig = createLogConfigs(Seq(newTopic), "0.9.0")
    cache.updateTopicMetadata(newTopic, newConfig(newTopic))

    val updatedMetadata = cache.getTopicMetadata(Set(existingTopic, newTopic), listenerName)
    assertEquals(2, updatedMetadata.size)
    assertEquals(2, updatedMetadata.head.messageFormatVersion())
    assertEquals(0, updatedMetadata.tail.head.messageFormatVersion())
  }

  private def createLogConfigs(topics: Seq[String], messageFormatVersion: String = "0.11.0", messageMaxBytes: Int = 1000000) : Map[String, LogConfig] = {
    val configs = mutable.Map[String, Object]()
    configs.put(LogConfig.MessageFormatVersionProp, messageFormatVersion)
    configs.put(LogConfig.MaxMessageBytesProp, messageMaxBytes.toString)
    topics.map { case topic =>
      (topic, LogConfig(configs.asJava))
    }.toMap
  }
}

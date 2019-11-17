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
import java.util.Optional
import util.Arrays.asList

import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.UpdateMetadataRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Test
import org.junit.Assert._
import org.scalatest.Assertions

import scala.collection.JavaConverters._

class MetadataCacheTest {
  val brokerEpoch = 0L

  @Test
  def getTopicMetadataNonExistingTopics(): Unit = {
    val topic = "topic"
    val cache = new MetadataCache(1)
    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assertTrue(topicMetadata.isEmpty)
  }

  @Test
  def getTopicMetadata(): Unit = {
    val topic0 = "topic-0"
    val topic1 = "topic-1"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1

    def endpoints(brokerId: Int): Seq[UpdateMetadataEndpoint] = {
      val host = s"foo-$brokerId"
      Seq(
        new UpdateMetadataEndpoint()
          .setHost(host)
          .setPort(9092)
          .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
          .setListener(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value),
        new UpdateMetadataEndpoint()
          .setHost(host)
          .setPort(9093)
          .setSecurityProtocol(SecurityProtocol.SSL.id)
          .setListener(ListenerName.forSecurityProtocol(SecurityProtocol.SSL).value)
      )
    }

    val brokers = (0 to 4).map { brokerId =>
      new UpdateMetadataBroker()
        .setId(brokerId)
        .setEndpoints(endpoints(brokerId).asJava)
        .setRack("rack1")
    }

    val partitionStates = Seq(
      new UpdateMetadataPartitionState()
        .setTopicName(topic0)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(0)
        .setLeaderEpoch(0)
        .setIsr(asList(0, 1, 3))
        .setZkVersion(zkVersion)
        .setReplicas(asList(0, 1, 3)),
      new UpdateMetadataPartitionState()
        .setTopicName(topic0)
        .setPartitionIndex(1)
        .setControllerEpoch(controllerEpoch)
        .setLeader(1)
        .setLeaderEpoch(1)
        .setIsr(asList(1, 0))
        .setZkVersion(zkVersion)
        .setReplicas(asList(1, 2, 0, 4)),
      new UpdateMetadataPartitionState()
        .setTopicName(topic1)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(2)
        .setLeaderEpoch(2)
        .setIsr(asList(2, 1))
        .setZkVersion(zkVersion)
        .setReplicas(asList(2, 1, 3)))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava).build()
    cache.updateMetadata(15, updateMetadataRequest)

    for (securityProtocol <- Seq(SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL)) {
      val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

      def checkTopicMetadata(topic: String): Unit = {
        val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName)
        assertEquals(1, topicMetadatas.size)

        val topicMetadata = topicMetadatas.head
        assertEquals(Errors.NONE, topicMetadata.error)
        assertEquals(topic, topicMetadata.topic)

        val topicPartitionStates = partitionStates.filter { ps => ps.topicName == topic }
        val partitionMetadatas = topicMetadata.partitionMetadata.asScala.sortBy(_.partition)
        assertEquals(s"Unexpected partition count for topic $topic", topicPartitionStates.size, partitionMetadatas.size)

        partitionMetadatas.zipWithIndex.foreach { case (partitionMetadata, partitionId) =>
          assertEquals(Errors.NONE, partitionMetadata.error)
          assertEquals(partitionId, partitionMetadata.partition)
          val leader = partitionMetadata.leader
          val partitionState = topicPartitionStates.find(_.partitionIndex == partitionId).getOrElse(
            Assertions.fail(s"Unable to find partition state for partition $partitionId"))
          assertEquals(partitionState.leader, leader.id)
          assertEquals(Optional.of(partitionState.leaderEpoch), partitionMetadata.leaderEpoch)
          assertEquals(partitionState.isr, partitionMetadata.isr.asScala.map(_.id).asJava)
          assertEquals(partitionState.replicas, partitionMetadata.replicas.asScala.map(_.id).asJava)
          val endpoint = endpoints(partitionMetadata.leader.id).find(_.listener == listenerName.value).get
          assertEquals(endpoint.host, leader.host)
          assertEquals(endpoint.port, leader.port)
        }
      }

      checkTopicMetadata(topic0)
      checkTopicMetadata(topic1)
    }

  }

  @Test
  def getTopicMetadataPartitionLeaderNotAvailable(): Unit = {
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(listenerName.value)).asJava))
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = true)
  }

  @Test
  def getTopicMetadataPartitionListenerNotAvailableOnLeader(): Unit = {
    val plaintextListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val sslListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL)
    val broker0Endpoints = Seq(
      new UpdateMetadataEndpoint()
        .setHost("host0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setListener(plaintextListenerName.value),
      new UpdateMetadataEndpoint()
        .setHost("host0")
        .setPort(9093)
        .setSecurityProtocol(SecurityProtocol.SSL.id)
        .setListener(sslListenerName.value))
    val broker1Endpoints = Seq(new UpdateMetadataEndpoint()
      .setHost("host1")
      .setPort(9092)
      .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
      .setListener(plaintextListenerName.value))
    val brokers = Seq(
      new UpdateMetadataBroker()
        .setId(0)
        .setEndpoints(broker0Endpoints.asJava),
      new UpdateMetadataBroker()
        .setId(1)
        .setEndpoints(broker1Endpoints.asJava))
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(brokers, sslListenerName,
      leader = 1, Errors.LISTENER_NOT_FOUND, errorUnavailableListeners = true)
  }

  @Test
  def getTopicMetadataPartitionListenerNotAvailableOnLeaderOldMetadataVersion(): Unit = {
    val plaintextListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val sslListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL)
    val broker0Endpoints = Seq(
      new UpdateMetadataEndpoint()
        .setHost("host0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setListener(plaintextListenerName.value),
      new UpdateMetadataEndpoint()
        .setHost("host0")
        .setPort(9093)
        .setSecurityProtocol(SecurityProtocol.SSL.id)
        .setListener(sslListenerName.value))
    val broker1Endpoints = Seq(new UpdateMetadataEndpoint()
      .setHost("host1")
      .setPort(9092)
      .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
      .setListener(plaintextListenerName.value))
    val brokers = Seq(
      new UpdateMetadataBroker()
        .setId(0)
        .setEndpoints(broker0Endpoints.asJava),
      new UpdateMetadataBroker()
        .setId(1)
        .setEndpoints(broker1Endpoints.asJava))
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(brokers, sslListenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
  }

  private def verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(brokers: Seq[UpdateMetadataBroker],
                                                                       listenerName: ListenerName,
                                                                       leader: Int,
                                                                       expectedError: Errors,
                                                                       errorUnavailableListeners: Boolean): Unit = {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1

    val leaderEpoch = 1
    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(0)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(asList(0))
      .setZkVersion(zkVersion)
      .setReplicas(asList(0)))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava).build()
    cache.updateMetadata(15, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableListeners = errorUnavailableListeners)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE, topicMetadata.error)

    val partitionMetadatas = topicMetadata.partitionMetadata
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partition)
    assertEquals(expectedError, partitionMetadata.error)
    assertFalse(partitionMetadata.isr.isEmpty)
    assertEquals(1, partitionMetadata.replicas.size)
    assertEquals(0, partitionMetadata.replicas.get(0).id)
  }

  @Test
  def getTopicMetadataReplicaNotAvailable(): Unit = {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(listenerName.value)).asJava))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0, 1)
    val isr = asList[Integer](0)

    val partitionStates = Seq(
      new UpdateMetadataPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(leader)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(zkVersion)
        .setReplicas(replicas))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava).build()
    cache.updateMetadata(15, updateMetadataRequest)

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
  def getTopicMetadataIsrNotAvailable(): Unit = {
    val topic = "topic"

    val cache = new MetadataCache(1)

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setRack("rack1")
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(listenerName.value)).asJava))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)

    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(0)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(zkVersion)
      .setReplicas(replicas))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava).build()
    cache.updateMetadata(15, updateMetadataRequest)

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
  def getTopicMetadataWithNonSupportedSecurityProtocol(): Unit = {
    val topic = "topic"
    val cache = new MetadataCache(1)
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setRack("")
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(ListenerName.forSecurityProtocol(securityProtocol).value)).asJava))
    val controllerEpoch = 1
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)
    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(0)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(3)
      .setReplicas(replicas))
    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, 2, controllerEpoch, brokerEpoch, partitionStates.asJava,
      brokers.asJava).build()
    cache.updateMetadata(15, updateMetadataRequest)

    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
    assertEquals(1, topicMetadata.size)
    assertEquals(1, topicMetadata.head.partitionMetadata.size)
    assertEquals(-1, topicMetadata.head.partitionMetadata.get(0).leaderId)
  }

  @Test
  def getAliveBrokersShouldNotBeMutatedByUpdateCache(): Unit = {
    val topic = "topic"
    val cache = new MetadataCache(1)

    def updateCache(brokerIds: Seq[Int]): Unit = {
      val brokers = brokerIds.map { brokerId =>
        val securityProtocol = SecurityProtocol.PLAINTEXT
        new UpdateMetadataBroker()
          .setId(brokerId)
          .setRack("")
          .setEndpoints(Seq(new UpdateMetadataEndpoint()
            .setHost("foo")
            .setPort(9092)
            .setSecurityProtocol(securityProtocol.id)
            .setListener(ListenerName.forSecurityProtocol(securityProtocol).value)).asJava)
      }
      val controllerEpoch = 1
      val leader = 0
      val leaderEpoch = 0
      val replicas = asList[Integer](0)
      val isr = asList[Integer](0, 1)
      val partitionStates = Seq(new UpdateMetadataPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(leader)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(3)
        .setReplicas(replicas))
      val version = ApiKeys.UPDATE_METADATA.latestVersion
      val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, 2, controllerEpoch, brokerEpoch, partitionStates.asJava,
        brokers.asJava).build()
      cache.updateMetadata(15, updateMetadataRequest)
    }

    val initialBrokerIds = (0 to 2)
    updateCache(initialBrokerIds)
    val aliveBrokersFromCache = cache.getAliveBrokers
    // This should not change `aliveBrokersFromCache`
    updateCache((0 to 3))
    assertEquals(initialBrokerIds.toSet, aliveBrokersFromCache.map(_.id).toSet)
  }

}

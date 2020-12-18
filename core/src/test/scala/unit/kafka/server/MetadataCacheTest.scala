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
import java.util.Collections
import util.Arrays.asList

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.UpdateMetadataRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Test
import org.junit.Assert._
import org.scalatest.Assertions

import scala.jdk.CollectionConverters._

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

    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put(topic0, Uuid.randomUuid())
    topicIds.put(topic1, Uuid.randomUuid())

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava, topicIds).build()
    cache.updateMetadata(15, updateMetadataRequest)

    for (securityProtocol <- Seq(SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL)) {
      val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

      def checkTopicMetadata(topic: String): Unit = {
        val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName)
        assertEquals(1, topicMetadatas.size)

        val topicMetadata = topicMetadatas.head
        assertEquals(Errors.NONE.code, topicMetadata.errorCode)
        assertEquals(topic, topicMetadata.name)
        assertEquals(topicIds.get(topic), topicMetadata.topicId())

        val topicPartitionStates = partitionStates.filter { ps => ps.topicName == topic }
        val partitionMetadatas = topicMetadata.partitions.asScala.sortBy(_.partitionIndex)
        assertEquals(s"Unexpected partition count for topic $topic", topicPartitionStates.size, partitionMetadatas.size)

        partitionMetadatas.zipWithIndex.foreach { case (partitionMetadata, partitionId) =>
          assertEquals(Errors.NONE.code, partitionMetadata.errorCode)
          assertEquals(partitionId, partitionMetadata.partitionIndex)
          val partitionState = topicPartitionStates.find(_.partitionIndex == partitionId).getOrElse(
            Assertions.fail(s"Unable to find partition state for partition $partitionId"))
          assertEquals(partitionState.leader, partitionMetadata.leaderId)
          assertEquals(partitionState.leaderEpoch, partitionMetadata.leaderEpoch)
          assertEquals(partitionState.isr, partitionMetadata.isrNodes)
          assertEquals(partitionState.replicas, partitionMetadata.replicaNodes)
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
    val metadataCacheBrokerId = 0
    // leader is not available. expect LEADER_NOT_AVAILABLE for any metadata version.
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(metadataCacheBrokerId, brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(metadataCacheBrokerId, brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = true)
  }

  @Test
  def getTopicMetadataPartitionListenerNotAvailableOnLeader(): Unit = {
    // when listener name is not present in the metadata cache for a broker, getTopicMetadata should
    // return LEADER_NOT_AVAILABLE or LISTENER_NOT_FOUND errors for old and new versions respectively.
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
    val metadataCacheBrokerId = 0
    // leader available in cache but listener name not present. expect LISTENER_NOT_FOUND error for new metadata version
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(metadataCacheBrokerId, brokers, sslListenerName,
      leader = 1, Errors.LISTENER_NOT_FOUND, errorUnavailableListeners = true)
    // leader available in cache but listener name not present. expect LEADER_NOT_AVAILABLE error for old metadata version
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(metadataCacheBrokerId, brokers, sslListenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
  }

  private def verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(metadataCacheBrokerId: Int,
                                                                       brokers: Seq[UpdateMetadataBroker],
                                                                       listenerName: ListenerName,
                                                                       leader: Int,
                                                                       expectedError: Errors,
                                                                       errorUnavailableListeners: Boolean): Unit = {
    val topic = "topic"

    val cache = new MetadataCache(metadataCacheBrokerId)

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
      partitionStates.asJava, brokers.asJava, Collections.emptyMap()).build()
    cache.updateMetadata(15, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableListeners = errorUnavailableListeners)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE.code, topicMetadata.errorCode)

    val partitionMetadatas = topicMetadata.partitions
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partitionIndex)
    assertEquals(expectedError.code, partitionMetadata.errorCode)
    assertFalse(partitionMetadata.isrNodes.isEmpty)
    assertEquals(List(0), partitionMetadata.replicaNodes.asScala)
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
      partitionStates.asJava, brokers.asJava, Collections.emptyMap()).build()
    cache.updateMetadata(15, updateMetadataRequest)

    // Validate errorUnavailableEndpoints = false
    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = false)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE.code(), topicMetadata.errorCode)

    val partitionMetadatas = topicMetadata.partitions
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partitionIndex)
    assertEquals(Errors.NONE.code, partitionMetadata.errorCode)
    assertEquals(Set(0, 1), partitionMetadata.replicaNodes.asScala.toSet)
    assertEquals(Set(0), partitionMetadata.isrNodes.asScala.toSet)

    // Validate errorUnavailableEndpoints = true
    val topicMetadatasWithError = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = true)
    assertEquals(1, topicMetadatasWithError.size)

    val topicMetadataWithError = topicMetadatasWithError.head
    assertEquals(Errors.NONE.code, topicMetadataWithError.errorCode)

    val partitionMetadatasWithError = topicMetadataWithError.partitions()
    assertEquals(1, partitionMetadatasWithError.size)

    val partitionMetadataWithError = partitionMetadatasWithError.get(0)
    assertEquals(0, partitionMetadataWithError.partitionIndex)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code, partitionMetadataWithError.errorCode)
    assertEquals(Set(0), partitionMetadataWithError.replicaNodes.asScala.toSet)
    assertEquals(Set(0), partitionMetadataWithError.isrNodes.asScala.toSet)
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
      partitionStates.asJava, brokers.asJava, Collections.emptyMap()).build()
    cache.updateMetadata(15, updateMetadataRequest)

    // Validate errorUnavailableEndpoints = false
    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = false)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE.code(), topicMetadata.errorCode)

    val partitionMetadatas = topicMetadata.partitions
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partitionIndex)
    assertEquals(Errors.NONE.code, partitionMetadata.errorCode)
    assertEquals(Set(0), partitionMetadata.replicaNodes.asScala.toSet)
    assertEquals(Set(0, 1), partitionMetadata.isrNodes.asScala.toSet)

    // Validate errorUnavailableEndpoints = true
    val topicMetadatasWithError = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = true)
    assertEquals(1, topicMetadatasWithError.size)

    val topicMetadataWithError = topicMetadatasWithError.head
    assertEquals(Errors.NONE.code, topicMetadataWithError.errorCode)

    val partitionMetadatasWithError = topicMetadataWithError.partitions
    assertEquals(1, partitionMetadatasWithError.size)

    val partitionMetadataWithError = partitionMetadatasWithError.get(0)
    assertEquals(0, partitionMetadataWithError.partitionIndex)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code, partitionMetadataWithError.errorCode)
    assertEquals(Set(0), partitionMetadataWithError.replicaNodes.asScala.toSet)
    assertEquals(Set(0), partitionMetadataWithError.isrNodes.asScala.toSet)
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
      brokers.asJava, Collections.emptyMap()).build()
    cache.updateMetadata(15, updateMetadataRequest)

    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
    assertEquals(1, topicMetadata.size)
    assertEquals(1, topicMetadata.head.partitions.size)
    assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, topicMetadata.head.partitions.get(0).leaderId)
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
        brokers.asJava, Collections.emptyMap()).build()
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

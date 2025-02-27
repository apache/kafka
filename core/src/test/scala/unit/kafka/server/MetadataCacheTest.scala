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

import kafka.server.metadata.KRaftMetadataCache
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiMessage, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.{DirectoryId, TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.common.KRaftVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util
import java.util.Arrays.asList
import java.util.Collections
import java.util.stream.Collectors
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

object MetadataCacheTest {
  def cacheProvider(): util.stream.Stream[MetadataCache] =
    util.stream.Stream.of[MetadataCache](
      MetadataCache.kRaftMetadataCache(1, () => KRaftVersion.KRAFT_VERSION_0)
    )

  def updateCache(cache: MetadataCache, records: Seq[ApiMessage]): Unit = {
    cache match {
      case c: KRaftMetadataCache => {
        val image = c.currentImage()
        val partialImage = new MetadataImage(
          new MetadataProvenance(100L, 10, 1000L, true),
          image.features(),
          image.cluster(),
          image.topics(),
          image.configs(),
          image.clientQuotas(),
          image.producerIds(),
          image.acls(),
          image.scram(),
          image.delegationTokens())
        val delta = new MetadataDelta.Builder().setImage(partialImage).build()
        records.foreach(record => delta.replay(record))
        c.setImage(delta.apply(new MetadataProvenance(100L, 10, 1000L, true)))
      }
      case _ => throw new RuntimeException("Unsupported cache type")
    }
  }
}

class MetadataCacheTest {
  val brokerEpoch = 0L

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataNonExistingTopics(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assertTrue(topicMetadata.isEmpty)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadata(cache: MetadataCache): Unit = {
    val topic0 = "topic-0"
    val topic1 = "topic-1"

    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put(topic0, Uuid.randomUuid())
    topicIds.put(topic1, Uuid.randomUuid())

    def endpoints(brokerId: Int): BrokerEndpointCollection = {
      val host = s"foo-$brokerId"
      new BrokerEndpointCollection(Seq(
        new BrokerEndpoint()
          .setHost(host)
          .setPort(9092)
          .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
          .setName(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value),
        new BrokerEndpoint()
          .setHost(host)
          .setPort(9093)
          .setSecurityProtocol(SecurityProtocol.SSL.id)
          .setName(ListenerName.forSecurityProtocol(SecurityProtocol.SSL).value)
      ).iterator.asJava)
    }

    val brokers = (0 to 4).map { brokerId =>
      new RegisterBrokerRecord()
        .setBrokerId(brokerId)
        .setEndPoints(endpoints(brokerId))
        .setRack("rack1")
    }

    val topic0Record = new TopicRecord().setName(topic0).setTopicId(topicIds.get(topic0))
    val topic1Record = new TopicRecord().setName(topic1).setTopicId(topicIds.get(topic1))

    val partitionStates = Seq(
      new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(0)
        .setLeader(0)
        .setLeaderEpoch(0)
        .setIsr(asList(0, 1, 3))
        .setReplicas(asList(0, 1, 3)),
      new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(1)
        .setLeader(1)
        .setLeaderEpoch(1)
        .setIsr(asList(1, 0))
        .setReplicas(asList(1, 2, 0, 4)),
      new PartitionRecord()
        .setTopicId(topicIds.get(topic1))
        .setPartitionId(0)
        .setLeader(2)
        .setLeaderEpoch(2)
        .setIsr(asList(2, 1))
        .setReplicas(asList(2, 1, 3)))
    MetadataCacheTest.updateCache(cache, brokers ++ Seq(topic0Record, topic1Record) ++ partitionStates)

    for (securityProtocol <- Seq(SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL)) {
      val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

      def checkTopicMetadata(topic: String): Unit = {
        val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName)
        assertEquals(1, topicMetadatas.size)

        val topicMetadata = topicMetadatas.head
        assertEquals(Errors.NONE.code, topicMetadata.errorCode)
        assertEquals(topic, topicMetadata.name)
        assertEquals(topicIds.get(topic), topicMetadata.topicId())

        val topicPartitionStates = partitionStates.filter { ps => ps.topicId == topicIds.get(topic) }
        val partitionMetadatas = topicMetadata.partitions.asScala.sortBy(_.partitionIndex)
        assertEquals(topicPartitionStates.size, partitionMetadatas.size, s"Unexpected partition count for topic $topic")

        partitionMetadatas.zipWithIndex.foreach { case (partitionMetadata, partitionId) =>
          assertEquals(Errors.NONE.code, partitionMetadata.errorCode)
          assertEquals(partitionId, partitionMetadata.partitionIndex)
          val partitionState = topicPartitionStates.find(_.partitionId == partitionId).getOrElse(
            fail(s"Unable to find partition state for partition $partitionId"))
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

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataPartitionLeaderNotAvailable(cache: MetadataCache): Unit = {
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setFenced(false)
      .setEndPoints(new BrokerEndpointCollection(Seq(new BrokerEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setName(listenerName.value)
      ).iterator.asJava)))

    // leader is not available. expect LEADER_NOT_AVAILABLE for any metadata version.
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = true)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataPartitionListenerNotAvailableOnLeader(cache: MetadataCache): Unit = {
    // when listener name is not present in the metadata cache for a broker, getTopicMetadata should
    // return LEADER_NOT_AVAILABLE or LISTENER_NOT_FOUND errors for old and new versions respectively.
    val plaintextListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val sslListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL)
    val broker0Endpoints = new BrokerEndpointCollection(Seq(
      new BrokerEndpoint()
        .setHost("host0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListenerName.value),
      new BrokerEndpoint()
        .setHost("host0")
        .setPort(9093)
        .setSecurityProtocol(SecurityProtocol.SSL.id)
        .setName(sslListenerName.value)
    ).iterator.asJava)

    val broker1Endpoints = new BrokerEndpointCollection(Seq(
      new BrokerEndpoint()
        .setHost("host1")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListenerName.value)
    ).iterator.asJava)

    val brokers = Seq(
      new RegisterBrokerRecord()
        .setBrokerId(0)
        .setFenced(false)
        .setEndPoints(broker0Endpoints),
      new RegisterBrokerRecord()
        .setBrokerId(1)
        .setFenced(false)
        .setEndPoints(broker1Endpoints))

    // leader available in cache but listener name not present. expect LISTENER_NOT_FOUND error for new metadata version
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, sslListenerName,
      leader = 1, Errors.LISTENER_NOT_FOUND, errorUnavailableListeners = true)
    // leader available in cache but listener name not present. expect LEADER_NOT_AVAILABLE error for old metadata version
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, sslListenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
  }

  private def verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache: MetadataCache,
                                                                       brokers: Seq[RegisterBrokerRecord],
                                                                       listenerName: ListenerName,
                                                                       leader: Int,
                                                                       expectedError: Errors,
                                                                       errorUnavailableListeners: Boolean): Unit = {
    val topic = "topic"
    val topicId = Uuid.randomUuid()
    val topicRecords = Seq(new TopicRecord().setName(topic).setTopicId(topicId))

    val leaderEpoch = 1
    val partitionEpoch = 3
    val partitionStates = Seq(new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setPartitionEpoch(partitionEpoch)
        .setLeader(leader)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(asList(0))
        .setReplicas(asList(0)))
    MetadataCacheTest.updateCache(cache, brokers ++ topicRecords ++ partitionStates)

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

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataReplicaNotAvailable(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicId = Uuid.randomUuid()

    val partitionEpoch = 3
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val endPoints = new BrokerEndpointCollection(Seq(new BrokerEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setName(listenerName.value)
    ).iterator.asJava)

    val brokers = Seq(new RegisterBrokerRecord()
        .setBrokerId(0)
        .setFenced(false)
        .setEndPoints(endPoints))

    val topicRecords = Seq(new TopicRecord()
        .setName(topic)
        .setTopicId(topicId))
    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0, 1)
    val isr = asList[Integer](0)

    val partitionStates = Seq(
      new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setLeader(leader)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setPartitionEpoch(partitionEpoch)
        .setReplicas(replicas))
    MetadataCacheTest.updateCache(cache, brokers ++ topicRecords ++ partitionStates)

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

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataIsrNotAvailable(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicId = Uuid.randomUuid()

    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

    val endpoints = new BrokerEndpointCollection(Seq(new BrokerEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setName(listenerName.value)
    ).iterator.asJava)

    val brokers = Seq(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setRack("rack1")
      .setFenced(false)
      .setEndPoints(endpoints))

    val topicRecords = Seq(new TopicRecord()
      .setName(topic)
      .setTopicId(topicId))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)

    val partitionStates = Seq(new PartitionRecord()
      .setTopicId(topicId)
      .setPartitionId(0)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setReplicas(replicas))
    MetadataCacheTest.updateCache(cache, brokers ++ topicRecords ++ partitionStates)

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

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataWithNonSupportedSecurityProtocol(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicId = Uuid.randomUuid()
    val securityProtocol = SecurityProtocol.PLAINTEXT

    val brokers = new RegisterBrokerRecord()
      .setBrokerId(0)
      .setRack("")
      .setEndPoints(new BrokerEndpointCollection(Seq(new BrokerEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setName(ListenerName.forSecurityProtocol(securityProtocol).value)
      ).iterator.asJava))

    val topicRecord = new TopicRecord().setName(topic).setTopicId(topicId)

    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)
    val partitionStates = Seq(new PartitionRecord()
      .setTopicId(topicId)
      .setPartitionId(0)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setReplicas(replicas))
    MetadataCacheTest.updateCache(cache, Seq(brokers, topicRecord) ++ partitionStates)

    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
    assertEquals(1, topicMetadata.size)
    assertEquals(1, topicMetadata.head.partitions.size)
    assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, topicMetadata.head.partitions.get(0).leaderId)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getAliveBrokersShouldNotBeMutatedByUpdateCache(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicId = Uuid.randomUuid()
    val topicRecords = Seq(new TopicRecord().setName(topic).setTopicId(topicId))

    def updateCache(brokerIds: Seq[Int]): Unit = {
      val brokers = brokerIds.map { brokerId =>
        val securityProtocol = SecurityProtocol.PLAINTEXT
        new RegisterBrokerRecord()
          .setBrokerId(brokerId)
          .setRack("")
          .setFenced(false)
          .setBrokerEpoch(brokerEpoch)
          .setEndPoints(new BrokerEndpointCollection(Seq(new BrokerEndpoint()
            .setHost("foo")
            .setPort(9092)
            .setSecurityProtocol(securityProtocol.id)
            .setName(ListenerName.forSecurityProtocol(securityProtocol).value)
          ).iterator.asJava))
      }
      val leader = 0
      val leaderEpoch = 0
      val replicas = asList[Integer](0)
      val isr = asList[Integer](0, 1)
      val partitionStates = Seq(new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setLeader(leader)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setReplicas(replicas))

      MetadataCacheTest.updateCache(cache, brokers ++ topicRecords ++ partitionStates)
    }

    val initialBrokerIds = (0 to 2)
    updateCache(initialBrokerIds)
    val aliveBrokersFromCache = cache.getAliveBrokers()
    // This should not change `aliveBrokersFromCache`
    updateCache((0 to 3))
    assertEquals(initialBrokerIds.toSet, aliveBrokersFromCache.map(_.id).toSet)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def testGetPartitionReplicaEndpoints(cache: MetadataCache): Unit = {
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

    // Set up broker data for the metadata cache
    val numBrokers = 10
    val fencedBrokerId = numBrokers / 3
    val brokerRecords = (0 until numBrokers).map { brokerId =>
      new RegisterBrokerRecord()
        .setBrokerId(brokerId)
        .setFenced(brokerId == fencedBrokerId)
        .setRack("rack" + (brokerId % 3))
        .setEndPoints(new BrokerEndpointCollection(
          Seq(new BrokerEndpoint()
            .setHost("foo" + brokerId)
            .setPort(9092)
            .setSecurityProtocol(securityProtocol.id)
            .setName(listenerName.value)
          ).iterator.asJava))
    }

    // Set up a single topic (with many partitions) for the metadata cache
    val topic = "many-partitions-topic"
    val topicId = Uuid.randomUuid()
    val topicRecords = Seq[ApiMessage](new TopicRecord().setName(topic).setTopicId(topicId))

    // Set up a number of partitions such that each different combination of
    // $replicationFactor brokers is made a replica set for exactly one partition
    val replicationFactor = 3
    val replicaSets = getAllReplicaSets(numBrokers, replicationFactor)
    val numPartitions = replicaSets.length
    val partitionRecords = (0 until numPartitions).map { partitionId =>
      val replicas = replicaSets(partitionId)
      val nonFencedReplicas = replicas.stream().filter(id => id != fencedBrokerId).collect(Collectors.toList())
      new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(partitionId)
        .setReplicas(replicas)
        .setLeader(replicas.get(0))
        .setIsr(nonFencedReplicas)
        .setEligibleLeaderReplicas(nonFencedReplicas)
    }

    // Load the prepared data in the metadata cache
    MetadataCacheTest.updateCache(cache, brokerRecords ++ topicRecords ++ partitionRecords)

    (0 until numPartitions).foreach { partitionId =>
      val tp = new TopicPartition(topic, partitionId)
      val brokerIdToNodeMap = cache.getPartitionReplicaEndpoints(tp, listenerName)
      val replicaSet = brokerIdToNodeMap.keySet
      val expectedReplicaSet = partitionRecords(partitionId).replicas().asScala.toSet
      // Verify that we have endpoints for exactly the non-fenced brokers of the replica set
      if (expectedReplicaSet.contains(fencedBrokerId)) {
        assertEquals(expectedReplicaSet,
                     replicaSet + fencedBrokerId,
                     s"Unexpected partial replica set for partition $partitionId")
      } else {
        assertEquals(expectedReplicaSet,
                     replicaSet,
                     s"Unexpected replica set for partition $partitionId")
      }
      // Verify that the endpoint data for each non-fenced replica is as expected
      replicaSet.foreach { brokerId =>
        val brokerNode =
          brokerIdToNodeMap.getOrElse(
            brokerId, fail(s"No brokerNode for broker $brokerId and partition $partitionId"))
        val expectedBroker = brokerRecords(brokerId)
        val expectedEndpoint = expectedBroker.endPoints().find(listenerName.value())
        assertEquals(expectedEndpoint.host(),
                     brokerNode.host(),
                     s"Unexpected host for broker $brokerId and partition $partitionId")
        assertEquals(expectedEndpoint.port(),
                     brokerNode.port(),
                     s"Unexpected port for broker $brokerId and partition $partitionId")
        assertEquals(expectedBroker.rack(),
                     brokerNode.rack(),
                     s"Unexpected rack for broker $brokerId and partition $partitionId")
      }
    }

    val tp = new TopicPartition(topic, numPartitions)
    val brokerIdToNodeMap = cache.getPartitionReplicaEndpoints(tp, listenerName)
    assertTrue(brokerIdToNodeMap.isEmpty)
  }

  private def getAllReplicaSets(numBrokers: Int,
                                replicationFactor: Int): Array[util.List[Integer]] = {
    (0 until numBrokers)
      .combinations(replicationFactor)
      .map(replicaSet => replicaSet.map(Integer.valueOf).toList.asJava)
      .toArray
  }

  @Test
  def testIsBrokerFenced(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0, () => KRaftVersion.KRAFT_VERSION_0)

    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setFenced(false))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertFalse(metadataCache.isBrokerFenced(0))

    delta.replay(new BrokerRegistrationChangeRecord()
      .setBrokerId(0)
      .setFenced(1.toByte))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertTrue(metadataCache.isBrokerFenced(0))
  }

  @Test
  def testGetAliveBrokersWithBrokerFenced(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0, () => KRaftVersion.KRAFT_VERSION_0)
    val listenerName = "listener"
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(new BrokerEndpoint().
      setName(listenerName).
      setHost("foo").
      setPort(123).
      setSecurityProtocol(0))
    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setFenced(false)
      .setEndPoints(endpoints))
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(1)
      .setFenced(false)
      .setEndPoints(endpoints))
    delta.replay(new BrokerRegistrationChangeRecord()
      .setBrokerId(1)
      .setFenced(1.toByte))

    val metadataImage = delta.apply(MetadataProvenance.EMPTY)

    metadataCache.setImage(metadataImage)
    assertFalse(metadataCache.isBrokerFenced(0))
    assertTrue(metadataCache.isBrokerFenced(1))

    val aliveBrokers = metadataCache.getAliveBrokers().map(_.id).toSet
    metadataImage.cluster().brokers().forEach { (brokerId, registration) =>
      assertEquals(!registration.fenced(), aliveBrokers.contains(brokerId))
      assertEquals(aliveBrokers.contains(brokerId), metadataCache.getAliveBrokerNode(brokerId, new ListenerName(listenerName)).isDefined)
    }
  }

  @Test
  def testIsBrokerInControlledShutdown(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0, () => KRaftVersion.KRAFT_VERSION_0)

    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setInControlledShutdown(false))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertFalse(metadataCache.isBrokerShuttingDown(0))

    delta.replay(new BrokerRegistrationChangeRecord()
      .setBrokerId(0)
      .setInControlledShutdown(1.toByte))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertTrue(metadataCache.isBrokerShuttingDown(0))
  }

  @Test
  def testGetLiveBrokerEpoch(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0, () => KRaftVersion.KRAFT_VERSION_0)

    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setBrokerEpoch(100)
      .setFenced(false))

    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(1)
      .setBrokerEpoch(101)
      .setFenced(true))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertEquals(100L, metadataCache.getAliveBrokerEpoch(0).getOrElse(-1L))
    assertEquals(-1L, metadataCache.getAliveBrokerEpoch(1).getOrElse(-1L))
  }

  @Test
  def testDescribeTopicResponse(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0, () => KRaftVersion.KRAFT_VERSION_0)

    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val topic0 = "test0"
    val topic1 = "test1"

    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put(topic0, Uuid.randomUuid())
    topicIds.put(topic1, Uuid.randomUuid())

    val partitionMap = Map[(String, Int), PartitionRecord](
      (topic0, 0) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(0)
        .setReplicas(asList(0, 1, 2))
        .setLeader(0)
        .setIsr(asList(0))
        .setEligibleLeaderReplicas(asList(1))
        .setLastKnownElr(asList(2))
        .setLeaderEpoch(0)
        .setPartitionEpoch(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
      (topic0, 2) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(2)
        .setReplicas(asList(0, 2, 3))
        .setLeader(3)
        .setIsr(asList(3))
        .setEligibleLeaderReplicas(asList(2))
        .setLastKnownElr(asList(0))
        .setLeaderEpoch(1)
        .setPartitionEpoch(2)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
      (topic0, 1) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(1)
        .setReplicas(asList(0, 1, 3))
        .setLeader(0)
        .setIsr(asList(0))
        .setEligibleLeaderReplicas(asList(1))
        .setLastKnownElr(asList(3))
        .setLeaderEpoch(0)
        .setPartitionEpoch(2)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
      (topic1, 0) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic1))
        .setPartitionId(0)
        .setReplicas(asList(0, 1, 2))
        .setLeader(2)
        .setIsr(asList(2))
        .setEligibleLeaderReplicas(asList(1))
        .setLastKnownElr(asList(0))
        .setLeaderEpoch(10)
        .setPartitionEpoch(11)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
    )
    new BrokerEndpointCollection()
    val brokers = Seq(
      new RegisterBrokerRecord().setBrokerEpoch(brokerEpoch).setFenced(false).setBrokerId(0)
        .setEndPoints(new BrokerEndpointCollection(Seq(new BrokerEndpoint().setHost("foo0").setPort(9092)
          .setSecurityProtocol(securityProtocol.id).setName(listenerName.value)
        ).iterator.asJava)),
      new RegisterBrokerRecord().setBrokerEpoch(brokerEpoch).setFenced(false).setBrokerId(1)
        .setEndPoints(new BrokerEndpointCollection(Seq(new BrokerEndpoint().setHost("foo1").setPort(9093)
          .setSecurityProtocol(securityProtocol.id).setName(listenerName.value)
        ).iterator.asJava)),
      new RegisterBrokerRecord().setBrokerEpoch(brokerEpoch).setFenced(false).setBrokerId(2)
        .setEndPoints(new BrokerEndpointCollection(Seq(new BrokerEndpoint().setHost("foo2").setPort(9094)
          .setSecurityProtocol(securityProtocol.id).setName(listenerName.value)
        ).iterator.asJava)),
      new RegisterBrokerRecord().setBrokerEpoch(brokerEpoch).setFenced(false).setBrokerId(3)
        .setEndPoints(new BrokerEndpointCollection(Seq(new BrokerEndpoint().setHost("foo3").setPort(9095)
          .setSecurityProtocol(securityProtocol.id).setName(listenerName.value)
        ).iterator.asJava)),
    )

    var recordSeq = Seq[ApiMessage](
      new TopicRecord().setName(topic0).setTopicId(topicIds.get(topic0)),
      new TopicRecord().setName(topic1).setTopicId(topicIds.get(topic1))
    )
    recordSeq = recordSeq ++ partitionMap.values.toSeq
    MetadataCacheTest.updateCache(metadataCache, brokers ++ recordSeq)

    def checkTopicMetadata(topic: String, partitionIds: Set[Int], partitions: mutable.Buffer[DescribeTopicPartitionsResponsePartition]): Unit = {
      partitions.foreach(partition => {
        val partitionId = partition.partitionIndex()
        assertTrue(partitionIds.contains(partitionId))
        val expectedPartition = partitionMap.get((topic, partitionId)).get
        assertEquals(0, partition.errorCode())
        assertEquals(expectedPartition.leaderEpoch(), partition.leaderEpoch())
        assertEquals(expectedPartition.partitionId(), partition.partitionIndex())
        assertEquals(expectedPartition.eligibleLeaderReplicas(), partition.eligibleLeaderReplicas())
        assertEquals(expectedPartition.isr(), partition.isrNodes())
        assertEquals(expectedPartition.lastKnownElr(), partition.lastKnownElr())
        assertEquals(expectedPartition.leader(), partition.leaderId())
      })
    }

    // Basic test
    var result = metadataCache.describeTopicResponse(Seq(topic0, topic1).iterator, listenerName, _ => 0, 10, false).topics().asScala.toList
    assertEquals(2, result.size)
    var resultTopic = result(0)
    assertEquals(topic0, resultTopic.name())
    assertEquals(0, resultTopic.errorCode())
    assertEquals(topicIds.get(topic0), resultTopic.topicId())
    assertEquals(3, resultTopic.partitions().size())
    checkTopicMetadata(topic0, Set(0, 1, 2), resultTopic.partitions().asScala)

    resultTopic = result(1)
    assertEquals(topic1, resultTopic.name())
    assertEquals(0, resultTopic.errorCode())
    assertEquals(topicIds.get(topic1), resultTopic.topicId())
    assertEquals(1, resultTopic.partitions().size())
    checkTopicMetadata(topic1, Set(0), resultTopic.partitions().asScala)

    // Quota reached
    var response = metadataCache.describeTopicResponse(Seq(topic0, topic1).iterator, listenerName, _ => 0, 2, false)
    result = response.topics().asScala.toList
    assertEquals(1, result.size)
    resultTopic = result(0)
    assertEquals(topic0, resultTopic.name())
    assertEquals(0, resultTopic.errorCode())
    assertEquals(topicIds.get(topic0), resultTopic.topicId())
    assertEquals(2, resultTopic.partitions().size())
    checkTopicMetadata(topic0, Set(0, 1), resultTopic.partitions().asScala)
    assertEquals(topic0, response.nextCursor().topicName())
    assertEquals(2, response.nextCursor().partitionIndex())

    // With start index
    result = metadataCache.describeTopicResponse(Seq(topic0).iterator, listenerName, t => if (t.equals(topic0)) 1 else 0, 10, false).topics().asScala.toList
    assertEquals(1, result.size)
    resultTopic = result(0)
    assertEquals(topic0, resultTopic.name())
    assertEquals(0, resultTopic.errorCode())
    assertEquals(topicIds.get(topic0), resultTopic.topicId())
    assertEquals(2, resultTopic.partitions().size())
    checkTopicMetadata(topic0, Set(1, 2), resultTopic.partitions().asScala)

    // With start index and quota reached
    response = metadataCache.describeTopicResponse(Seq(topic0, topic1).iterator, listenerName, t => if (t.equals(topic0)) 2 else 0, 1, false)
    result = response.topics().asScala.toList
    assertEquals(1, result.size)

    resultTopic = result(0)
    assertEquals(topic0, resultTopic.name())
    assertEquals(0, resultTopic.errorCode())
    assertEquals(topicIds.get(topic0), resultTopic.topicId())
    assertEquals(1, resultTopic.partitions().size())
    checkTopicMetadata(topic0, Set(2), resultTopic.partitions().asScala)
    assertEquals(topic1, response.nextCursor().topicName())
    assertEquals(0, response.nextCursor().partitionIndex())

    // When the first topic does not exist
    result = metadataCache.describeTopicResponse(Seq("Non-exist", topic0).iterator, listenerName, t => if (t.equals("Non-exist")) 1 else 0, 1, false).topics().asScala.toList
    assertEquals(2, result.size)
    resultTopic = result(0)
    assertEquals("Non-exist", resultTopic.name())
    assertEquals(3, resultTopic.errorCode())

    resultTopic = result(1)
    assertEquals(topic0, resultTopic.name())
    assertEquals(0, resultTopic.errorCode())
    assertEquals(topicIds.get(topic0), resultTopic.topicId())
    assertEquals(1, resultTopic.partitions().size())
    checkTopicMetadata(topic0, Set(0), resultTopic.partitions().asScala)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def testGetLeaderAndIsr(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    val leader = 0
    val leaderEpoch = 0
    val isr = asList[Integer](2, 3, 0)
    val replicas = asList[Integer](2, 3, 0, 1, 4)

    val topicRecords = Seq(new TopicRecord().setName(topic).setTopicId(topicId))

    val partitionStates = Seq(new PartitionRecord()
      .setTopicId(topicId)
      .setPartitionId(partitionIndex)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setReplicas(replicas))

    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setBrokerEpoch(brokerEpoch)
      .setRack("rack1")
      .setEndPoints(new BrokerEndpointCollection(
        Seq(new BrokerEndpoint()
          .setHost("foo")
          .setPort(9092)
          .setSecurityProtocol(securityProtocol.id)
          .setName(listenerName.value)
        ).iterator.asJava)))

    MetadataCacheTest.updateCache(cache, brokers ++ topicRecords ++ partitionStates)

    val leaderAndIsr = cache.getLeaderAndIsr(topic, partitionIndex)
    assertEquals(Some(leader), leaderAndIsr.map(_.leader()))
    assertEquals(Some(leaderEpoch), leaderAndIsr.map(_.leaderEpoch()))
    assertEquals(Some(isr), leaderAndIsr.map(_.isr()))
    assertEquals(Some(-1), leaderAndIsr.map(_.partitionEpoch()))
    assertEquals(Some(LeaderRecoveryState.RECOVERED), leaderAndIsr.map(_.leaderRecoveryState()))
  }

  @Test
  def testGetOfflineReplicasConsidersDirAssignment(): Unit = {
    case class Broker(id: Int, dirs: util.List[Uuid])
    case class Partition(id: Int, replicas: util.List[Integer], dirs: util.List[Uuid])

    def offlinePartitions(brokers: Seq[Broker], partitions: Seq[Partition]): Map[Int, util.List[Integer]] = {
      val delta = new MetadataDelta.Builder().build()
      brokers.foreach(broker => delta.replay(
        new RegisterBrokerRecord().setFenced(false).
          setBrokerId(broker.id).setLogDirs(broker.dirs).
          setEndPoints(new BrokerEndpointCollection(Collections.singleton(
            new RegisterBrokerRecord.BrokerEndpoint().setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
              setPort(9093.toShort).setName("PLAINTEXT").setHost(s"broker-${broker.id}")).iterator()))))
      val topicId = Uuid.fromString("95OVr1IPRYGrcNCLlpImCA")
      delta.replay(new TopicRecord().setTopicId(topicId).setName("foo"))
      partitions.foreach(partition => delta.replay(
        new PartitionRecord().setTopicId(topicId).setPartitionId(partition.id).
          setReplicas(partition.replicas).setDirectories(partition.dirs).
          setLeader(partition.replicas.get(0)).setIsr(partition.replicas)))
      val cache = MetadataCache.kRaftMetadataCache(1, () => KRaftVersion.KRAFT_VERSION_0)
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      val topicMetadata = cache.getTopicMetadata(Set("foo"), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).head
      topicMetadata.partitions().asScala.map(p => (p.partitionIndex(), p.offlineReplicas())).toMap
    }

    val brokers = Seq(
      Broker(0, asList(Uuid.fromString("broker1logdirjEo71BG0w"))),
      Broker(1, asList(Uuid.fromString("broker2logdirRmQQgLxgw")))
    )
    val partitions = Seq(
      Partition(0, asList(0, 1), asList(Uuid.fromString("broker1logdirjEo71BG0w"), DirectoryId.LOST)),
      Partition(1, asList(0, 1), asList(Uuid.fromString("unknownlogdirjEo71BG0w"), DirectoryId.UNASSIGNED)),
      Partition(2, asList(0, 1), asList(DirectoryId.MIGRATING, Uuid.fromString("broker2logdirRmQQgLxgw")))
    )
    assertEquals(Map(
      0 -> asList(1),
      1 -> asList(0),
      2 -> asList(),
    ), offlinePartitions(brokers, partitions))
  }


  val oldRequestControllerEpoch: Int = 122
  val newRequestControllerEpoch: Int = 123

  val fooTopicName: String = "foo"
  val fooTopicId: Uuid = Uuid.fromString("HDceyWK0Ry-j3XLR8DvvGA")
  val oldFooPart0 = new PartitionRecord().
    setTopicId(fooTopicId).
    setPartitionId(0).
    setLeader(4).
    setIsr(java.util.Arrays.asList(4, 5, 6)).
    setReplicas(java.util.Arrays.asList(4, 5, 6))
  val newFooPart0 = new PartitionRecord().
    setTopicId(fooTopicId).
    setPartitionId(0).
    setLeader(5).
    setIsr(java.util.Arrays.asList(4, 5, 6)).
    setReplicas(java.util.Arrays.asList(4, 5, 6))
  val oldFooPart1 = new PartitionRecord().
    setTopicId(fooTopicId).
    setPartitionId(1).
    setLeader(5).
    setIsr(java.util.Arrays.asList(4, 5, 6)).
    setReplicas(java.util.Arrays.asList(4, 5, 6))
  val newFooPart1 = new PartitionRecord().
    setTopicId(fooTopicId).
    setPartitionId(1).
    setLeader(5).
    setIsr(java.util.Arrays.asList(4, 5)).
    setReplicas(java.util.Arrays.asList(4, 5, 6))
  val barTopicName: String = "bar"
  val barTopicId: Uuid = Uuid.fromString("97FBD1g4QyyNNZNY94bkRA")
  val recreatedBarTopicId: Uuid = Uuid.fromString("lZokxuaPRty7c5P4dNdTYA")
  val oldBarPart0 = new PartitionRecord().
    setTopicId(fooTopicId).
    setPartitionId(0).
    setLeader(7).
    setIsr(java.util.Arrays.asList(7, 8)).
    setReplicas(java.util.Arrays.asList(7, 8, 9))
  val newBarPart0 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(0).
    setLeader(7).
    setIsr(java.util.Arrays.asList(7, 8)).
    setReplicas(java.util.Arrays.asList(7, 8, 9))
  val deletedBarPart0 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(0).
    setLeader(-2).
    setIsr(java.util.Arrays.asList(7, 8)).
    setReplicas(java.util.Arrays.asList(7, 8, 9))
  val oldBarPart1 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(1).
    setLeader(5).
    setIsr(java.util.Arrays.asList(4, 5, 6)).
    setReplicas(java.util.Arrays.asList(4, 5, 6))
  val newBarPart1 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(1).
    setLeader(5).
    setIsr(java.util.Arrays.asList(4, 5, 6)).
    setReplicas(java.util.Arrays.asList(4, 5, 6))
  val deletedBarPart1 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(1).
    setLeader(-2).
    setIsr(java.util.Arrays.asList(4, 5, 6)).
    setReplicas(java.util.Arrays.asList(4, 5, 6))

  val oldBarPart2 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(2).
    setLeader(9).
    setIsr(java.util.Arrays.asList(7, 8, 9)).
    setReplicas(java.util.Arrays.asList(7, 8, 9))

  val newBarPart2 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(2).
    setLeader(8).
    setIsr(java.util.Arrays.asList(7, 8)).
    setReplicas(java.util.Arrays.asList(7, 8, 9))

  val deletedBarPart2 = new PartitionRecord().
    setTopicId(barTopicId).
    setPartitionId(2).
    setLeader(-2).
    setIsr(java.util.Arrays.asList(7, 8, 9)).
    setReplicas(java.util.Arrays.asList(7, 8, 9))
}

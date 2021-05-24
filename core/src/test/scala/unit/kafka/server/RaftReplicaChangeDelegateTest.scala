/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Collections

import kafka.cluster.Partition
import kafka.controller.StateChangeLogger
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.server.metadata.{MetadataBroker, MetadataBrokers, MetadataPartition}
import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import scala.jdk.CollectionConverters._

class RaftReplicaChangeDelegateTest {
  private val listenerName = new ListenerName("PLAINTEXT")

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testLeaderAndIsrPropagation(isLeader: Boolean): Unit = {
    val leaderId = 0
    val topicPartition = new TopicPartition("foo", 5)
    val replicas = Seq(0, 1, 2).map(Int.box).asJava
    val topicId = Uuid.randomUuid()
    val topicIds = (topicName: String) => if (topicName == "foo") Some(topicId) else None

    val helper = mockedHelper()
    val partition = mock(classOf[Partition])
    when(partition.topicPartition).thenReturn(topicPartition)
    when(partition.topic).thenReturn(topicPartition.topic)

    val highWatermarkCheckpoints = mock(classOf[OffsetCheckpoints])
    when(highWatermarkCheckpoints.fetch(
      anyString(),
      ArgumentMatchers.eq(topicPartition)
    )).thenReturn(None)

    val metadataPartition = new MetadataPartition(
      topicName = topicPartition.topic,
      partitionIndex = topicPartition.partition,
      leaderId = leaderId,
      leaderEpoch = 27,
      replicas = replicas,
      isr = replicas,
      partitionEpoch = 50,
      offlineReplicas = Collections.emptyList(),
      addingReplicas = Collections.emptyList(),
      removingReplicas = Collections.emptyList()
    )

    val expectedLeaderAndIsr = new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
      .setTopicName(topicPartition.topic)
      .setPartitionIndex(topicPartition.partition)
      .setIsNew(true)
      .setLeader(leaderId)
      .setLeaderEpoch(27)
      .setReplicas(replicas)
      .setIsr(replicas)
      .setAddingReplicas(Collections.emptyList())
      .setRemovingReplicas(Collections.emptyList())
      .setZkVersion(50)

    val delegate = new RaftReplicaChangeDelegate(helper)
    val updatedPartitions = if (isLeader) {
      when(partition.makeLeader(expectedLeaderAndIsr, highWatermarkCheckpoints, Some(topicId)))
        .thenReturn(true)
      delegate.makeLeaders(
        prevPartitionsAlreadyExisting = Set.empty,
        partitionStates = Map(partition -> metadataPartition),
        highWatermarkCheckpoints,
        metadataOffset = Some(500),
        topicIds
      )
    } else {
      when(partition.makeFollower(expectedLeaderAndIsr, highWatermarkCheckpoints, Some(topicId)))
        .thenReturn(true)
      when(partition.leaderReplicaIdOpt).thenReturn(Some(leaderId))
      delegate.makeFollowers(
        prevPartitionsAlreadyExisting = Set.empty,
        currentBrokers = aliveBrokers(replicas),
        partitionStates = Map(partition -> metadataPartition),
        highWatermarkCheckpoints,
        metadataOffset = Some(500),
        topicIds
      )
    }

    assertEquals(Set(partition), updatedPartitions)
  }

  private def aliveBrokers(replicas: java.util.List[Integer]): MetadataBrokers = {
    def mkNode(replicaId: Int): Node = {
      new Node(replicaId, "localhost", 9092 + replicaId, "")
    }

    val brokers = replicas.asScala.map { replicaId =>
      replicaId -> MetadataBroker(
        id = replicaId,
        rack = "",
        endpoints = Map(listenerName.value -> mkNode(replicaId)),
        fenced = false
      )
    }.toMap

    MetadataBrokers(brokers.values.toList.asJava, brokers.asJava)
  }

  private def mockedHelper(): RaftReplicaChangeDelegateHelper = {
    val helper = mock(classOf[RaftReplicaChangeDelegateHelper])

    val stateChangeLogger = mock(classOf[StateChangeLogger])
    when(helper.stateChangeLogger).thenReturn(stateChangeLogger)
    when(stateChangeLogger.isDebugEnabled).thenReturn(false)
    when(stateChangeLogger.isTraceEnabled).thenReturn(false)

    val replicaFetcherManager = mock(classOf[ReplicaFetcherManager])
    when(helper.replicaFetcherManager).thenReturn(replicaFetcherManager)

    val replicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    when(helper.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)

    val config = mock(classOf[KafkaConfig])
    when(config.interBrokerListenerName).thenReturn(listenerName)
    when(helper.config).thenReturn(config)

    helper
  }

}

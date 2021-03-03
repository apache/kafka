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
package kafka.server.metadata

import java.util.Properties

import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.server.RaftReplicaManager
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.{ConfigRecord, PartitionRecord, RemoveTopicRecord, TopicRecord}
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import kafka.utils.Implicits._

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class BrokerMetadataListenerTest {

  private val brokerId = 1
  private val time = new MockTime()
  private val configRepository = new CachedConfigRepository
  private val metadataCache = new RaftMetadataCache(brokerId)
  private val groupCoordinator = mock(classOf[GroupCoordinator])
  private val replicaManager = mock(classOf[RaftReplicaManager])
  private val txnCoordinator = mock(classOf[TransactionCoordinator])
  private val clientQuotaManager = mock(classOf[ClientQuotaMetadataManager])

  private val listener = new BrokerMetadataListener(
    brokerId,
    time,
    metadataCache,
    configRepository,
    groupCoordinator,
    replicaManager,
    txnCoordinator,
    threadNamePrefix = None,
    clientQuotaManager
  )

  @Test
  def testTopicCreationAndDeletion(): Unit = {
    val topicId = Uuid.randomUuid()
    val topic = "foo"
    val numPartitions = 10
    val config = Map("cleanup.policy" -> "compact")
    val localPartitions = createAndAssert(topicId, topic, config, numPartitions, numBrokers = 4)
    deleteAndAssert(topicId, topic, numPartitions, localPartitions)
  }

  private def deleteAndAssert(
    topicId: Uuid,
    topic: String,
    numPartitions: Int,
    localPartitions: Set[TopicPartition]
  ): Unit = {
    val deleteRecord = new RemoveTopicRecord()
      .setTopicId(topicId)

    listener.execCommits(lastOffset = 3L, List[ApiMessage](
      deleteRecord,
    ).asJava)

    assertFalse(metadataCache.contains(topic))
    assertEquals(new Properties, configRepository.topicConfig(topic))

    verify(groupCoordinator).handleDeletedPartitions(ArgumentMatchers.argThat[Seq[TopicPartition]] { partitions =>
      partitions.toSet == partitionSet(topic, numPartitions)
    })

    val deleteImageCapture: ArgumentCaptor[MetadataImageBuilder] =
      ArgumentCaptor.forClass(classOf[MetadataImageBuilder])
    verify(replicaManager).handleMetadataRecords(
      deleteImageCapture.capture(),
      ArgumentMatchers.eq(3L),
      any()
    )

    val deleteImage = deleteImageCapture.getValue
    assertTrue(deleteImage.hasPartitionChanges)
    val localRemoved = deleteImage.partitionsBuilder().localRemoved()
    assertEquals(localPartitions, localRemoved.map(_.toTopicPartition).toSet)
  }

  private def createAndAssert(
    topicId: Uuid,
    topic: String,
    config: Map[String, String],
    numPartitions: Int,
    numBrokers: Int
  ): Set[TopicPartition] = {
    // First create the topic and verify state is updated
    val records = new java.util.ArrayList[ApiMessage]
    records.add(new TopicRecord()
      .setName(topic)
      .setTopicId(topicId)
    )

    val localTopicPartitions = mutable.Set.empty[TopicPartition]
    (0 until numPartitions).map { partitionId =>
      val preferredLeaderId = partitionId % numBrokers
      val replicas = asJavaList(Seq(
        preferredLeaderId,
        preferredLeaderId + 1,
        preferredLeaderId + 2
      ))

      if (replicas.contains(brokerId)) {
        localTopicPartitions.add(new TopicPartition(topic, partitionId))
      }

      records.add(new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(partitionId)
        .setLeader(preferredLeaderId)
        .setLeaderEpoch(0)
        .setPartitionEpoch(0)
        .setReplicas(replicas)
        .setIsr(replicas)
      )
    }

    config.forKeyValue { (key, value) =>
      records.add(new ConfigRecord()
        .setResourceName(topic)
        .setResourceType(ConfigResource.Type.TOPIC.id())
        .setName(key)
        .setValue(value)
      )
    }

    listener.execCommits(lastOffset = 2L, records)
    assertTrue(metadataCache.contains(topic))
    assertEquals(Some(numPartitions), metadataCache.numPartitions(topic))
    assertEquals(config, configRepository.topicConfig(topic).asScala)

    val imageCapture: ArgumentCaptor[MetadataImageBuilder] =
      ArgumentCaptor.forClass(classOf[MetadataImageBuilder])
    verify(replicaManager).handleMetadataRecords(
      imageCapture.capture(),
      ArgumentMatchers.eq(2L),
      any()
    )

    val createImage = imageCapture.getValue
    assertTrue(createImage.hasPartitionChanges)
    val localChanged = createImage.partitionsBuilder().localChanged()
    assertEquals(localTopicPartitions, localChanged.map(_.toTopicPartition).toSet)

    localTopicPartitions.toSet
  }

  private def partitionSet(topic: String, numPartitions: Int): Set[TopicPartition] = {
    (0 until numPartitions).map(new TopicPartition(topic, _)).toSet
  }

  private def asJavaList(replicas: Iterable[Int]): java.util.List[Integer] = {
    replicas.map(Int.box).toList.asJava
  }

}

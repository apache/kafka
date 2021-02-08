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

package kafka.server

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Partition
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.{CachedConfigRepository, MetadataBroker, MetadataBrokers, MetadataImage, MetadataImageBuilder, MetadataPartition, RaftMetadataCache}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.metadata.PartitionRecord
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, never, verify, when}
import org.slf4j.Logger

import scala.collection.mutable

trait LeadershipChangeHandler {
  def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]): Unit
}

class RaftReplicaManagerTest {
  var alterIsrManager: AlterIsrManager = _
  var config: KafkaConfig = _
  val configRepository = new CachedConfigRepository()
  val metrics = new Metrics
  var quotaManager: QuotaManagers = _
  val time = new MockTime
  var mockDelegate: RaftReplicaChangeDelegate = _
  var imageBuilder: MetadataImageBuilder = _
  val brokerId0 = 0
  val metadataBroker0 = new MetadataBroker(brokerId0, null, Map.empty, false)
  val brokerId1 = 1
  val metadataBroker1 = new MetadataBroker(brokerId1, null, Map.empty, false)
  val topicName = "topicName"
  val topicId = Uuid.randomUuid()
  val partitionId0 = 0
  val partitionId1 = 1
  val topicPartition0 = new TopicPartition(topicName, partitionId0)
  val topicPartition1 = new TopicPartition(topicName, partitionId1)
  val topicPartitionRecord0 = new PartitionRecord()
    .setPartitionId(partitionId0)
    .setTopicId(topicId)
    .setReplicas(util.Arrays.asList(brokerId0, brokerId1))
    .setLeader(brokerId0)
    .setLeaderEpoch(0)
  val topicPartitionRecord1 = new PartitionRecord()
    .setPartitionId(partitionId1)
    .setTopicId(topicId)
    .setReplicas(util.Arrays.asList(brokerId0, brokerId1))
    .setLeader(brokerId1)
    .setLeaderEpoch(0)
  val offset1 = 1L
  val metadataPartition0 = MetadataPartition(topicName, topicPartitionRecord0, Some(offset1))
  val metadataPartition1 = MetadataPartition(topicName, topicPartitionRecord1, Some(offset1))
  val onLeadershipChange = mock(classOf[LeadershipChangeHandler]).onLeadershipChange _
  var metadataCache: RaftMetadataCache = _

  @BeforeEach
  def setUp(): Unit = {
    alterIsrManager = mock(classOf[AlterIsrManager])
    config = KafkaConfig.fromProps({
      val nodeId = brokerId0
      val props = TestUtils.createBrokerConfig(nodeId, "")
      props.put(KafkaConfig.ProcessRolesProp, "broker")
      props.put(KafkaConfig.NodeIdProp, nodeId.toString)
      props
    })
    metadataCache = new RaftMetadataCache(config.brokerId)
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "")
    mockDelegate = mock(classOf[RaftReplicaChangeDelegate])
    imageBuilder = MetadataImageBuilder(brokerId0, mock(classOf[Logger]), new MetadataImage())
  }

  @AfterEach
  def tearDown(): Unit = {
    TestUtils.clearYammerMetrics()
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  def createRaftReplicaManager(): RaftReplicaManager = {
    val mockLogMgr = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    new RaftReplicaManager(config, metrics, time, new MockScheduler(time), mockLogMgr,
      new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
      metadataCache, new LogDirFailureChannel(config.logDirs.size), alterIsrManager,
      configRepository, None)
  }

  @Test
  def testRejectsZkConfig(): Unit = {
    assertThrows(classOf[IllegalStateException], () => {
      val zkConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, ""))
      val mockLogMgr = TestUtils.createLogManager(zkConfig.logDirs.map(new File(_)))
      new RaftReplicaManager(zkConfig, metrics, time, new MockScheduler(time), mockLogMgr,
        new AtomicBoolean(false), quotaManager, new BrokerTopicStats,
        metadataCache, new LogDirFailureChannel(config.logDirs.size), alterIsrManager,
        configRepository)
    })
  }

  @Test
  def testDefersChangesImmediatelyThenAppliesChanges(): Unit = {
    val rrm = createRaftReplicaManager()
    rrm.delegate = mockDelegate

    processTopicPartitionMetadata(rrm)
    // verify changes would have been deferred
    val partitionsNewMapCaptor: ArgumentCaptor[mutable.Map[Partition, Boolean]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, Boolean]])
    verify(mockDelegate).makeDeferred(partitionsNewMapCaptor.capture(), ArgumentMatchers.eq(offset1), ArgumentMatchers.eq(onLeadershipChange))
    val partitionsDeferredMap = partitionsNewMapCaptor.getValue
    assertEquals(2, partitionsDeferredMap.size)
    val partition0 = partitionsDeferredMap.keys.filter(partition => partition.topicPartition == topicPartition0).head
    assertTrue(partitionsDeferredMap(partition0))
    val partition1 = partitionsDeferredMap.keys.filter(partition => partition.topicPartition == topicPartition1).head
    assertTrue(partitionsDeferredMap(partition1))
    verify(mockDelegate, never()).makeLeaders(any(), any(), any(), anyLong())
    verify(mockDelegate, never()).makeFollowers(any(), any(), any(), any(), any())
    Mockito.reset(mockDelegate)
    // now mark those topic partitions as being deferred so we can later apply the changes
    rrm.markPartitionDeferred(partition0, isNew = true, onLeadershipChange)
    rrm.markPartitionDeferred(partition1, isNew = true, onLeadershipChange)

    // apply the changes
    // first update the partitions in the metadata cache so they aren't marked as being deferred
    val imageBuilder2 = MetadataImageBuilder(brokerId0, mock(classOf[Logger]), metadataCache.currentImage())
    imageBuilder2.partitionsBuilder().set(MetadataPartition(metadataPartition0))
    imageBuilder2.partitionsBuilder().set(MetadataPartition(metadataPartition1))
    metadataCache.image(imageBuilder2.build())
    // define some return values to avoid NPE
    when(mockDelegate.makeLeaders(any(), any(), any(), anyLong())).thenReturn(Set.empty)
    when(mockDelegate.makeFollowers(any(), any(), any(), any(), anyLong())).thenReturn(Set.empty)
    rrm.endMetadataChangeDeferral()
    // verify that the deferred changes would have been applied
    val leaderPartitionStatesCaptor: ArgumentCaptor[mutable.Map[Partition, MetadataPartition]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, MetadataPartition]])
    verify(mockDelegate).makeLeaders(ArgumentMatchers.eq(mutable.Set()), leaderPartitionStatesCaptor.capture(), any(),
      ArgumentMatchers.eq(MetadataPartition.OffsetNeverDeferred))
    val followerPartitionStatesCaptor: ArgumentCaptor[mutable.Map[Partition, MetadataPartition]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, MetadataPartition]])
    val brokersCaptor: ArgumentCaptor[MetadataBrokers] = ArgumentCaptor.forClass(classOf[MetadataBrokers])
    verify(mockDelegate).makeFollowers(ArgumentMatchers.eq(mutable.Set()), brokersCaptor.capture(),
      followerPartitionStatesCaptor.capture(), any(), ArgumentMatchers.eq(MetadataPartition.OffsetNeverDeferred))
    val brokers = brokersCaptor.getValue
    assertEquals(2, brokers.size())
    assertTrue(brokers.aliveBroker(brokerId0).isDefined)
    assertTrue(brokers.aliveBroker(brokerId1).isDefined)
  }

  private def processTopicPartitionMetadata(raftReplicaManager: RaftReplicaManager): Unit = {
    // create brokers
    imageBuilder.brokersBuilder().add(metadataBroker0)
    imageBuilder.brokersBuilder().add(metadataBroker1)
    // create topic
    imageBuilder.partitionsBuilder().addUuidMapping(topicName, topicId)
    // create deferred partitions
    imageBuilder.partitionsBuilder().set(metadataPartition0)
    imageBuilder.partitionsBuilder().set(metadataPartition1)
    // apply the changes to metadata cache
    metadataCache.image(imageBuilder.build())
    // apply the changes to replica manager
    raftReplicaManager.handleMetadataRecords(imageBuilder, offset1, onLeadershipChange)
  }
}

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
import kafka.server.checkpoints.LazyOffsetCheckpoints
import kafka.server.metadata.{CachedConfigRepository, MetadataBroker, MetadataBrokers, MetadataImage, MetadataImageBuilder, MetadataPartition, RaftMetadataCache}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.errors.InconsistentTopicIdException
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.metadata.PartitionRecord
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify, when}
import org.slf4j.Logger

import scala.collection.{Set, mutable}

trait LeadershipChangeHandler {
  def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]): Unit
}

class RaftReplicaManagerTest {
  private var alterIsrManager: AlterIsrManager = _
  private var config: KafkaConfig = _
  private val configRepository = new CachedConfigRepository()
  private val metrics = new Metrics
  private var quotaManager: QuotaManagers = _
  private val time = new MockTime
  private var mockDelegate: RaftReplicaChangeDelegate = _
  private var imageBuilder: MetadataImageBuilder = _
  private val brokerId0 = 0
  private val metadataBroker0 = new MetadataBroker(brokerId0, null, Map.empty, false)
  private val brokerId1 = 1
  private val metadataBroker1 = new MetadataBroker(brokerId1, null, Map.empty, false)
  private val topicName = "topicName"
  private val topicId = Uuid.randomUuid()
  private val partitionId0 = 0
  private val partitionId1 = 1
  private val topicPartition0 = new TopicPartition(topicName, partitionId0)
  private val topicPartition1 = new TopicPartition(topicName, partitionId1)
  private val topicPartitionRecord0 = new PartitionRecord()
    .setPartitionId(partitionId0)
    .setTopicId(topicId)
    .setReplicas(util.Arrays.asList(brokerId0, brokerId1))
    .setLeader(brokerId0)
    .setLeaderEpoch(0)
  private val topicPartitionRecord1 = new PartitionRecord()
    .setPartitionId(partitionId1)
    .setTopicId(topicId)
    .setReplicas(util.Arrays.asList(brokerId0, brokerId1))
    .setLeader(brokerId1)
    .setLeaderEpoch(0)
  private val offset1 = 1L
  private val metadataPartition0 = MetadataPartition(topicName, topicPartitionRecord0)
  private val metadataPartition1 = MetadataPartition(topicName, topicPartitionRecord1)
  private var onLeadershipChangeHandler: LeadershipChangeHandler = _
  private var onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit = _
  private var metadataCache: RaftMetadataCache = _

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
    onLeadershipChangeHandler = mock(classOf[LeadershipChangeHandler])
    onLeadershipChange = onLeadershipChangeHandler.onLeadershipChange _
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
    val partition0 =  Partition(topicPartition0, time, configRepository, rrm)
    val partition1 =  Partition(topicPartition1, time, configRepository, rrm)

    processTopicPartitionMetadata(rrm)
    // verify changes would have been deferred
    val partitionsNewMapCaptor: ArgumentCaptor[mutable.Map[Partition, Boolean]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, Boolean]])
    verify(mockDelegate).makeDeferred(partitionsNewMapCaptor.capture(), ArgumentMatchers.eq(offset1))
    val partitionsDeferredMap = partitionsNewMapCaptor.getValue
    assertEquals(Map(partition0 -> true, partition1 -> true), partitionsDeferredMap)
    verify(mockDelegate, never()).makeFollowers(any(), any(), any(), any(), any(), any())

    // now mark those topic partitions as being deferred so we can later apply the changes
    rrm.markPartitionDeferred(partition0, isNew = true)
    rrm.markPartitionDeferred(partition1, isNew = true)

    // apply the changes
    // define some return values to avoid NPE
    when(mockDelegate.makeLeaders(any(), any(), any(), any(), any())).thenReturn(Set(partition0))
    when(mockDelegate.makeFollowers(any(), any(), any(), any(), any(), any())).thenReturn(Set(partition1))

    // Simulate creation of logs in makeLeaders/makeFollowers
    partition0.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(topicId))
    partition1.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(topicId))

    rrm.endMetadataChangeDeferral(onLeadershipChange)
    // verify that the deferred changes would have been applied

    // leaders...
    val leaderPartitionStates = verifyMakeLeaders(mutable.Set(), None)
    assertEquals(Map(partition0 -> metadataPartition0), leaderPartitionStates)

    // followers...
    val followerPartitionStates = verifyMakeFollowers(mutable.Set(), Set(brokerId0, brokerId1), None)
    assertEquals(Map(partition1 -> metadataPartition1), followerPartitionStates)

    // leadership change callbacks
    verifyLeadershipChangeCallbacks(List(partition0), List(partition1))

    // partition.metadata file
    verifyPartitionMetadataFile(rrm, List(topicPartition0, topicPartition1))
  }

  @Test
  def testAppliesChangesWhenNotDeferring(): Unit = {
    val rrm = createRaftReplicaManager()
    rrm.delegate = mockDelegate
    val partition0 = Partition(topicPartition0, time, configRepository, rrm)
    val partition1 = Partition(topicPartition1, time, configRepository, rrm)

    rrm.endMetadataChangeDeferral(onLeadershipChange)

    // define some return values to avoid NPE
    when(mockDelegate.makeLeaders(any(), any(), any(), ArgumentMatchers.eq(Some(offset1)), any())).thenReturn(Set(partition0))
    when(mockDelegate.makeFollowers(any(), any(), any(), any(), ArgumentMatchers.eq(Some(offset1)), any())).thenReturn(Set(partition1))

    // Simulate creation of logs in makeLeaders/makeFollowers
    partition0.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(topicId))
    partition1.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(topicId))

    // process the changes
    processTopicPartitionMetadata(rrm)
    // verify that the changes would have been applied

    // leaders...
    val leaderPartitionStates = verifyMakeLeaders(mutable.Set(), Some(offset1))
    assertEquals(Map(partition0 -> metadataPartition0), leaderPartitionStates)

    // followers...
    val followerPartitionStates = verifyMakeFollowers(mutable.Set(), Set(brokerId0, brokerId1), Some(offset1))
    assertEquals(Map(partition1 -> metadataPartition1), followerPartitionStates)

    // leadership change callbacks
    verifyLeadershipChangeCallbacks(List(partition0), List(partition1))

    // partition.metadata file
    verifyPartitionMetadataFile(rrm, List(topicPartition0, topicPartition1))
  }

  @Test
  def testInconsistentTopicIdDefersChanges(): Unit = {
    val rrm = createRaftReplicaManager()
    rrm.delegate = mockDelegate
    val partition0 = rrm.createPartition(topicPartition0)
    val partition1 = rrm.createPartition(topicPartition1)

    partition0.createLogIfNotExists(isNew = false, isFutureReplica = false,
     new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(Uuid.randomUuid()))
    partition1.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(Uuid.randomUuid()))

    try {
      processTopicPartitionMetadata(rrm)
    } catch {
      case e: Throwable => assertTrue(e.isInstanceOf[InconsistentTopicIdException])
    }
  }

  @Test
  def testInconsistentTopicIdWhenNotDeferring(): Unit = {
    val rrm = createRaftReplicaManager()
    rrm.delegate = mockDelegate
    val partition0 = rrm.createPartition(topicPartition0)
    val partition1 = rrm.createPartition(topicPartition1)

    partition0.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(Uuid.randomUuid()))
    partition1.createLogIfNotExists(isNew = false, isFutureReplica = false,
      new LazyOffsetCheckpoints(rrm.highWatermarkCheckpoints), Some(Uuid.randomUuid()))

    rrm.endMetadataChangeDeferral(onLeadershipChange)

    // define some return values to avoid NPE
    when(mockDelegate.makeLeaders(any(), any(), any(), ArgumentMatchers.eq(Some(offset1)), any())).thenReturn(Set(partition0))
    when(mockDelegate.makeFollowers(any(), any(), any(), any(), ArgumentMatchers.eq(Some(offset1)), any())).thenReturn(Set(partition1))

    try {
      processTopicPartitionMetadata(rrm)
    } catch {
      case e: Throwable => assertTrue(e.isInstanceOf[InconsistentTopicIdException])
    }
  }

  private def verifyMakeLeaders(expectedPrevPartitionsAlreadyExisting: Set[MetadataPartition],
                                expectedMetadataOffset: Option[Long]): mutable.Map[Partition, MetadataPartition] = {
    val leaderPartitionStatesCaptor: ArgumentCaptor[mutable.Map[Partition, MetadataPartition]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, MetadataPartition]])
    verify(mockDelegate).makeLeaders(ArgumentMatchers.eq(expectedPrevPartitionsAlreadyExisting),
      leaderPartitionStatesCaptor.capture(), any(), ArgumentMatchers.eq(expectedMetadataOffset), any())
    leaderPartitionStatesCaptor.getValue
  }

  private def verifyMakeFollowers(expectedPrevPartitionsAlreadyExisting: Set[MetadataPartition],
                                  expectedBrokers: Set[Int],
                                  expectedMetadataOffset: Option[Long]): mutable.Map[Partition, MetadataPartition] = {
    val followerPartitionStatesCaptor: ArgumentCaptor[mutable.Map[Partition, MetadataPartition]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, MetadataPartition]])
    val brokersCaptor: ArgumentCaptor[MetadataBrokers] = ArgumentCaptor.forClass(classOf[MetadataBrokers])
    verify(mockDelegate).makeFollowers(ArgumentMatchers.eq(expectedPrevPartitionsAlreadyExisting), brokersCaptor.capture(),
      followerPartitionStatesCaptor.capture(), any(), ArgumentMatchers.eq(expectedMetadataOffset), any())
    val brokers = brokersCaptor.getValue
    assertEquals(expectedBrokers.size, brokers.size())
    expectedBrokers.foreach(brokerId => assertTrue(brokers.aliveBroker(brokerId).isDefined))
    followerPartitionStatesCaptor.getValue
  }

  private def verifyLeadershipChangeCallbacks(expectedUpdatedLeaders: List[Partition], expectedUpdatedFollowers: List[Partition]): Unit = {
    val updatedLeadersCaptor: ArgumentCaptor[Iterable[Partition]] = ArgumentCaptor.forClass(classOf[Iterable[Partition]])
    val updatedFollowersCaptor: ArgumentCaptor[Iterable[Partition]] = ArgumentCaptor.forClass(classOf[Iterable[Partition]])
    verify(onLeadershipChangeHandler).onLeadershipChange(updatedLeadersCaptor.capture(), updatedFollowersCaptor.capture())
    assertEquals(expectedUpdatedLeaders, updatedLeadersCaptor.getValue.toList)
    assertEquals(expectedUpdatedFollowers, updatedFollowersCaptor.getValue.toList)
  }

  private def verifyPartitionMetadataFile(rrm: RaftReplicaManager, topicPartitions: List[TopicPartition]) = {
    topicPartitions.foreach ( topicPartition => {
      val log = rrm.getLog(topicPartition).get
      assertTrue(log.partitionMetadataFile.exists())
      val partitionMetadata = log.partitionMetadataFile.read()

      // Current version of PartitionMetadataFile is 0.
      assertEquals(0, partitionMetadata.version)
      assertEquals(topicId, partitionMetadata.topicId)
    })
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

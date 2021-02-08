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
import kafka.server.metadata.{CachedConfigRepository, MetadataImage, MetadataImageBuilder, MetadataPartition, RaftMetadataCache}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.metadata.PartitionRecord
import org.apache.kafka.common.metrics.Metrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, never, verify}
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
  val brokerId = 1
  val topicName = "topicName"
  val topicId = Uuid.randomUuid()
  val partitionId = 0
  val topicPartition = new TopicPartition(topicName, partitionId)
  val topicPartitionRecord = new PartitionRecord().setPartitionId(partitionId).setTopicId(topicId).setReplicas(util.Arrays.asList(brokerId))
  val onLeadershipChange = mock(classOf[LeadershipChangeHandler]).onLeadershipChange _
  var metadataCache: RaftMetadataCache = _
  val offset1 = 1L

  @BeforeEach
  def setUp(): Unit = {
    alterIsrManager = mock(classOf[AlterIsrManager])
    config = KafkaConfig.fromProps({
      val nodeId = brokerId
      val props = TestUtils.createBrokerConfig(nodeId, "")
      props.put(KafkaConfig.ProcessRolesProp, "broker")
      props.put(KafkaConfig.NodeIdProp, nodeId.toString)
      props
    })
    metadataCache = new RaftMetadataCache(config.brokerId)
    quotaManager = QuotaFactory.instantiate(config, metrics, time, "")
    mockDelegate = mock(classOf[RaftReplicaChangeDelegate])
    imageBuilder = MetadataImageBuilder(brokerId, mock(classOf[Logger]), new MetadataImage())
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
      new RaftMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size), alterIsrManager,
      configRepository, None, Some(mockDelegate))
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
  def testDefersChangesImmediately(): Unit = {
    // create topic
    imageBuilder.partitionsBuilder().addUuidMapping(topicName, topicId)
    // create partition deferred at offset 1
    val metadataPartition = MetadataPartition(topicName, topicPartitionRecord, Some(offset1))
    imageBuilder.partitionsBuilder().set(metadataPartition)
    // apply the changes to metadata cache
    metadataCache.image(imageBuilder.build())
    // apply the changes to replica manager
    val rrm = createRaftReplicaManager()
    rrm.handleMetadataRecords(imageBuilder, 1, onLeadershipChange)
    // verify changes were deferred
    val mapCaptor: ArgumentCaptor[mutable.Map[Partition, Boolean]] =
      ArgumentCaptor.forClass(classOf[mutable.Map[Partition, Boolean]])
    verify(mockDelegate).makeDeferred(mapCaptor.capture(), ArgumentMatchers.eq(offset1), ArgumentMatchers.eq(onLeadershipChange))
    val partitionsDeferredMap = mapCaptor.getValue
    assertEquals(1, partitionsDeferredMap.size)
    val (partition, isNew) = partitionsDeferredMap.head
    assertEquals(topicPartition, partition.topicPartition)
    assertTrue(isNew)
    verify(mockDelegate, never()).makeLeaders(any(), any(), any(), anyLong())
    verify(mockDelegate, never()).makeFollowers(any(), any(), any(), any(), any())
  }
}

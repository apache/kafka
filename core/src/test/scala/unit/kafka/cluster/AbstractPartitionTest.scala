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
package kafka.cluster

import kafka.log.LogManager
import kafka.server.MetadataCache
import kafka.server.metadata.MockConfigRepository
import kafka.utils.TestUtils
import kafka.utils.TestUtils.MockAlterPartitionManager
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpoints
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}

import java.io.File
import java.lang.{Long => JLong}
import java.util.{Optional, Properties}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

object AbstractPartitionTest {
  val brokerId = 101
}

class AbstractPartitionTest {

  val brokerId = AbstractPartitionTest.brokerId
  val remoteReplicaId = brokerId + 1
  val topicId : Option[Uuid] = Option(Uuid.randomUuid())
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  var tmpDir: File = _
  var logDir1: File = _
  var logDir2: File = _
  var logManager: LogManager = _
  var alterPartitionManager: MockAlterPartitionManager = _
  var alterPartitionListener: MockAlterPartitionListener = _
  var logConfig: LogConfig = _
  var configRepository: MockConfigRepository = _
  val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
  val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
  var partition: Partition = _

  @BeforeEach
  def setup(): Unit = {
    TestUtils.clearYammerMetrics()

    val logProps = createLogProperties(Map.empty)
    logConfig = new LogConfig(logProps)
    configRepository = MockConfigRepository.forTopic(topicPartition.topic, logProps)

    tmpDir = TestUtils.tempDir()
    logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(Seq(logDir1, logDir2), logConfig, configRepository,
      new CleanerConfig(false), time, interBrokerProtocolVersion, transactionVerificationEnabled = true)
    logManager.startup(Set.empty)

    alterPartitionManager = TestUtils.createAlterIsrManager()
    alterPartitionListener = createIsrChangeListener()
    partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT,
      interBrokerProtocolVersion = interBrokerProtocolVersion,
      localBrokerId = brokerId,
      () => defaultBrokerEpoch(brokerId),
      time,
      alterPartitionListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterPartitionManager)

    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(Optional.empty[JLong])
  }

  protected def interBrokerProtocolVersion: MetadataVersion = MetadataVersion.latestTesting

  def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 512: java.lang.Integer)
    logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 1000: java.lang.Integer)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, 999: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  @AfterEach
  def tearDown(): Unit = {
    if (tmpDir.exists()) {
      logManager.shutdown()
      Utils.delete(tmpDir)
      TestUtils.clearYammerMetrics()
    }
  }

  protected def setupPartitionWithMocks(leaderEpoch: Int,
                                        isLeader: Boolean): Partition = {
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val controllerEpoch = 0
    val replicas = List[Integer](brokerId, remoteReplicaId).asJava
    val isr = replicas

    if (isLeader) {
      assertTrue(partition.makeLeader(new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setPartitionEpoch(1)
        .setReplicas(replicas)
        .setIsNew(true), offsetCheckpoints, None), "Expected become leader transition to succeed")
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
    } else {
      assertTrue(partition.makeFollower(new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(remoteReplicaId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setPartitionEpoch(1)
        .setReplicas(replicas)
        .setIsNew(true), offsetCheckpoints, None), "Expected become follower transition to succeed")
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
      assertEquals(None, partition.leaderLogIfLocal)
    }

    partition
  }

  def defaultBrokerEpoch(brokerId: Int): Long = {
    brokerId + 1000L
  }

  private def createIsrChangeListener(): MockAlterPartitionListener = {
    new MockAlterPartitionListener()
  }

  class MockAlterPartitionListener extends AlterPartitionListener {
    val expands: AtomicInteger = new AtomicInteger(0)
    val shrinks: AtomicInteger = new AtomicInteger(0)
    val failures: AtomicInteger = new AtomicInteger(0)

    override def markIsrExpand(): Unit = expands.incrementAndGet()

    override def markIsrShrink(): Unit = shrinks.incrementAndGet()

    override def markFailed(): Unit = failures.incrementAndGet()


    def reset(): Unit = {
      expands.set(0)
      shrinks.set(0)
      failures.set(0)
    }
  }
}

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

import kafka.api.ApiVersion
import kafka.log.{CleanerConfig, LogConfig, LogManager}
import kafka.server.{Defaults, MetadataCache}
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.server.metadata.MockConfigRepository
import kafka.utils.TestUtils.{MockAlterIsrManager, MockIsrChangeListener}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}

import java.io.File
import java.util.Properties

import scala.jdk.CollectionConverters._

object AbstractPartitionTest {
  val brokerId = 101
}

class AbstractPartitionTest {

  val brokerId = AbstractPartitionTest.brokerId
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  var tmpDir: File = _
  var logDir1: File = _
  var logDir2: File = _
  var logManager: LogManager = _
  var alterIsrManager: MockAlterIsrManager = _
  var isrChangeListener: MockIsrChangeListener = _
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
    logConfig = LogConfig(logProps)
    configRepository = MockConfigRepository.forTopic(topicPartition.topic(), logProps)

    tmpDir = TestUtils.tempDir()
    logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(Seq(logDir1, logDir2), logConfig, configRepository,
      CleanerConfig(enableCleaner = false), time, interBrokerProtocolVersion)
    logManager.startup(Set.empty)

    alterIsrManager = TestUtils.createAlterIsrManager()
    isrChangeListener = TestUtils.createIsrChangeListener()
    partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = interBrokerProtocolVersion,
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterIsrManager)

    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(None)
  }

  protected def interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion

  def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
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
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    if (isLeader) {
      assertTrue(partition.makeLeader(new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(1)
        .setReplicas(replicas)
        .setIsNew(true), offsetCheckpoints, None), "Expected become leader transition to succeed")
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
    } else {
      assertTrue(partition.makeFollower(new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId + 1)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(1)
        .setReplicas(replicas)
        .setIsNew(true), offsetCheckpoints, None), "Expected become follower transition to succeed")
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
      assertEquals(None, partition.leaderLogIfLocal)
    }

    partition
  }
}

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

import java.io.File
import java.util.Properties
import kafka.cluster.Partition
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.TestUtils.MockAlterPartitionManager
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{LogDirFailureChannel, LogOffsetMetadata}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.{atLeastOnce, mock, verify, when}

import scala.collection.{Seq, mutable}

class IsrExpirationTest {

  var topicPartitionIsr: mutable.Map[(String, Int), Seq[Int]] = new mutable.HashMap[(String, Int), Seq[Int]]()
  val replicaLagTimeMaxMs = 100L
  val replicaFetchWaitMaxMs = 100
  val leaderLogEndOffset = 20
  val leaderLogHighWatermark = 20L

  val overridingProps = new Properties()
  overridingProps.put(ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG, replicaLagTimeMaxMs.toString)
  overridingProps.put(ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_CONFIG, replicaFetchWaitMaxMs.toString)
  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  val topic = "foo"

  val time = new MockTime
  val metrics = new Metrics

  var quotaManager: QuotaManagers = _
  var replicaManager: ReplicaManager = _

  var alterIsrManager: MockAlterPartitionManager = _

  @BeforeEach
  def setUp(): Unit = {
    val logManager: LogManager = mock(classOf[LogManager])
    when(logManager.liveLogDirs).thenReturn(Array.empty[File])

    alterIsrManager = TestUtils.createAlterIsrManager()
    quotaManager = QuotaFactory.instantiate(configs.head, metrics, time, "")
    replicaManager = new ReplicaManager(
      metrics = metrics,
      config = configs.head,
      time = time,
      scheduler = null,
      logManager = logManager,
      quotaManagers = quotaManager,
      metadataCache = MetadataCache.zkMetadataCache(configs.head.brokerId, configs.head.interBrokerProtocolVersion),
      logDirFailureChannel = new LogDirFailureChannel(configs.head.logDirs.size),
      alterPartitionManager = alterIsrManager)
  }

  @AfterEach
  def tearDown(): Unit = {
    Option(replicaManager).foreach(_.shutdown(false))
    Option(quotaManager).foreach(_.shutdown())
    metrics.close()
  }

  /*
   * Test the case where a follower is caught up but stops making requests to the leader. Once beyond the configured time limit, it should fall out of ISR
   */
  @Test
  def testIsrExpirationForStuckFollowers(): Unit = {
    val log = logMock

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals(configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds, "All replicas should be in ISR")

    // let the follower catch up to the Leader logEndOffset - 1
    for (replica <- partition0.remoteReplicas)
      replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(leaderLogEndOffset - 1),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset,
        brokerEpoch = 1L)
    var partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set.empty[Int], partition0OSR, "No replica should be out of sync")

    // let some time pass
    time.sleep(150)

    // now follower hasn't pulled any data for > replicaMaxLagTimeMs ms. So it is stuck
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set(configs.last.brokerId), partition0OSR, "Replica 1 should be out of sync")
    verify(log, atLeastOnce()).logEndOffset
  }

  /*
   * Test the case where a follower never makes a fetch request. It should fall out of ISR because it will be declared stuck
   */
  @Test
  def testIsrExpirationIfNoFetchRequestMade(): Unit = {
    val log = logMock

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals(configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds, "All replicas should be in ISR")

    // Let enough time pass for the replica to be considered stuck
    time.sleep(150)

    val partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set(configs.last.brokerId), partition0OSR, "Replica 1 should be out of sync")
    verify(log, atLeastOnce()).logEndOffset
  }

  /*
   * Test the case where a follower continually makes fetch requests but is unable to catch up. It should fall out of the ISR
   * However, any time it makes a request to the LogEndOffset it should be back in the ISR
   */
  @Test
  def testIsrExpirationForSlowFollowers(): Unit = {
    // create leader replica
    val log = logMock
    // add one partition
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals(configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds, "All replicas should be in ISR")
    // Make the remote replica not read to the end of log. It should be not be out of sync for at least 100 ms
    for (replica <- partition0.remoteReplicas)
      replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(leaderLogEndOffset - 2),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset,
        brokerEpoch = 1L)

    // Simulate 2 fetch requests spanning more than 100 ms which do not read to the end of the log.
    // The replicas will no longer be in ISR. We do 2 fetches because we want to simulate the case where the replica is lagging but is not stuck
    var partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set.empty[Int], partition0OSR, "No replica should be out of sync")

    time.sleep(75)

    partition0.remoteReplicas.foreach { r =>
      r.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(leaderLogEndOffset - 1),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset,
        brokerEpoch = 1L)
    }
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set.empty[Int], partition0OSR, "No replica should be out of sync")

    time.sleep(75)

    // The replicas will no longer be in ISR
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set(configs.last.brokerId), partition0OSR, "Replica 1 should be out of sync")

    // Now actually make a fetch to the end of the log. The replicas should be back in ISR
    partition0.remoteReplicas.foreach { r =>
      r.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(leaderLogEndOffset),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset,
        brokerEpoch = 1L)
    }
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set.empty[Int], partition0OSR, "No replica should be out of sync")
    verify(log, atLeastOnce()).logEndOffset
  }

  /*
   * Test the case where a follower has already caught up with same log end offset with the leader. This follower should not be considered as out-of-sync
   */
  @Test
  def testIsrExpirationForCaughtUpFollowers(): Unit = {
    val log = logMock

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals(configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds, "All replicas should be in ISR")

    // let the follower catch up to the Leader logEndOffset
    for (replica <- partition0.remoteReplicas)
      replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(leaderLogEndOffset),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset,
        brokerEpoch = 1L)

    var partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set.empty[Int], partition0OSR, "No replica should be out of sync")

    // let some time pass
    time.sleep(150)

    // even though follower hasn't pulled any data for > replicaMaxLagTimeMs ms, the follower has already caught up. So it is not out-of-sync.
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals(Set.empty[Int], partition0OSR, "No replica should be out of sync")
    verify(log, atLeastOnce()).logEndOffset
  }

  private def getPartitionWithAllReplicasInIsr(topic: String, partitionId: Int, time: Time, config: KafkaConfig,
                                               localLog: UnifiedLog): Partition = {
    val leaderId = config.brokerId
    val tp = new TopicPartition(topic, partitionId)
    val partition = replicaManager.createPartition(tp)
    partition.setLog(localLog, isFutureLog = false)

    partition.updateAssignmentAndIsr(
      replicas = configs.map(_.brokerId),
      isLeader = true,
      isr = configs.map(_.brokerId).toSet,
      addingReplicas = Seq.empty,
      removingReplicas = Seq.empty,
      leaderRecoveryState = LeaderRecoveryState.RECOVERED
    )

    // set lastCaughtUpTime to current time
    for (replica <- partition.remoteReplicas)
      replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata = new LogOffsetMetadata(0L),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = 0L,
        brokerEpoch = 1L)

    // set the leader and its hw and the hw update time
    partition.leaderReplicaIdOpt = Some(leaderId)
    partition
  }

  private def logMock: UnifiedLog = {
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    when(log.logEndOffset).thenReturn(leaderLogEndOffset)
    log
  }
}

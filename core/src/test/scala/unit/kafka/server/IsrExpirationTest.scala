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
import java.util.concurrent.atomic.AtomicBoolean

import kafka.cluster.Partition
import kafka.log.{Log, LogManager}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.TestUtils.MockAlterIsrManager
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.Seq
import scala.collection.mutable.{HashMap, Map}

class IsrExpirationTest {

  var topicPartitionIsr: Map[(String, Int), Seq[Int]] = new HashMap[(String, Int), Seq[Int]]()
  val replicaLagTimeMaxMs = 100L
  val replicaFetchWaitMaxMs = 100
  val leaderLogEndOffset = 20
  val leaderLogHighWatermark = 20L

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.ReplicaLagTimeMaxMsProp, replicaLagTimeMaxMs.toString)
  overridingProps.put(KafkaConfig.ReplicaFetchWaitMaxMsProp, replicaFetchWaitMaxMs.toString)
  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  val topic = "foo"

  val time = new MockTime
  val metrics = new Metrics

  var quotaManager: QuotaManagers = null
  var replicaManager: ReplicaManager = null

  var alterIsrManager: MockAlterIsrManager = _

  @Before
  def setUp(): Unit = {
    val logManager: LogManager = EasyMock.createMock(classOf[LogManager])
    EasyMock.expect(logManager.liveLogDirs).andReturn(Array.empty[File]).anyTimes()
    EasyMock.replay(logManager)

    alterIsrManager = TestUtils.createAlterIsrManager()
    quotaManager = QuotaFactory.instantiate(configs.head, metrics, time, "")
    replicaManager = new ReplicaManager(configs.head, metrics, time, null, null, logManager, new AtomicBoolean(false),
      quotaManager, new BrokerTopicStats, new MetadataCache(configs.head.brokerId),
      new LogDirFailureChannel(configs.head.logDirs.size), alterIsrManager)
  }

  @After
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
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds)

    // let the follower catch up to the Leader logEndOffset - 1
    for (replica <- partition0.remoteReplicas)
      replica.updateFetchState(
        followerFetchOffsetMetadata = LogOffsetMetadata(leaderLogEndOffset - 1),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset)
    var partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR)

    // let some time pass
    time.sleep(150)

    // now follower hasn't pulled any data for > replicaMaxLagTimeMs ms. So it is stuck
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR)
    EasyMock.verify(log)
  }

  /*
   * Test the case where a follower never makes a fetch request. It should fall out of ISR because it will be declared stuck
   */
  @Test
  def testIsrExpirationIfNoFetchRequestMade(): Unit = {
    val log = logMock

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds)

    // Let enough time pass for the replica to be considered stuck
    time.sleep(150)

    val partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR)
    EasyMock.verify(log)
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
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds)
    // Make the remote replica not read to the end of log. It should be not be out of sync for at least 100 ms
    for (replica <- partition0.remoteReplicas)
      replica.updateFetchState(
        followerFetchOffsetMetadata = LogOffsetMetadata(leaderLogEndOffset - 2),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset)

    // Simulate 2 fetch requests spanning more than 100 ms which do not read to the end of the log.
    // The replicas will no longer be in ISR. We do 2 fetches because we want to simulate the case where the replica is lagging but is not stuck
    var partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR)

    time.sleep(75)

    partition0.remoteReplicas.foreach { r =>
      r.updateFetchState(
        followerFetchOffsetMetadata = LogOffsetMetadata(leaderLogEndOffset - 1),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset)
    }
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR)

    time.sleep(75)

    // The replicas will no longer be in ISR
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR)

    // Now actually make a fetch to the end of the log. The replicas should be back in ISR
    partition0.remoteReplicas.foreach { r =>
      r.updateFetchState(
        followerFetchOffsetMetadata = LogOffsetMetadata(leaderLogEndOffset),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset)
    }
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR)

    EasyMock.verify(log)
  }

  /*
   * Test the case where a follower has already caught up with same log end offset with the leader. This follower should not be considered as out-of-sync
   */
  @Test
  def testIsrExpirationForCaughtUpFollowers(): Unit = {
    val log = logMock

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicaIds)

    // let the follower catch up to the Leader logEndOffset
    for (replica <- partition0.remoteReplicas)
      replica.updateFetchState(
        followerFetchOffsetMetadata = LogOffsetMetadata(leaderLogEndOffset),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = leaderLogEndOffset)

    var partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR)

    // let some time pass
    time.sleep(150)

    // even though follower hasn't pulled any data for > replicaMaxLagTimeMs ms, the follower has already caught up. So it is not out-of-sync.
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR)
    EasyMock.verify(log)
  }

  private def getPartitionWithAllReplicasInIsr(topic: String, partitionId: Int, time: Time, config: KafkaConfig,
                                               localLog: Log): Partition = {
    val leaderId = config.brokerId
    val tp = new TopicPartition(topic, partitionId)
    val partition = replicaManager.createPartition(tp)
    partition.setLog(localLog, isFutureLog = false)

    partition.updateAssignmentAndIsr(
      assignment = configs.map(_.brokerId),
      isr = configs.map(_.brokerId).toSet,
      addingReplicas = Seq.empty,
      removingReplicas = Seq.empty
    )

    // set lastCaughtUpTime to current time
    for (replica <- partition.remoteReplicas)
      replica.updateFetchState(
        followerFetchOffsetMetadata = LogOffsetMetadata(0L),
        followerStartOffset = 0L,
        followerFetchTimeMs= time.milliseconds,
        leaderEndOffset = 0L)

    // set the leader and its hw and the hw update time
    partition.leaderReplicaIdOpt = Some(leaderId)
    partition
  }

  private def logMock: Log = {
    val log: Log = EasyMock.createMock(classOf[Log])
    EasyMock.expect(log.dir).andReturn(TestUtils.tempDir()).anyTimes()
    EasyMock.expect(log.logEndOffsetMetadata).andReturn(LogOffsetMetadata(leaderLogEndOffset)).anyTimes()
    EasyMock.expect(log.logEndOffset).andReturn(leaderLogEndOffset).anyTimes()
    EasyMock.expect(log.highWatermark).andReturn(leaderLogHighWatermark).anyTimes()
    EasyMock.replay(log)
    log
  }
}

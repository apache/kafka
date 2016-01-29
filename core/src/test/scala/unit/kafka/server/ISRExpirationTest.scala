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

import java.util.Properties

import org.apache.kafka.common.metrics.Metrics
import org.junit.{Test, Before, After}
import collection.mutable.HashMap
import collection.mutable.Map
import kafka.cluster.{Partition, Replica}
import org.easymock.EasyMock
import kafka.log.Log
import org.junit.Assert._
import kafka.utils._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.message.MessageSet
import org.apache.kafka.common.utils.{MockTime => JMockTime}


class IsrExpirationTest {

  var topicPartitionIsr: Map[(String, Int), Seq[Int]] = new HashMap[(String, Int), Seq[Int]]()
  val replicaLagTimeMaxMs = 100L
  val replicaFetchWaitMaxMs = 100

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.ReplicaLagTimeMaxMsProp, replicaLagTimeMaxMs.toString)
  overridingProps.put(KafkaConfig.ReplicaFetchWaitMaxMsProp, replicaFetchWaitMaxMs.toString)
  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  val topic = "foo"

  val time = new MockTime
  val jTime = new JMockTime
  val metrics = new Metrics

  var replicaManager: ReplicaManager = null

  @Before
  def setUp() {
    replicaManager = new ReplicaManager(configs.head, metrics, time, jTime, null, null, null, new AtomicBoolean(false))
  }

  @After
  def tearDown() {
    replicaManager.shutdown(false)
    metrics.close()
  }

  /*
   * Test the case where a follower is caught up but stops making requests to the leader. Once beyond the configured time limit, it should fall out of ISR
   */
  @Test
  def testIsrExpirationForStuckFollowers() {
    val log = getLogWithLogEndOffset(15L, 2) // set logEndOffset for leader to 15L

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get

    // let the follower catch up to the Leader logEndOffset (15)
    (partition0.assignedReplicas() - leaderReplica).foreach(
      r => r.updateLogReadResult(new LogReadResult(FetchDataInfo(new LogOffsetMetadata(15L),
                                                                 MessageSet.Empty),
                                                   -1L,
                                                   -1,
                                                   true)))
    var partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR.map(_.brokerId))

    // let some time pass
    time.sleep(150)

    // now follower hasn't pulled any data for > replicaMaxLagTimeMs ms. So it is stuck
    partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))
    EasyMock.verify(log)
  }

  /*
   * Test the case where a follower never makes a fetch request. It should fall out of ISR because it will be declared stuck
   */
  @Test
  def testIsrExpirationIfNoFetchRequestMade() {
    val log = getLogWithLogEndOffset(15L, 1) // set logEndOffset for leader to 15L

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get

    // Let enough time pass for the replica to be considered stuck
    time.sleep(150)

    val partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))
    EasyMock.verify(log)
  }

  /*
   * Test the case where a follower continually makes fetch requests but is unable to catch up. It should fall out of the ISR
   * However, any time it makes a request to the LogEndOffset it should be back in the ISR
   */
  @Test
  def testIsrExpirationForSlowFollowers() {
    // create leader replica
    val log = getLogWithLogEndOffset(15L, 4)
    // add one partition
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get

    // Make the remote replica not read to the end of log. It should be not be out of sync for at least 100 ms
    for(replica <- (partition0.assignedReplicas() - leaderReplica))
      replica.updateLogReadResult(new LogReadResult(FetchDataInfo(new LogOffsetMetadata(10L), MessageSet.Empty), -1L, -1, false))

    // Simulate 2 fetch requests spanning more than 100 ms which do not read to the end of the log.
    // The replicas will no longer be in ISR. We do 2 fetches because we want to simulate the case where the replica is lagging but is not stuck
    var partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR.map(_.brokerId))

    time.sleep(75)

    (partition0.assignedReplicas() - leaderReplica).foreach(
      r => r.updateLogReadResult(new LogReadResult(FetchDataInfo(new LogOffsetMetadata(11L), MessageSet.Empty), -1L, -1, false)))
    partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR.map(_.brokerId))

    time.sleep(75)

    // The replicas will no longer be in ISR
    partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))

    // Now actually make a fetch to the end of the log. The replicas should be back in ISR
    (partition0.assignedReplicas() - leaderReplica).foreach(
      r => r.updateLogReadResult(new LogReadResult(FetchDataInfo(new LogOffsetMetadata(15L), MessageSet.Empty), -1L, -1, true)))
    partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR.map(_.brokerId))

    EasyMock.verify(log)
  }

  private def getPartitionWithAllReplicasInIsr(topic: String, partitionId: Int, time: Time, config: KafkaConfig,
                                               localLog: Log): Partition = {
    val leaderId=config.brokerId
    val partition = replicaManager.getOrCreatePartition(topic, partitionId)
    val leaderReplica = new Replica(leaderId, partition, time, 0, Some(localLog))

    val allReplicas = getFollowerReplicas(partition, leaderId, time) :+ leaderReplica
    allReplicas.foreach(r => partition.addReplicaIfNotExists(r))
    // set in sync replicas for this partition to all the assigned replicas
    partition.inSyncReplicas = allReplicas.toSet
    // set the leader and its hw and the hw update time
    partition.leaderReplicaIdOpt = Some(leaderId)
    partition
  }

  private def getLogWithLogEndOffset(logEndOffset: Long, expectedCalls: Int): Log = {
    val log1 = EasyMock.createMock(classOf[kafka.log.Log])
    EasyMock.expect(log1.logEndOffsetMetadata).andReturn(new LogOffsetMetadata(logEndOffset)).times(expectedCalls)
    EasyMock.replay(log1)

    log1
  }

  private def getFollowerReplicas(partition: Partition, leaderId: Int, time: Time): Seq[Replica] = {
    configs.filter(_.brokerId != leaderId).map { config =>
      new Replica(config.brokerId, partition, time)
    }
  }
}

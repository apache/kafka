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

import org.scalatest.junit.JUnit3Suite
import collection.mutable.HashMap
import collection.mutable.Map
import kafka.cluster.{Partition, Replica}
import org.easymock.EasyMock
import kafka.log.Log
import org.junit.Assert._
import kafka.utils._
import java.util.concurrent.atomic.AtomicBoolean

class IsrExpirationTest extends JUnit3Suite {

  var topicPartitionIsr: Map[(String, Int), Seq[Int]] = new HashMap[(String, Int), Seq[Int]]()
  val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
    override val replicaLagTimeMaxMs = 100L
    override val replicaFetchWaitMaxMs = 100
    override val replicaLagMaxMessages = 10L
  })
  val topic = "foo"

  def testIsrExpirationForStuckFollowers() {
    val time = new MockTime
    val log = getLogWithLogEndOffset(15L, 2) // set logEndOffset for leader to 15L

    // create one partition and all replicas
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get

    // let the follower catch up to 10
    (partition0.assignedReplicas() - leaderReplica).foreach(r => r.logEndOffset = new LogOffsetMetadata(10L))
    var partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs, configs.head.replicaLagMaxMessages)
    assertEquals("No replica should be out of sync", Set.empty[Int], partition0OSR.map(_.brokerId))

    // let some time pass
    time.sleep(150)

    // now follower (broker id 1) has caught up to only 10, while the leader is at 15 AND the follower hasn't
    // pulled any data for > replicaMaxLagTimeMs ms. So it is stuck
    partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs, configs.head.replicaLagMaxMessages)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))
    EasyMock.verify(log)
  }

  def testIsrExpirationForSlowFollowers() {
    val time = new MockTime
    // create leader replica
    val log = getLogWithLogEndOffset(15L, 1)
    // add one partition
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get
    // set remote replicas leo to something low, like 4
    (partition0.assignedReplicas() - leaderReplica).foreach(r => r.logEndOffset = new LogOffsetMetadata(4L))

    // now follower (broker id 1) has caught up to only 4, while the leader is at 15. Since the gap it larger than
    // replicaMaxLagBytes, the follower is out of sync.
    val partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs, configs.head.replicaLagMaxMessages)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))

    EasyMock.verify(log)
  }

  private def getPartitionWithAllReplicasInIsr(topic: String, partitionId: Int, time: Time, config: KafkaConfig,
                                               localLog: Log): Partition = {
    val leaderId=config.brokerId
    val replicaManager = new ReplicaManager(config, time, null, null, null, new AtomicBoolean(false))
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
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
import kafka.utils.{Time, MockTime, TestUtils}
import org.junit.Assert._
import org.I0Itec.zkclient.ZkClient

class ISRExpirationTest extends JUnit3Suite {

  var topicPartitionISR: Map[(String, Int), Seq[Int]] = new HashMap[(String, Int), Seq[Int]]()
  val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
    override val keepInSyncTimeMs = 100L
    override val keepInSyncBytes = 10L
  })
  val topic = "foo"

  def testISRExpirationForStuckFollowers() {
    val time = new MockTime
    // create leader replica
    val log = EasyMock.createMock(classOf[kafka.log.Log])
    EasyMock.expect(log.logEndOffset).andReturn(5L).times(12)
    EasyMock.expect(log.setHW(5L)).times(1)
    EasyMock.replay(log)

    // add one partition
    val partition0 = getPartitionWithAllReplicasInISR(topic, 0, time, configs.head.brokerId, log, 5L)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get
    // set remote replicas leo to something low, like 2
    (partition0.assignedReplicas() - leaderReplica).foreach(partition0.updateReplicaLEO(_, 2))

    time.sleep(150)
    leaderReplica.logEndOffsetUpdateTime(Some(time.milliseconds))

    var partition0OSR = partition0.getOutOfSyncReplicas(configs.head.keepInSyncTimeMs, configs.head.keepInSyncBytes)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))

    // add all replicas back to the ISR
    partition0.inSyncReplicas ++= partition0.assignedReplicas()
    assertEquals("Replica 1 should be in sync", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))

    leaderReplica.logEndOffsetUpdateTime(Some(time.milliseconds))
    // let the follower catch up only upto 3
    (partition0.assignedReplicas() - leaderReplica).foreach(partition0.updateReplicaLEO(_, 3))
    time.sleep(150)
    // now follower broker id 1 has caught upto only 3, while the leader is at 5 AND follower broker id 1 hasn't
    // pulled any data for > keepInSyncTimeMs ms. So it is stuck
    partition0OSR = partition0.getOutOfSyncReplicas(configs.head.keepInSyncTimeMs, configs.head.keepInSyncBytes)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))
    EasyMock.verify(log)
  }

  def testISRExpirationForSlowFollowers() {
    val time = new MockTime
    // create leader replica
    val log = getLogWithHW(15L)
    // add one partition
    val partition0 = getPartitionWithAllReplicasInISR(topic, 0, time, configs.head.brokerId, log, 15L)
    assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId))
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get
    // set remote replicas leo to something low, like 4
    (partition0.assignedReplicas() - leaderReplica).foreach(partition0.updateReplicaLEO(_, 4))

    time.sleep(150)
    leaderReplica.logEndOffsetUpdateTime(Some(time.milliseconds))
    time.sleep(10)
    (partition0.inSyncReplicas - leaderReplica).foreach(r => r.logEndOffsetUpdateTime(Some(time.milliseconds)))

    val partition0OSR = partition0.getOutOfSyncReplicas(configs.head.keepInSyncTimeMs, configs.head.keepInSyncBytes)
    assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))

    EasyMock.verify(log)
  }

  def testISRExpirationForMultiplePartitions() {
    val time = new MockTime
    // mock zkclient
    val zkClient = EasyMock.createMock(classOf[ZkClient])
    EasyMock.replay(zkClient)
    // create replica manager
    val replicaManager = new ReplicaManager(configs.head, time, zkClient)
    try {
      val partition0 = replicaManager.getOrCreatePartition(topic, 0, configs.map(_.brokerId).toSet)
      // create leader log
      val log0 = getLogWithHW(5L)

      // create leader and follower replicas
      val leaderReplicaPartition0 = replicaManager.addLocalReplica(topic, 0, log0, configs.map(_.brokerId).toSet)
      val followerReplicaPartition0 = replicaManager.addRemoteReplica(topic, 0, configs.last.brokerId, partition0)

      partition0.inSyncReplicas = Set(followerReplicaPartition0, leaderReplicaPartition0)
      // set the leader and its hw and the hw update time
      partition0.leaderId(Some(configs.head.brokerId))
      partition0.leaderHW(Some(5L))

      // set the leo for non-leader replicas to something low
      (partition0.assignedReplicas() - leaderReplicaPartition0).foreach(r => partition0.updateReplicaLEO(r, 2))

      val log1 = getLogWithHW(15L)
      // create leader and follower replicas for partition 1
      val partition1 = replicaManager.getOrCreatePartition(topic, 1, configs.map(_.brokerId).toSet)
      val leaderReplicaPartition1 = replicaManager.addLocalReplica(topic, 1, log1, configs.map(_.brokerId).toSet)
      val followerReplicaPartition1 = replicaManager.addRemoteReplica(topic, 1, configs.last.brokerId, partition0)

      partition1.inSyncReplicas = Set(followerReplicaPartition1, leaderReplicaPartition1)
      // set the leader and its hw and the hw update time
      partition1.leaderId(Some(configs.head.brokerId))
      partition1.leaderHW(Some(15L))

      // set the leo for non-leader replicas to something low
      (partition1.assignedReplicas() - leaderReplicaPartition1).foreach(r => partition1.updateReplicaLEO(r, 4))

      time.sleep(150)
      leaderReplicaPartition0.logEndOffsetUpdateTime(Some(time.milliseconds))
      leaderReplicaPartition1.logEndOffsetUpdateTime(Some(time.milliseconds))
      time.sleep(10)
      (partition1.inSyncReplicas - leaderReplicaPartition1).foreach(r => r.logEndOffsetUpdateTime(Some(time.milliseconds)))

      val partition0OSR = partition0.getOutOfSyncReplicas(configs.head.keepInSyncTimeMs, configs.head.keepInSyncBytes)
      assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId))

      val partition1OSR = partition1.getOutOfSyncReplicas(configs.head.keepInSyncTimeMs, configs.head.keepInSyncBytes)
      assertEquals("Replica 0 should be out of sync", Set(configs.last.brokerId), partition1OSR.map(_.brokerId))

      EasyMock.verify(log0)
      EasyMock.verify(log1)
    }catch {
      case e => e.printStackTrace()
    }finally {
      replicaManager.close()
    }
  }

  private def getPartitionWithAllReplicasInISR(topic: String, partitionId: Int, time: Time, leaderId: Int,
                                               localLog: Log, leaderHW: Long): Partition = {
    val partition = new Partition(topic, partitionId, time)
    val leaderReplica = new Replica(leaderId, partition, topic, Some(localLog))

    val allReplicas = getFollowerReplicas(partition, leaderId) :+ leaderReplica
    partition.assignedReplicas(Some(allReplicas.toSet))
    // set in sync replicas for this partition to all the assigned replicas
    partition.inSyncReplicas = allReplicas.toSet
    // set the leader and its hw and the hw update time
    partition.leaderId(Some(leaderId))
    partition.leaderHW(Some(leaderHW))
    partition
  }

  private def getLogWithHW(hw: Long): Log = {
    val log1 = EasyMock.createMock(classOf[kafka.log.Log])
    EasyMock.expect(log1.logEndOffset).andReturn(hw).times(6)
    EasyMock.expect(log1.setHW(hw)).times(1)
    EasyMock.replay(log1)

    log1
  }

  private def getFollowerReplicas(partition: Partition, leaderId: Int): Seq[Replica] = {
    configs.filter(_.brokerId != leaderId).map { config =>
      new Replica(config.brokerId, partition, topic)
    }
  }
}
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

import kafka.log.Log
import org.I0Itec.zkclient.ZkClient
import org.scalatest.junit.JUnit3Suite
import org.easymock.EasyMock
import org.junit.Assert._
import kafka.utils.{KafkaScheduler, TestUtils, MockTime}
import kafka.common.KafkaException

class HighwatermarkPersistenceTest extends JUnit3Suite {

  val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
    override val defaultFlushIntervalMs = 100
  })
  val topic = "foo"

  def testHighWatermarkPersistenceSinglePartition() {
    // mock zkclient
    val zkClient = EasyMock.createMock(classOf[ZkClient])
    EasyMock.replay(zkClient)
    // create kafka scheduler
    val scheduler = new KafkaScheduler(2)
    scheduler.startUp
    // create replica manager
    val replicaManager = new ReplicaManager(configs.head, new MockTime(), zkClient, scheduler)
    replicaManager.startup()
    // sleep until flush ms
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    var fooPartition0Hw = replicaManager.readCheckpointedHighWatermark(topic, 0)
    assertEquals(0L, fooPartition0Hw)
    val partition0 = replicaManager.getOrCreatePartition(topic, 0, configs.map(_.brokerId).toSet)
    // create leader log
    val log0 = getMockLog
    // create leader and follower replicas
    val leaderReplicaPartition0 = replicaManager.addLocalReplica(topic, 0, log0, configs.map(_.brokerId).toSet)
    val followerReplicaPartition0 = replicaManager.addRemoteReplica(topic, 0, configs.last.brokerId, partition0)
    // sleep until flush ms
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    fooPartition0Hw = replicaManager.readCheckpointedHighWatermark(topic, 0)
    assertEquals(leaderReplicaPartition0.highWatermark(), fooPartition0Hw)
    try {
      followerReplicaPartition0.highWatermark()
      fail("Should fail with IllegalStateException")
    }catch {
      case e: KafkaException => // this is ok
    }
    // set the leader
    partition0.leaderId(Some(leaderReplicaPartition0.brokerId))
    // set the highwatermark for local replica
    partition0.leaderHW(Some(5L))
    // sleep until flush interval
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    fooPartition0Hw = replicaManager.readCheckpointedHighWatermark(topic, 0)
    assertEquals(leaderReplicaPartition0.highWatermark(), fooPartition0Hw)
    EasyMock.verify(zkClient)
    EasyMock.verify(log0)
  }

  def testHighWatermarkPersistenceMultiplePartitions() {
    val topic1 = "foo1"
    val topic2 = "foo2"
    // mock zkclient
    val zkClient = EasyMock.createMock(classOf[ZkClient])
    EasyMock.replay(zkClient)
    // create kafka scheduler
    val scheduler = new KafkaScheduler(2)
    scheduler.startUp
    // create replica manager
    val replicaManager = new ReplicaManager(configs.head, new MockTime(), zkClient, scheduler)
    replicaManager.startup()
    // sleep until flush ms
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    var topic1Partition0Hw = replicaManager.readCheckpointedHighWatermark(topic1, 0)
    assertEquals(0L, topic1Partition0Hw)
    val topic1Partition0 = replicaManager.getOrCreatePartition(topic1, 0, configs.map(_.brokerId).toSet)
    // create leader log
    val topic1Log0 = getMockLog
    // create leader and follower replicas
    val leaderReplicaTopic1Partition0 = replicaManager.addLocalReplica(topic1, 0, topic1Log0, configs.map(_.brokerId).toSet)
    // sleep until flush ms
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    topic1Partition0Hw = replicaManager.readCheckpointedHighWatermark(topic1, 0)
    assertEquals(leaderReplicaTopic1Partition0.highWatermark(), topic1Partition0Hw)
    // set the leader
    topic1Partition0.leaderId(Some(leaderReplicaTopic1Partition0.brokerId))
    // set the highwatermark for local replica
    topic1Partition0.leaderHW(Some(5L))
    // sleep until flush interval
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    topic1Partition0Hw = replicaManager.readCheckpointedHighWatermark(topic1, 0)
    assertEquals(5L, leaderReplicaTopic1Partition0.highWatermark())
    assertEquals(5L, topic1Partition0Hw)
    // add another partition and set highwatermark
    val topic2Partition0 = replicaManager.getOrCreatePartition(topic2, 0, configs.map(_.brokerId).toSet)
    // create leader log
    val topic2Log0 = getMockLog
    // create leader and follower replicas
    val leaderReplicaTopic2Partition0 = replicaManager.addLocalReplica(topic2, 0, topic2Log0, configs.map(_.brokerId).toSet)
    // sleep until flush ms
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    var topic2Partition0Hw = replicaManager.readCheckpointedHighWatermark(topic2, 0)
    assertEquals(leaderReplicaTopic2Partition0.highWatermark(), topic2Partition0Hw)
    // set the leader
    topic2Partition0.leaderId(Some(leaderReplicaTopic2Partition0.brokerId))
    // set the highwatermark for local replica
    topic2Partition0.leaderHW(Some(15L))
    assertEquals(15L, leaderReplicaTopic2Partition0.highWatermark())
    // change the highwatermark for topic1
    topic1Partition0.leaderHW(Some(10L))
    assertEquals(10L, leaderReplicaTopic1Partition0.highWatermark())
    // sleep until flush interval
    Thread.sleep(configs.head.defaultFlushIntervalMs)
    // verify checkpointed hw for topic 2
    topic2Partition0Hw = replicaManager.readCheckpointedHighWatermark(topic2, 0)
    assertEquals(15L, topic2Partition0Hw)
    // verify checkpointed hw for topic 1
    topic1Partition0Hw = replicaManager.readCheckpointedHighWatermark(topic1, 0)
    assertEquals(10L, topic1Partition0Hw)
    EasyMock.verify(zkClient)
    EasyMock.verify(topic1Log0)
    EasyMock.verify(topic2Log0)
  }

  private def getMockLog: Log = {
    val log = EasyMock.createMock(classOf[kafka.log.Log])
    EasyMock.replay(log)
    log
  }
}
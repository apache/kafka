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

package kafka.utils

import kafka.controller.{ZkLeaderAndIsrUpdateBatch, LeaderIsrAndControllerEpoch}
import kafka.server.{ReplicaFetcherManager, KafkaConfig}
import kafka.api.LeaderAndIsr
import kafka.zk.ZooKeeperTestHarness
import kafka.common.TopicAndPartition
import org.junit.Assert._
import org.junit.{Before, Test}
import org.easymock.EasyMock


class ReplicationUtilsTest extends ZooKeeperTestHarness {
  val topic = "my-topic-test"
  val partitionId = 0
  val brokerId = 1
  val leaderEpoch = 1
  val controllerEpoch = 1
  val zkVersion = 1
  val topicPath = "/brokers/topics/my-topic-test/partitions/0/state"
  val topicData = Json.encode(Map("controller_epoch" -> 1, "leader" -> 1,
    "versions" -> 1, "leader_epoch" -> 1,"isr" -> List(1,2)))
  val topicDataVersionMismatch = Json.encode(Map("controller_epoch" -> 1, "leader" -> 1,
    "versions" -> 2, "leader_epoch" -> 1,"isr" -> List(1,2)))
  val topicDataMismatch = Json.encode(Map("controller_epoch" -> 1, "leader" -> 1,
    "versions" -> 2, "leader_epoch" -> 2,"isr" -> List(1,2)))

  val topicDataLeaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(1,leaderEpoch,List(1,2),0), controllerEpoch)

  @Before
  override def setUp() {
    super.setUp()
    zkUtils.createPersistentPath(topicPath, topicData)
  }

  @Test
  def testUpdateLeaderAndIsr() {
    val configs = TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps)
    val log = EasyMock.createMock(classOf[kafka.log.Log])
    EasyMock.expect(log.logEndOffset).andReturn(20).anyTimes()
    EasyMock.expect(log)
    EasyMock.replay(log)

    val logManager = EasyMock.createMock(classOf[kafka.log.LogManager])
    EasyMock.expect(logManager.getLog(TopicAndPartition(topic, partitionId))).andReturn(Some(log)).anyTimes()
    EasyMock.replay(logManager)

    val replicaManager = EasyMock.createMock(classOf[kafka.server.ReplicaManager])
    EasyMock.expect(replicaManager.config).andReturn(configs.head)
    EasyMock.expect(replicaManager.logManager).andReturn(logManager)
    EasyMock.expect(replicaManager.replicaFetcherManager).andReturn(EasyMock.createMock(classOf[ReplicaFetcherManager]))
    EasyMock.expect(replicaManager.zkUtils).andReturn(zkUtils)
    EasyMock.replay(replicaManager)

    zkUtils.makeSurePersistentPathExists(ZkUtils.IsrChangeNotificationPath)

    val replicas = List(0,1)

    // regular update
    val newLeaderAndIsr1 = new LeaderAndIsr(brokerId, leaderEpoch, replicas, 0)
    val (updateSucceeded1,newZkVersion1) = ReplicationUtils.updateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr1, controllerEpoch, 0)
    assertTrue(updateSucceeded1)
    assertEquals(newZkVersion1, 1)

    // mismatched zkVersion with the same data
    val newLeaderAndIsr2 = new LeaderAndIsr(brokerId, leaderEpoch, replicas, zkVersion + 1)
    val (updateSucceeded2,newZkVersion2) = ReplicationUtils.updateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr2, controllerEpoch, zkVersion + 1)
    assertTrue(updateSucceeded2)
    // returns true with existing zkVersion
    assertEquals(newZkVersion2,1)

    // mismatched zkVersion and leaderEpoch
    val newLeaderAndIsr3 = new LeaderAndIsr(brokerId, leaderEpoch + 1, replicas, zkVersion + 1)
    val (updateSucceeded3,newZkVersion3) = ReplicationUtils.updateLeaderAndIsr(zkUtils,
      "my-topic-test", partitionId, newLeaderAndIsr3, controllerEpoch, zkVersion + 1)
    assertFalse(updateSucceeded3)
    assertEquals(newZkVersion3,-1)

    //test async update leader and isr
    val topicAndPartition = TopicAndPartition("my-topic-test", partitionId)
    val leaderAndIsrUpdateBatch = new ZkLeaderAndIsrUpdateBatch(zkUtils)

    // Test regular update
    var updateSucceeded4 = false
    var newZkVersion4 = -1
    val newLeaderAndIsr4 = new LeaderAndIsr(brokerId, leaderEpoch + 1, replicas, zkVersion = 2)
    leaderAndIsrUpdateBatch.addLeaderAndIsrUpdate(topicAndPartition, newLeaderAndIsr4, expectZkVersion = 1,
      (_, updateResult) => {
        updateSucceeded4 = true
        newZkVersion4 = updateResult.newZkVersion
      }
    )
    leaderAndIsrUpdateBatch.writeLeaderAndIsrUpdateToZk(controllerEpoch)
    assertTrue(updateSucceeded4)
    assertEquals(2, newZkVersion4)
    assertEquals(0, leaderAndIsrUpdateBatch.incompleteUpdates)

    // Test mismatched zkversion with same data
    var updateSucceeded5 = false
    var newZkVersion5 = -1
    val newLeaderAndIsr5 = new LeaderAndIsr(brokerId, leaderEpoch + 1, replicas, zkVersion = 3)
    leaderAndIsrUpdateBatch.addLeaderAndIsrUpdate(topicAndPartition, newLeaderAndIsr5, expectZkVersion = 3,
      (_, updateResult) => {
        updateSucceeded5 = true
        newZkVersion5 = updateResult.newZkVersion
      }
    )
    leaderAndIsrUpdateBatch.writeLeaderAndIsrUpdateToZk(controllerEpoch)
    assertFalse(updateSucceeded5)
    assertEquals(-1, newZkVersion5)
    assertEquals(1, leaderAndIsrUpdateBatch.incompleteUpdates)
    assertTrue(leaderAndIsrUpdateBatch.containsPartition(topicAndPartition))

    // Test mismatched zkversion with different data
    var updateSucceeded6 = false
    var newZkVersion6 = -1
    val newLeaderAndIsr6 = new LeaderAndIsr(brokerId, leaderEpoch + 2, replicas, zkVersion = 3)
    leaderAndIsrUpdateBatch.addLeaderAndIsrUpdate(topicAndPartition, newLeaderAndIsr1, expectZkVersion = 3,
      (_, updateResult) => {
        updateSucceeded6 = true
        newZkVersion6 = updateResult.newZkVersion
      }
    )
    leaderAndIsrUpdateBatch.writeLeaderAndIsrUpdateToZk(controllerEpoch)
    assertFalse(updateSucceeded6)
    assertEquals(-1, newZkVersion6)
    assertEquals(1, leaderAndIsrUpdateBatch.incompleteUpdates)
    assertTrue(leaderAndIsrUpdateBatch.containsPartition(topicAndPartition))
  }

  @Test
  def testGetLeaderIsrAndEpochForPartition() {
    val leaderIsrAndControllerEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partitionId)
    assertEquals(topicDataLeaderIsrAndControllerEpoch, leaderIsrAndControllerEpoch.get)
    assertEquals(None, ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partitionId + 1))
  }

}

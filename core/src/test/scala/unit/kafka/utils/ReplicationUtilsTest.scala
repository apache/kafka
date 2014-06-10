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

import kafka.cluster.{Replica, Partition}
import kafka.server.{ReplicaFetcherManager, KafkaConfig}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import kafka.log.Log
import kafka.common.TopicAndPartition
import org.scalatest.junit.JUnit3Suite
import org.junit.Assert._
import org.junit.Test
import org.I0Itec.zkclient.ZkClient
import org.easymock.EasyMock
import org.apache.log4j.Logger


class ReplicationUtilsTest extends JUnit3Suite with ZooKeeperTestHarness {
  private val logger = Logger.getLogger(classOf[UtilsTest])
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
    "versions" -> 2, "leader_epoch" -> 1,"isr" -> List(2,1)))
  val topicDataMismatch = Json.encode(Map("controller_epoch" -> 1, "leader" -> 1,
    "versions" -> 2, "leader_epoch" -> 2,"isr" -> List(1,2)))


  override def setUp() {
    super.setUp()
    ZkUtils.createPersistentPath(zkClient,topicPath,topicData)
  }

  def testCheckLeaderAndIsrZkData() {
    //mismatched zkVersion with the same data
    val(dataMatched1,newZkVersion1) = ReplicationUtils.checkLeaderAndIsrZkData(zkClient,topicPath,topicDataVersionMismatch,1)
    assertTrue(dataMatched1)
    assertEquals(newZkVersion1,0)

    //mismatched zkVersion and leaderEpoch
    val(dataMatched2,newZkVersion2) = ReplicationUtils.checkLeaderAndIsrZkData(zkClient,topicPath,topicDataMismatch,1)
    assertFalse(dataMatched2)
    assertEquals(newZkVersion2,-1)
  }

 def testUpdateIsr() {
   val configs = TestUtils.createBrokerConfigs(1).map(new KafkaConfig(_))

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
   EasyMock.expect(replicaManager.zkClient).andReturn(zkClient)
   EasyMock.replay(replicaManager)

   val partition = new Partition(topic,0,1,new MockTime,replicaManager)
   val replicas = Set(new Replica(1,partition),new Replica(2,partition))

   // regular update
   val (updateSucceeded1,newZkVersion1) = ReplicationUtils.updateIsr(zkClient,
     "my-topic-test", partitionId, brokerId, leaderEpoch, controllerEpoch, 0, replicas)
   assertTrue(updateSucceeded1)
   assertEquals(newZkVersion1,1)

   // mismatched zkVersion with the same data
   val (updateSucceeded2,newZkVersion2) = ReplicationUtils.updateIsr(zkClient,
     "my-topic-test", partitionId, brokerId, leaderEpoch, controllerEpoch, zkVersion + 1, replicas)
   assertTrue(updateSucceeded2)
   // returns true with existing zkVersion
   assertEquals(newZkVersion2,1)

   // mismatched zkVersion and leaderEpoch
   val (updateSucceeded3,newZkVersion3) = ReplicationUtils.updateIsr(zkClient,
     "my-topic-test", partitionId, brokerId, leaderEpoch + 1, controllerEpoch, zkVersion + 1, replicas)
   assertFalse(updateSucceeded3)
   assertEquals(newZkVersion3,-1)
 }

}

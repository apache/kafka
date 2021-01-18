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

import kafka.server.{KafkaConfig, ReplicaFetcherManager, ReplicaManager}
import kafka.api.LeaderAndIsr
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.log.{Log, LogManager}
import kafka.zk._
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.easymock.EasyMock

class ReplicationUtilsTest extends ZooKeeperTestHarness {
  private val zkVersion = 1
  private val topic = "my-topic-test"
  private val partition = 0
  private val leader = 1
  private val leaderEpoch = 1
  private val controllerEpoch = 1
  private val isr = List(1, 2)

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    zkClient.makeSurePersistentPathExists(TopicZNode.path(topic))
    val topicPartition = new TopicPartition(topic, partition)
    val leaderAndIsr = LeaderAndIsr(leader, leaderEpoch, isr, 1)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    zkClient.createTopicPartitionStatesRaw(Map(topicPartition -> leaderIsrAndControllerEpoch), ZkVersion.MatchAnyVersion)
  }

  @Test
  def testUpdateLeaderAndIsr(): Unit = {
    val configs = TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps)
    val log: Log = EasyMock.createMock(classOf[Log])
    EasyMock.expect(log.logEndOffset).andReturn(20).anyTimes()
    EasyMock.expect(log)
    EasyMock.replay(log)

    val logManager: LogManager = EasyMock.createMock(classOf[LogManager])
    EasyMock.expect(logManager.getLog(new TopicPartition(topic, partition), false)).andReturn(Some(log)).anyTimes()
    EasyMock.replay(logManager)

    val replicaManager: ReplicaManager = EasyMock.createMock(classOf[ReplicaManager])
    EasyMock.expect(replicaManager.config).andReturn(configs.head)
    EasyMock.expect(replicaManager.logManager).andReturn(logManager)
    EasyMock.expect(replicaManager.replicaFetcherManager).andReturn(EasyMock.createMock(classOf[ReplicaFetcherManager]))
    EasyMock.expect(replicaManager.zkClient).andReturn(zkClient)
    EasyMock.replay(replicaManager)

    zkClient.makeSurePersistentPathExists(IsrChangeNotificationZNode.path)

    val replicas = List(0, 1)

    // regular update
    val newLeaderAndIsr1 = new LeaderAndIsr(leader, leaderEpoch, replicas, 0)
    val (updateSucceeded1, newZkVersion1) = ReplicationUtils.updateLeaderAndIsr(zkClient,
      new TopicPartition(topic, partition), newLeaderAndIsr1, controllerEpoch)
    assertTrue(updateSucceeded1)
    assertEquals(newZkVersion1, 1)

    // mismatched zkVersion with the same data
    val newLeaderAndIsr2 = new LeaderAndIsr(leader, leaderEpoch, replicas, zkVersion + 1)
    val (updateSucceeded2, newZkVersion2) = ReplicationUtils.updateLeaderAndIsr(zkClient,
      new TopicPartition(topic, partition), newLeaderAndIsr2, controllerEpoch)
    assertTrue(updateSucceeded2)
    // returns true with existing zkVersion
    assertEquals(newZkVersion2, 1)

    // mismatched zkVersion and leaderEpoch
    val newLeaderAndIsr3 = new LeaderAndIsr(leader, leaderEpoch + 1, replicas, zkVersion + 1)
    val (updateSucceeded3, newZkVersion3) = ReplicationUtils.updateLeaderAndIsr(zkClient,
      new TopicPartition(topic, partition), newLeaderAndIsr3, controllerEpoch)
    assertFalse(updateSucceeded3)
    assertEquals(newZkVersion3, -1)
  }

}

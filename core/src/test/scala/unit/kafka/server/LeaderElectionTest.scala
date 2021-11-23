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

import java.util.Collections

import org.apache.kafka.common.{TopicPartition, Uuid}

import scala.jdk.CollectionConverters._
import kafka.api.LeaderAndIsr
import org.apache.kafka.common.requests._
import org.junit.jupiter.api.Assertions._
import kafka.utils.TestUtils
import kafka.cluster.Broker
import kafka.controller.{ControllerChannelManager, ControllerContext, StateChangeLogger}
import kafka.utils.TestUtils._
import kafka.server.QuorumTestHarness
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

class LeaderElectionTest extends QuorumTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  var staleControllerEpochDetected = false

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    val configProps1 = TestUtils.createBrokerConfig(brokerId1, zkConnect, enableControlledShutdown = false)
    val configProps2 = TestUtils.createBrokerConfig(brokerId2, zkConnect, enableControlledShutdown = false)

    configProps1.put("unclean.leader.election.enable", "true")
    configProps2.put("unclean.leader.election.enable", "true")

    // start both servers
    val server1 = TestUtils.createServer(KafkaConfig.fromProps(configProps1))
    val server2 = TestUtils.createServer(KafkaConfig.fromProps(configProps2))
    servers ++= List(server1, server2)
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testLeaderElectionAndEpoch(): Unit = {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)

    // create topic with 1 partition, 2 replicas, one on each broker
    val leader1 = createTopic(zkClient, topic, partitionReplicaAssignment = Map(0 -> Seq(0, 1)), servers = servers)(0)

    val leaderEpoch1 = zkClient.getEpochForPartition(new TopicPartition(topic, partitionId)).get
    assertTrue(leader1 == 0, "Leader should be broker 0")
    assertEquals(0, leaderEpoch1, "First epoch value should be 0")

    // kill the server hosting the preferred replica/initial leader
    servers.head.shutdown()
    // check if leader moves to the other server
    val leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = Some(leader1))
    val leaderEpoch2 = zkClient.getEpochForPartition(new TopicPartition(topic, partitionId)).get
    assertEquals(1, leader2, "Leader must move to broker 1")
    // new leaderEpoch will be leaderEpoch1+2, one increment during ReplicaStateMachine.startup()-> handleStateChanges
    // for offline replica and one increment during PartitionStateMachine.triggerOnlinePartitionStateChange()
    assertEquals(leaderEpoch1 + 2 , leaderEpoch2, "Second epoch value should be %d".format(leaderEpoch1 + 2))

    servers.head.startup()
    //make sure second server joins the ISR
    TestUtils.waitUntilTrue(() => {
      servers.last.metadataCache.getPartitionInfo(topic, partitionId).exists(_.isr.size == 2)
    }, "Inconsistent metadata after second broker startup")

    servers.last.shutdown()

    Thread.sleep(zookeeper.tickTime)
    val leader3 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = Some(leader2))
    val leaderEpoch3 = zkClient.getEpochForPartition(new TopicPartition(topic, partitionId)).get
    assertEquals(0, leader3, "Leader must return to 0")
    assertEquals(leaderEpoch2 + 2 , leaderEpoch3, "Second epoch value should be %d".format(leaderEpoch2 + 2))
  }

  @Test
  def testLeaderElectionWithStaleControllerEpoch(): Unit = {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 2 replicas, one on each broker
    val leader1 = createTopic(zkClient, topic, partitionReplicaAssignment = Map(0 -> Seq(0, 1)), servers = servers)(0)

    val leaderEpoch1 = zkClient.getEpochForPartition(new TopicPartition(topic, partitionId)).get
    debug("leader Epoch: " + leaderEpoch1)
    debug("Leader is elected to be: %s".format(leader1))
    // NOTE: this is to avoid transient test failures
    assertTrue(leader1 == 0 || leader1 == 1, "Leader could be broker 0 or broker 1")
    assertEquals(0, leaderEpoch1, "First epoch value should be 0")

    // start another controller
    val controllerId = 2

    val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokerAndEpochs = servers.map(s =>
      (new Broker(s.config.brokerId, "localhost", TestUtils.boundPort(s), listenerName, securityProtocol),
        s.kafkaController.brokerEpoch)).toMap
    val nodes = brokerAndEpochs.keys.map(_.node(listenerName))

    val controllerContext = new ControllerContext
    controllerContext.setLiveBrokers(brokerAndEpochs)
    val metrics = new Metrics
    val controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig, Time.SYSTEM,
      metrics, new StateChangeLogger(controllerId, inControllerContext = true, None))
    controllerChannelManager.startup()
    try {
      val staleControllerEpoch = 0
      val partitionStates = Seq(
        new LeaderAndIsrPartitionState()
          .setTopicName(topic)
          .setPartitionIndex(partitionId)
          .setControllerEpoch(2)
          .setLeader(brokerId2)
          .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch)
          .setIsr(Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava)
          .setZkVersion(LeaderAndIsr.initialZKVersion)
          .setReplicas(Seq(0, 1).map(Integer.valueOf).asJava)
          .setIsNew(false)
      )
      val requestBuilder = new LeaderAndIsrRequest.Builder(
        ApiKeys.LEADER_AND_ISR.latestVersion, controllerId, staleControllerEpoch,
        servers(brokerId2).kafkaController.brokerEpoch, partitionStates.asJava,
        Collections.singletonMap(topic, Uuid.randomUuid()), nodes.toSet.asJava)

      controllerChannelManager.sendRequest(brokerId2, requestBuilder, staleControllerEpochCallback)
      TestUtils.waitUntilTrue(() => staleControllerEpochDetected, "Controller epoch should be stale")
      assertTrue(staleControllerEpochDetected, "Stale controller epoch not detected by the broker")
    } finally {
      controllerChannelManager.shutdown()
      metrics.close()
    }
  }

  private def staleControllerEpochCallback(response: AbstractResponse): Unit = {
    val leaderAndIsrResponse = response.asInstanceOf[LeaderAndIsrResponse]
    staleControllerEpochDetected = leaderAndIsrResponse.error match {
      case Errors.STALE_CONTROLLER_EPOCH => true
      case _ => false
    }
  }
}

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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.LeaderAndIsrRequest.PartitionState

import scala.collection.JavaConverters._
import kafka.api.{PartitionStateInfo, LeaderAndIsr}
import org.apache.kafka.common.requests.{LeaderAndIsrResponse, LeaderAndIsrRequest, AbstractRequestResponse}
import org.junit.Assert._
import kafka.utils.{TestUtils, ZkUtils, CoreUtils}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.controller.{ControllerChannelManager, ControllerContext, LeaderIsrAndControllerEpoch}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.utils.SystemTime
import org.junit.{Test, After, Before}

class LeaderElectionTest extends ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  var staleControllerEpochDetected = false

  @Before
  override def setUp() {
    super.setUp()

    val configProps1 = TestUtils.createBrokerConfig(brokerId1, zkConnect, enableControlledShutdown = false)
    val configProps2 = TestUtils.createBrokerConfig(brokerId2, zkConnect, enableControlledShutdown = false)

    // start both servers
    val server1 = TestUtils.createServer(KafkaConfig.fromProps(configProps1))
    val server2 = TestUtils.createServer(KafkaConfig.fromProps(configProps2))
    servers ++= List(server1, server2)
  }

  @After
  override def tearDown() {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.rm(server.config.logDirs))
    super.tearDown()
  }

  @Test
  def testLeaderElectionAndEpoch {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 2 replicas, one on each broker
    val leader1 = createTopic(zkClient, topic, partitionReplicaAssignment = Map(0 -> Seq(0, 1)), servers = servers)(0)

    val leaderEpoch1 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId)
    debug("leader Epoc: " + leaderEpoch1)
    debug("Leader is elected to be: %s".format(leader1.getOrElse(-1)))
    assertTrue("Leader should get elected", leader1.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader1.getOrElse(-1) == 0) || (leader1.getOrElse(-1) == 1))
    assertEquals("First epoch value should be 0", 0, leaderEpoch1)

    // kill the server hosting the preferred replica
    servers.last.shutdown()
    // check if leader moves to the other server
    val leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId,
                                                    oldLeaderOpt = if(leader1.get == 0) None else leader1)
    val leaderEpoch2 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId)
    debug("Leader is elected to be: %s".format(leader1.getOrElse(-1)))
    debug("leader Epoc: " + leaderEpoch2)
    assertEquals("Leader must move to broker 0", 0, leader2.getOrElse(-1))
    if(leader1.get == leader2.get)
      assertEquals("Second epoch value should be " + leaderEpoch1+1, leaderEpoch1+1, leaderEpoch2)
    else
      assertEquals("Second epoch value should be %d".format(leaderEpoch1+1) , leaderEpoch1+1, leaderEpoch2)

    servers.last.startup()
    servers.head.shutdown()
    Thread.sleep(zookeeper.tickTime)
    val leader3 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId,
                                                    oldLeaderOpt = if(leader2.get == 1) None else leader2)
    val leaderEpoch3 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId)
    debug("leader Epoc: " + leaderEpoch3)
    debug("Leader is elected to be: %s".format(leader3.getOrElse(-1)))
    assertEquals("Leader must return to 1", 1, leader3.getOrElse(-1))
    if(leader2.get == leader3.get)
      assertEquals("Second epoch value should be " + leaderEpoch2, leaderEpoch2, leaderEpoch3)
    else
      assertEquals("Second epoch value should be %d".format(leaderEpoch2+1) , leaderEpoch2+1, leaderEpoch3)
  }

  @Test
  def testLeaderElectionWithStaleControllerEpoch() {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 2 replicas, one on each broker
    val leader1 = createTopic(zkClient, topic, partitionReplicaAssignment = Map(0 -> Seq(0, 1)), servers = servers)(0)

    val leaderEpoch1 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId)
    debug("leader Epoc: " + leaderEpoch1)
    debug("Leader is elected to be: %s".format(leader1.getOrElse(-1)))
    assertTrue("Leader should get elected", leader1.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader1.getOrElse(-1) == 0) || (leader1.getOrElse(-1) == 1))
    assertEquals("First epoch value should be 0", 0, leaderEpoch1)

    // start another controller
    val controllerId = 2

    val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
    val brokers = servers.map(s => new Broker(s.config.brokerId, "localhost", s.boundPort()))
    val brokerEndPoints = brokers.map { b =>
      val brokerEndPoint = b.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)
      new LeaderAndIsrRequest.EndPoint(brokerEndPoint.id, brokerEndPoint.host, brokerEndPoint.port)
    }

    val controllerContext = new ControllerContext(zkClient, 6000)
    controllerContext.liveBrokers = brokers.toSet
    val controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig, new SystemTime, new Metrics)
    controllerChannelManager.startup()
    val staleControllerEpoch = 0
    val partitionStates = Map(
      new TopicPartition(topic, partitionId) -> new PartitionState(2, brokerId2, LeaderAndIsr.initialLeaderEpoch,
        Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava, LeaderAndIsr.initialZKVersion,
        Set(0, 1).map(Integer.valueOf).asJava)
    )
    val leaderAndIsrRequest = new LeaderAndIsrRequest(controllerId, staleControllerEpoch, partitionStates.asJava,
      brokerEndPoints.toSet.asJava)

    controllerChannelManager.sendRequest(brokerId2, ApiKeys.LEADER_AND_ISR, None, leaderAndIsrRequest,
      staleControllerEpochCallback)
    TestUtils.waitUntilTrue(() => staleControllerEpochDetected == true,
                            "Controller epoch should be stale")
    assertTrue("Stale controller epoch not detected by the broker", staleControllerEpochDetected)

    controllerChannelManager.shutdown()
  }

  private def staleControllerEpochCallback(response: AbstractRequestResponse): Unit = {
    val leaderAndIsrResponse = response.asInstanceOf[LeaderAndIsrResponse]
    staleControllerEpochDetected = leaderAndIsrResponse.errorCode match {
      case ErrorMapping.StaleControllerEpochCode => true
      case _ => false
    }
  }
}

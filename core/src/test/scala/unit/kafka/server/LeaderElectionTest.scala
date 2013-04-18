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
import kafka.zk.ZooKeeperTestHarness
import kafka.admin.AdminUtils
import kafka.utils.TestUtils._
import junit.framework.Assert._
import kafka.utils.{ZkUtils, Utils, TestUtils}
import kafka.controller.{ControllerContext, LeaderIsrAndControllerEpoch, ControllerChannelManager}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.api._

class LeaderElectionTest extends JUnit3Suite with ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  val port1 = TestUtils.choosePort()
  val port2 = TestUtils.choosePort()

  val configProps1 = TestUtils.createBrokerConfig(brokerId1, port1)
  val configProps2 = TestUtils.createBrokerConfig(brokerId2, port2)
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  var staleControllerEpochDetected = false

  override def setUp() {
    super.setUp()
    // start both servers
    val server1 = TestUtils.createServer(new KafkaConfig(configProps1))
    val server2 = TestUtils.createServer(new KafkaConfig(configProps2))
    servers ++= List(server1, server2)
  }

  override def tearDown() {
    servers.map(server => server.shutdown())
    servers.map(server => Utils.rm(server.config.logDirs))
    super.tearDown()
  }

  def testLeaderElectionAndEpoch {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 2 replicas, one on each broker
    AdminUtils.createTopicWithAssignment(zkClient, topic, Map(0 -> Seq(0, 1)))

    // wait until leader is elected
    val leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500)
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
    val leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1500,
      if(leader1.get == 0) None else leader1)
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
    val leader3 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1500,
      if(leader2.get == 1) None else leader2)
    val leaderEpoch3 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId)
    debug("leader Epoc: " + leaderEpoch3)
    debug("Leader is elected to be: %s".format(leader3.getOrElse(-1)))
    assertEquals("Leader must return to 1", 1, leader3.getOrElse(-1))
    if(leader2.get == leader3.get)
      assertEquals("Second epoch value should be " + leaderEpoch2, leaderEpoch2, leaderEpoch3)
    else
      assertEquals("Second epoch value should be %d".format(leaderEpoch2+1) , leaderEpoch2+1, leaderEpoch3)
  }

  def testLeaderElectionWithStaleControllerEpoch() {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 2 replicas, one on each broker
    AdminUtils.createTopicWithAssignment(zkClient, topic, Map(0 -> Seq(0, 1)))

    // wait until leader is elected
    val leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500)
    val leaderEpoch1 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId)
    debug("leader Epoc: " + leaderEpoch1)
    debug("Leader is elected to be: %s".format(leader1.getOrElse(-1)))
    assertTrue("Leader should get elected", leader1.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader1.getOrElse(-1) == 0) || (leader1.getOrElse(-1) == 1))
    assertEquals("First epoch value should be 0", 0, leaderEpoch1)

    // start another controller
    val controllerId = 2
    val controllerConfig = new KafkaConfig(TestUtils.createBrokerConfig(controllerId, TestUtils.choosePort()))
    val brokers = servers.map(s => new Broker(s.config.brokerId, "localhost", s.config.port))
    val controllerContext = new ControllerContext(zkClient)
    controllerContext.liveBrokers = brokers.toSet
    val controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig)
    controllerChannelManager.startup()
    val staleControllerEpoch = 0
    val leaderAndIsr = new collection.mutable.HashMap[(String, Int), LeaderIsrAndControllerEpoch]
    leaderAndIsr.put((topic, partitionId),
      new LeaderIsrAndControllerEpoch(new LeaderAndIsr(brokerId2, List(brokerId1, brokerId2)), 2))
    val partitionStateInfo = leaderAndIsr.mapValues(l => new PartitionStateInfo(l, 1)).toMap
    val leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfo, brokers.toSet, controllerId, staleControllerEpoch, 0)

    controllerChannelManager.sendRequest(brokerId2, leaderAndIsrRequest, staleControllerEpochCallback)
    TestUtils.waitUntilTrue(() => staleControllerEpochDetected == true, 1000)
    assertTrue("Stale controller epoch not detected by the broker", staleControllerEpochDetected)

    controllerChannelManager.shutdown()
  }

  private def staleControllerEpochCallback(response: RequestOrResponse): Unit = {
    val leaderAndIsrResponse = response.asInstanceOf[LeaderAndIsrResponse]
    staleControllerEpochDetected = leaderAndIsrResponse.errorCode match {
      case ErrorMapping.StaleControllerEpochCode => true
      case _ => false
    }
  }
}
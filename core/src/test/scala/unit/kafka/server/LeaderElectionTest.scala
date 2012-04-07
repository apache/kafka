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
import kafka.admin.CreateTopicCommand
import kafka.utils.TestUtils._
import junit.framework.Assert._
import kafka.utils.{Utils, TestUtils}

class LeaderElectionTest extends JUnit3Suite with ZooKeeperTestHarness {

  val brokerId1 = 0
  val brokerId2 = 1

  val port1 = TestUtils.choosePort()
  val port2 = TestUtils.choosePort()

  val configProps1 = TestUtils.createBrokerConfig(brokerId1, port1)
  val configProps2 = TestUtils.createBrokerConfig(brokerId2, port2)

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  override def setUp() {
    super.setUp()

    // start both servers
    val server1 = TestUtils.createServer(new KafkaConfig(configProps1))
    val server2 = TestUtils.createServer(new KafkaConfig(configProps2))

    servers ++= List(server1, server2)
  }

  override def tearDown() {
    // shutdown the servers and delete data hosted on them
    servers.map(server => server.shutdown())
    servers.map(server => Utils.rm(server.config.logDir))

    super.tearDown()
  }

  def testLeaderElectionWithCreateTopic {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, "0:1")

    // wait until leader is elected
    var leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)

    assertEquals("Leader must be preferred replica on broker 0", 0, leader.getOrElse(-1))

    // kill the server hosting the preferred replica
    servers.head.shutdown()

    // check if leader moves to the other server
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 5000)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    Thread.sleep(zookeeper.tickTime)

    // bring the preferred replica back
    servers.head.startup()

    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)
    // TODO: Once the optimization for preferred replica re-election is in, this check should change to broker 0
    assertEquals("Leader must remain on broker 1", 1, leader.getOrElse(-1))

    // shutdown current leader (broker 1)
    servers.last.shutdown()
    leader = waitUntilLeaderIsElected(zkClient, topic, partitionId, 500)

    // test if the leader is the preferred replica
    assertEquals("Leader must be preferred replica on broker 0", 0, leader.getOrElse(-1))
  }
}
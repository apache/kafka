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

package kafka.integration

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import junit.framework.Assert._
import kafka.utils.{Utils, TestUtils}
import kafka.server.{KafkaConfig, KafkaServer}

class RollingBounceTest extends JUnit3Suite with ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1
  val brokerId3 = 2
  val brokerId4 = 3

  val port1 = TestUtils.choosePort()
  val port2 = TestUtils.choosePort()
  val port3 = TestUtils.choosePort()
  val port4 = TestUtils.choosePort()

  // controlled.shutdown.enable is true by default
  val configProps1 = TestUtils.createBrokerConfig(brokerId1, port1)
  val configProps2 = TestUtils.createBrokerConfig(brokerId2, port2)
  val configProps3 = TestUtils.createBrokerConfig(brokerId3, port3)
  val configProps4 = TestUtils.createBrokerConfig(brokerId4, port4)
  configProps4.put("controlled.shutdown.retry.backoff.ms", "100")

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  val partitionId = 0

  override def setUp() {
    super.setUp()
    // start all the servers
    val server1 = TestUtils.createServer(new KafkaConfig(configProps1))
    val server2 = TestUtils.createServer(new KafkaConfig(configProps2))
    val server3 = TestUtils.createServer(new KafkaConfig(configProps3))
    val server4 = TestUtils.createServer(new KafkaConfig(configProps4))

    servers ++= List(server1, server2, server3, server4)
  }

  override def tearDown() {
    servers.map(server => server.shutdown())
    servers.map(server => Utils.rm(server.config.logDirs))
    super.tearDown()
  }

  def testRollingBounce {
    // start all the brokers
    val topic1 = "new-topic1"
    val topic2 = "new-topic2"
    val topic3 = "new-topic3"
    val topic4 = "new-topic4"

    // create topics with 1 partition, 2 replicas, one on each broker
    createTopic(zkClient, topic1, partitionReplicaAssignment = Map(0->Seq(0,1)), servers = servers)
    createTopic(zkClient, topic2, partitionReplicaAssignment = Map(0->Seq(1,2)), servers = servers)
    createTopic(zkClient, topic3, partitionReplicaAssignment = Map(0->Seq(2,3)), servers = servers)
    createTopic(zkClient, topic4, partitionReplicaAssignment = Map(0->Seq(0,3)), servers = servers)

    // Do a rolling bounce and check if leader transitions happen correctly

    // Bring down the leader for the first topic
    bounceServer(topic1, 0)

    // Bring down the leader for the second topic
    bounceServer(topic2, 1)

    // Bring down the leader for the third topic
    bounceServer(topic3, 2)

    // Bring down the leader for the fourth topic
    bounceServer(topic4, 3)
  }

  private def bounceServer(topic: String, startIndex: Int) {
    var prevLeader = 0
    if (isLeaderLocalOnBroker(topic, partitionId, servers(startIndex))) {
      servers(startIndex).shutdown()
      prevLeader = startIndex
    }
    else {
      servers((startIndex + 1) % 4).shutdown()
      prevLeader = (startIndex + 1) % 4
    }
    var newleader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)
    // Ensure the new leader is different from the old
    assertTrue("Leader transition did not happen for " + topic, newleader.getOrElse(-1) != -1 && (newleader.getOrElse(-1) != prevLeader))
    // Start the server back up again
    servers(prevLeader).startup()
  }
}

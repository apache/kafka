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

import org.junit.{Test, After, Before}
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import org.junit.Assert._
import kafka.utils.{CoreUtils, TestUtils}
import kafka.server.{KafkaConfig, KafkaServer}

class RollingBounceTest extends ZooKeeperTestHarness {

  val partitionId = 0
  var servers: Seq[KafkaServer] = null

  @Before
  override def setUp() {
    super.setUp()
    // controlled.shutdown.enable is true by default
    val configs = (0 until 4).map(i => TestUtils.createBrokerConfig(i, zkConnect))
    configs(3).put("controlled.shutdown.retry.backoff.ms", "100")
 
    // start all the servers
    servers = configs.map(c => TestUtils.createServer(KafkaConfig.fromProps(c)))
  }

  @After
  override def tearDown() {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    super.tearDown()
  }

  @Test
  def testRollingBounce {
    // start all the brokers
    val topic1 = "new-topic1"
    val topic2 = "new-topic2"
    val topic3 = "new-topic3"
    val topic4 = "new-topic4"

    // create topics with 1 partition, 2 replicas, one on each broker
    createTopic(zkUtils, topic1, partitionReplicaAssignment = Map(0->Seq(0,1)), servers = servers)
    createTopic(zkUtils, topic2, partitionReplicaAssignment = Map(0->Seq(1,2)), servers = servers)
    createTopic(zkUtils, topic3, partitionReplicaAssignment = Map(0->Seq(2,3)), servers = servers)
    createTopic(zkUtils, topic4, partitionReplicaAssignment = Map(0->Seq(0,3)), servers = servers)

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
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, oldLeaderOpt = Some(prevLeader))
    // Start the server back up again
    servers(prevLeader).startup()
  }
}

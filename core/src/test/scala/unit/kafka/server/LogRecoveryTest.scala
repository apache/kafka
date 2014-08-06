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

import kafka.utils.TestUtils._
import kafka.utils.IntEncoder
import kafka.utils.{Utils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import kafka.common._
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringEncoder

import java.io.File

import org.scalatest.junit.JUnit3Suite
import org.junit.Assert._

class LogRecoveryTest extends JUnit3Suite with ZooKeeperTestHarness {

  val configs = TestUtils.createBrokerConfigs(2, false).map(new KafkaConfig(_) {
    override val replicaLagTimeMaxMs = 5000L
    override val replicaLagMaxMessages = 10L
    override val replicaFetchWaitMaxMs = 1000
    override val replicaFetchMinBytes = 20
  })
  val topic = "new-topic"
  val partitionId = 0

  var server1: KafkaServer = null
  var server2: KafkaServer = null

  val configProps1 = configs.head
  val configProps2 = configs.last

  val message = "hello"

  var producer: Producer[Int, String] = null
  var hwFile1: OffsetCheckpoint = new OffsetCheckpoint(new File(configProps1.logDirs(0), ReplicaManager.HighWatermarkFilename))
  var hwFile2: OffsetCheckpoint = new OffsetCheckpoint(new File(configProps2.logDirs(0), ReplicaManager.HighWatermarkFilename))
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  override def setUp() {
    super.setUp()

    // start both servers
    server1 = TestUtils.createServer(configProps1)
    server2 = TestUtils.createServer(configProps2)
    servers ++= List(server1, server2)

    // create topic with 1 partition, 2 replicas, one on each broker
    createTopic(zkClient, topic, partitionReplicaAssignment = Map(0->Seq(0,1)), servers = servers)

    // create the producer
    producer = TestUtils.createProducer[Int, String](TestUtils.getBrokerListStrFromConfigs(configs),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[IntEncoder].getName)
  }

  override def tearDown() {
    producer.close()
    for(server <- servers) {
      server.shutdown()
      Utils.rm(server.config.logDirs(0))
    }
    super.tearDown()
  }

  def testHWCheckpointNoFailuresSingleLogSegment {
    val numMessages = 2L
    sendMessages(numMessages.toInt)

    // give some time for the follower 1 to record leader HW
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.getReplica(topic, 0).get.highWatermark.messageOffset == numMessages,
      "Failed to update high watermark for follower after timeout")

    servers.foreach(server => server.replicaManager.checkpointHighWatermarks())
    val leaderHW = hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L)
    assertEquals(numMessages, leaderHW)
    val followerHW = hwFile2.read.getOrElse(TopicAndPartition(topic, 0), 0L)
    assertEquals(numMessages, followerHW)
  }

  def testHWCheckpointWithFailuresSingleLogSegment {
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)

    assertEquals(0L, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))

    sendMessages(1)
    Thread.sleep(1000)
    var hw = 1L

    // kill the server hosting the preferred replica
    server1.shutdown()
    assertEquals(hw, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))

    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = leader)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    // bring the preferred replica back
    server1.startup()

    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)
    assertTrue("Leader must remain on broker 1, in case of zookeeper session expiration it can move to broker 0",
      leader.isDefined && (leader.get == 0 || leader.get == 1))

    assertEquals(hw, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))
    // since server 2 was never shut down, the hw value of 30 is probably not checkpointed to disk yet
    server2.shutdown()
    assertEquals(hw, hwFile2.read.getOrElse(TopicAndPartition(topic, 0), 0L))

    server2.startup()
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = leader)
    assertTrue("Leader must remain on broker 0, in case of zookeeper session expiration it can move to broker 1",
      leader.isDefined && (leader.get == 0 || leader.get == 1))

    sendMessages(1)
    hw += 1

    // give some time for follower 1 to record leader HW of 60
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.getReplica(topic, 0).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(server => server.shutdown())
    assertEquals(hw, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))
    assertEquals(hw, hwFile2.read.getOrElse(TopicAndPartition(topic, 0), 0L))
  }

  def testHWCheckpointNoFailuresMultipleLogSegments {
    sendMessages(20)
    val hw = 20L
    // give some time for follower 1 to record leader HW of 600
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.getReplica(topic, 0).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(server => server.shutdown())
    val leaderHW = hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L)
    assertEquals(hw, leaderHW)
    val followerHW = hwFile2.read.getOrElse(TopicAndPartition(topic, 0), 0L)
    assertEquals(hw, followerHW)
  }

  def testHWCheckpointWithFailuresMultipleLogSegments {
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)

    sendMessages(2)
    var hw = 2L

    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.getReplica(topic, 0).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // kill the server hosting the preferred replica
    server1.shutdown()
    server2.shutdown()
    assertEquals(hw, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))
    assertEquals(hw, hwFile2.read.getOrElse(TopicAndPartition(topic, 0), 0L))

    server2.startup()
    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = leader)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    assertEquals(hw, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))

    // bring the preferred replica back
    server1.startup()

    assertEquals(hw, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))
    assertEquals(hw, hwFile2.read.getOrElse(TopicAndPartition(topic, 0), 0L))

    sendMessages(2)
    hw += 2

    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() =>
      server1.replicaManager.getReplica(topic, 0).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(server => server.shutdown())
    assertEquals(hw, hwFile1.read.getOrElse(TopicAndPartition(topic, 0), 0L))
    assertEquals(hw, hwFile2.read.getOrElse(TopicAndPartition(topic, 0), 0L))
  }

  private def sendMessages(n: Int = 1) {
    for(i <- 0 until n)
      producer.send(new KeyedMessage[Int, String](topic, 0, message))
  }
}

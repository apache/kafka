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

import java.util.Properties

import kafka.utils.TestUtils
import TestUtils._
import kafka.zk.ZooKeeperTestHarness
import java.io.File

import kafka.server.checkpoints.OffsetCheckpointFile
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.junit.{After, Before, Test}
import org.junit.Assert._

class LogRecoveryTest extends ZooKeeperTestHarness {

  val replicaLagTimeMaxMs = 5000L
  val replicaLagMaxMessages = 10L
  val replicaFetchWaitMaxMs = 1000
  val replicaFetchMinBytes = 20

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.ReplicaLagTimeMaxMsProp, replicaLagTimeMaxMs.toString)
  overridingProps.put(KafkaConfig.ReplicaFetchWaitMaxMsProp, replicaFetchWaitMaxMs.toString)
  overridingProps.put(KafkaConfig.ReplicaFetchMinBytesProp, replicaFetchMinBytes.toString)

  var configs: Seq[KafkaConfig] = null
  val topic = "new-topic"
  val partitionId = 0
  val topicPartition = new TopicPartition(topic, partitionId)

  var server1: KafkaServer = null
  var server2: KafkaServer = null

  def configProps1 = configs.head
  def configProps2 = configs.last

  val message = "hello"

  var producer: KafkaProducer[Integer, String] = null
  def hwFile1 = new OffsetCheckpointFile(new File(configProps1.logDirs.head, ReplicaManager.HighWatermarkFilename))
  def hwFile2 = new OffsetCheckpointFile(new File(configProps2.logDirs.head, ReplicaManager.HighWatermarkFilename))
  var servers = Seq.empty[KafkaServer]

  // Some tests restart the brokers then produce more data. But since test brokers use random ports, we need
  // to use a new producer that knows the new ports
  def updateProducer() = {
    if (producer != null)
      producer.close()
    producer = TestUtils.createProducer(
      TestUtils.getBrokerListStrFromServers(servers),
      keySerializer = new IntegerSerializer,
      valueSerializer = new StringSerializer
    )
  }

  @Before
  override def setUp() {
    super.setUp()

    configs = TestUtils.createBrokerConfigs(2, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

    // start both servers
    server1 = TestUtils.createServer(configProps1)
    server2 = TestUtils.createServer(configProps2)
    servers = List(server1, server2)

    // create topic with 1 partition, 2 replicas, one on each broker
    createTopic(zkClient, topic, partitionReplicaAssignment = Map(0 -> Seq(0,1)), servers = servers)

    // create the producer
    updateProducer()
  }

  @After
  override def tearDown() {
    producer.close()
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testHWCheckpointNoFailuresSingleLogSegment(): Unit = {
    val numMessages = 2L
    sendMessages(numMessages.toInt)

    // give some time for the follower 1 to record leader HW
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.localReplica(topicPartition).get.highWatermark.messageOffset == numMessages,
      "Failed to update high watermark for follower after timeout")

    servers.foreach(_.replicaManager.checkpointHighWatermarks())
    val leaderHW = hwFile1.read.getOrElse(topicPartition, 0L)
    assertEquals(numMessages, leaderHW)
    val followerHW = hwFile2.read.getOrElse(topicPartition, 0L)
    assertEquals(numMessages, followerHW)
  }

  @Test
  def testHWCheckpointWithFailuresSingleLogSegment(): Unit = {
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)

    assertEquals(0L, hwFile1.read.getOrElse(topicPartition, 0L))

    sendMessages(1)
    Thread.sleep(1000)
    var hw = 1L

    // kill the server hosting the preferred replica
    server1.shutdown()
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))

    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = Some(leader))
    assertEquals("Leader must move to broker 1", 1, leader)

    // bring the preferred replica back
    server1.startup()
    // Update producer with new server settings
    updateProducer()

    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)
    assertTrue("Leader must remain on broker 1, in case of ZooKeeper session expiration it can move to broker 0",
      leader == 0 || leader == 1)

    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))
    /** We plan to shutdown server2 and transfer the leadership to server1.
      * With unclean leader election turned off, a prerequisite for the successful leadership transition
      * is that server1 has caught up on the topicPartition, and has joined the ISR.
      * In the line below, we wait until the condition is met before shutting down server2
      */
    waitUntilTrue(() => server2.replicaManager.getPartition(topicPartition).get.inSyncReplicas.size == 2,
      "Server 1 is not able to join the ISR after restart")


    // since server 2 was never shut down, the hw value of 30 is probably not checkpointed to disk yet
    server2.shutdown()
    assertEquals(hw, hwFile2.read.getOrElse(topicPartition, 0L))

    server2.startup()
    updateProducer()
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = Some(leader))
    assertTrue("Leader must remain on broker 0, in case of ZooKeeper session expiration it can move to broker 1",
      leader == 0 || leader == 1)

    sendMessages(1)
    hw += 1

    // give some time for follower 1 to record leader HW of 60
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.localReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(_.shutdown())
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile2.read.getOrElse(topicPartition, 0L))
  }

  @Test
  def testHWCheckpointNoFailuresMultipleLogSegments(): Unit = {
    sendMessages(20)
    val hw = 20L
    // give some time for follower 1 to record leader HW of 600
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.localReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(_.shutdown())
    val leaderHW = hwFile1.read.getOrElse(topicPartition, 0L)
    assertEquals(hw, leaderHW)
    val followerHW = hwFile2.read.getOrElse(topicPartition, 0L)
    assertEquals(hw, followerHW)
  }

  @Test
  def testHWCheckpointWithFailuresMultipleLogSegments(): Unit = {
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId)

    sendMessages(2)
    var hw = 2L

    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() =>
      server2.replicaManager.localReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // kill the server hosting the preferred replica
    server1.shutdown()
    server2.shutdown()
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile2.read.getOrElse(topicPartition, 0L))

    server2.startup()
    updateProducer()
    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, oldLeaderOpt = Some(leader))
    assertEquals("Leader must move to broker 1", 1, leader)

    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))

    // bring the preferred replica back
    server1.startup()
    updateProducer()

    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile2.read.getOrElse(topicPartition, 0L))

    sendMessages(2)
    hw += 2

    // allow some time for the follower to create replica
    TestUtils.waitUntilTrue(() => server1.replicaManager.localReplica(topicPartition).nonEmpty,
      "Failed to create replica in follower after timeout")
    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() =>
      server1.replicaManager.localReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(_.shutdown())
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile2.read.getOrElse(topicPartition, 0L))
  }

  private def sendMessages(n: Int) {
    (0 until n).map(_ => producer.send(new ProducerRecord(topic, 0, message))).foreach(_.get)
  }
}

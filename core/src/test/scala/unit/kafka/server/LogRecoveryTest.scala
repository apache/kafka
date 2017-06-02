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
  overridingProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")

  var configs: Seq[KafkaConfig] = null
  val topic = "new-topic"
  val partitionId = 0
  val topicPartition = new TopicPartition(topic, partitionId)

  var server0: KafkaServer = null
  var server1: KafkaServer = null

  def configProps0 = configs.head
  def configProps1 = configs.last

  val message = "hello"

  var producer: KafkaProducer[Integer, String] = null
  def hwFile0 = new OffsetCheckpointFile(new File(configProps0.logDirs.head, ReplicaManager.HighWatermarkFilename))
  def hwFile1 = new OffsetCheckpointFile(new File(configProps1.logDirs.head, ReplicaManager.HighWatermarkFilename))
  var servers = Seq.empty[KafkaServer]

  // Some tests restart the brokers then produce more data. But since test brokers use random ports, we need
  // to use a new producer that knows the new ports
  def updateProducer() = {
    if (producer != null)
      producer.close()
    producer = TestUtils.createNewProducer(
      TestUtils.getBrokerListStrFromServers(servers),
      retries = 5,
      keySerializer = new IntegerSerializer,
      valueSerializer = new StringSerializer
    )
  }

  @Before
  override def setUp() {
    super.setUp()

    configs = TestUtils.createBrokerConfigs(2, zkConnect, enableControlledShutdown = false).map(KafkaConfig.fromProps(_, overridingProps))

    // start both servers
    server0 = TestUtils.createServer(configProps0)
    server1 = TestUtils.createServer(configProps1)
    servers = List(server0, server1)

    // create topic with 1 partition, 2 replicas, one on each broker
    createTopic(zkUtils, topic, partitionReplicaAssignment = Map(0 -> Seq(0, 1)), servers = servers)

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
  def testHWCheckpointNoFailuresSingleLogSegment {
    val numMessages = 2L
    sendMessages(numMessages.toInt)

    // give some time for the follower 1 to record leader HW
    TestUtils.waitUntilTrue(() =>
      server1.replicaManager.getReplica(topicPartition).get.highWatermark.messageOffset == numMessages,
      "Failed to update high watermark for follower after timeout")

    servers.foreach(_.replicaManager.checkpointHighWatermarks())
    val leaderHW = hwFile0.read.getOrElse(topicPartition, 0L)
    assertEquals(numMessages, leaderHW)
    val followerHW = hwFile1.read.getOrElse(topicPartition, 0L)
    assertEquals(numMessages, followerHW)
  }

  @Test
  def testHWCheckpointWithFailuresSingleLogSegment {
    var leader = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId)

    assertEquals(0L, hwFile0.read.getOrElse(topicPartition, 0L))

    sendMessages(1)
    Thread.sleep(1000)
    var hw = 1L

    // kill the server hosting the preferred replica
    server0.shutdown()
    assertEquals(hw, hwFile0.read.getOrElse(topicPartition, 0L))

    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, oldLeaderOpt = Some(leader))
    assertEquals("Leader must move to broker 1", 1, leader)

    // bring the preferred replica back
    server0.startup()
    // Update producer with new server settings
    updateProducer()

    // Leader must remain on broker 1
    leader = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, newLeaderOpt = Some(1))

    assertEquals(hw, hwFile0.read.getOrElse(topicPartition, 0L))
    // since server 1 was never shut down, the hw value is probably not checkpointed to disk yet
    server1.shutdown()
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))

    server1.startup()
    updateProducer()

    leader = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId)
    assertTrue("Leader must either remain on broker 1 or in the case of zookeeper session expiration it can move to broker 0",
      leader == 0 || leader == 1)

    sendMessages(1)
    hw += 1

    // give some time for follower to record leader HW (check both brokers, just in case)
    TestUtils.waitUntilTrue(() =>
      server0.replicaManager.getReplica(topicPartition).get.highWatermark.messageOffset == hw &&
        server1.replicaManager.getReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower before timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(_.shutdown())
    assertEquals(hw, hwFile0.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))
  }

  @Test
  def testHWCheckpointNoFailuresMultipleLogSegments {
    sendMessages(20)
    val hw = 20L
    // give some time for follower 1 to record leader HW of 600
    TestUtils.waitUntilTrue(() =>
      server1.replicaManager.getReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(_.shutdown())
    val leaderHW = hwFile0.read.getOrElse(topicPartition, 0L)
    assertEquals(hw, leaderHW)
    val followerHW = hwFile1.read.getOrElse(topicPartition, 0L)
    assertEquals(hw, followerHW)
  }

  @Test
  def testHWCheckpointWithFailuresMultipleLogSegments {
    var leader = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId)

    sendMessages(2)
    var hw = 2L

    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() =>
      server1.replicaManager.getReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // kill the server hosting the preferred replica
    server0.shutdown()
    server1.shutdown()
    assertEquals(hw, hwFile0.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))

    server1.startup()
    updateProducer()
    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, oldLeaderOpt = Some(leader))
    assertEquals("Leader must move to broker 1", 1, leader)

    assertEquals(hw, hwFile0.read.getOrElse(topicPartition, 0L))

    // bring the preferred replica back
    server0.startup()
    updateProducer()

    assertEquals(hw, hwFile0.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))

    sendMessages(2)
    hw += 2

    // allow some time for the follower to create replica
    TestUtils.waitUntilTrue(() => server0.replicaManager.getReplica(topicPartition).nonEmpty,
      "Failed to create replica in follower after timeout")
    // allow some time for the follower to get the leader HW
    TestUtils.waitUntilTrue(() =>
      server0.replicaManager.getReplica(topicPartition).get.highWatermark.messageOffset == hw,
      "Failed to update high watermark for follower after timeout")
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(_.shutdown())
    assertEquals(hw, hwFile0.read.getOrElse(topicPartition, 0L))
    assertEquals(hw, hwFile1.read.getOrElse(topicPartition, 0L))
  }

  private def sendMessages(n: Int = 1) {
    (0 until n).map(_ => producer.send(new ProducerRecord(topic, 0, message))).foreach(_.get)
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.io.File
import java.util.Collections
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.server.LogDirFailureTest._
import kafka.api.IntegrationTestHarness
import kafka.controller.{OfflineReplica, PartitionAndReplica}
import kafka.utils.{CoreUtils, Exit, TestUtils}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.errors.{KafkaStorageException, NotLeaderForPartitionException}
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import scala.collection.JavaConverters._

/**
  * Test whether clients can producer and consume when there is log directory failure
  */
class LogDirFailureTest extends IntegrationTestHarness {

  val producerCount: Int = 1
  val consumerCount: Int = 1
  val serverCount: Int = 2
  private val topic = "topic"
  private val partitionNum = 12

  this.logDirCount = 3
  this.producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "0")
  this.producerConfig.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")
  this.serverConfig.setProperty(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp, "60000")
  this.serverConfig.setProperty(KafkaConfig.NumReplicaFetchersProp, "1")

  @Before
  override def setUp(): Unit = {
    super.setUp()
    createTopic(topic, partitionNum, serverCount)
  }

  @Test
  def testIOExceptionDuringLogRoll(): Unit = {
    testProduceAfterLogDirFailureOnLeader(Roll)
  }

  @Test
  // Broker should halt on any log directory failure if inter-broker protocol < 1.0
  def brokerWithOldInterBrokerProtocolShouldHaltOnLogDirFailure(): Unit = {
    @volatile var statusCodeOption: Option[Int] = None
    Exit.setHaltProcedure { (statusCode, _) =>
      statusCodeOption = Some(statusCode)
      throw new IllegalArgumentException
    }

    var server: KafkaServer = null
    try {
      val props = TestUtils.createBrokerConfig(serverCount, zkConnect, logDirCount = 3)
      props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.11.0")
      props.put(KafkaConfig.LogMessageFormatVersionProp, "0.11.0")
      val kafkaConfig = KafkaConfig.fromProps(props)
      val logDir = new File(kafkaConfig.logDirs.head)
      // Make log directory of the partition on the leader broker inaccessible by replacing it with a file
      CoreUtils.swallow(Utils.delete(logDir), this)
      logDir.createNewFile()
      assertTrue(logDir.isFile)

      server = TestUtils.createServer(kafkaConfig)
      TestUtils.waitUntilTrue(() => statusCodeOption.contains(1), "timed out waiting for broker to halt")
    } finally {
      Exit.resetHaltProcedure()
      if (server != null)
        TestUtils.shutdownServers(List(server))
    }
  }

  @Test
  def testIOExceptionDuringCheckpoint(): Unit = {
    testProduceAfterLogDirFailureOnLeader(Checkpoint)
  }

  @Test
  def testReplicaFetcherThreadAfterLogDirFailureOnFollower(): Unit = {
    val producer = producers.head
    val partition = new TopicPartition(topic, 0)

    val partitionInfo = producer.partitionsFor(topic).asScala.find(_.partition() == 0).get
    val leaderServerId = partitionInfo.leader().id()
    val leaderServer = servers.find(_.config.brokerId == leaderServerId).get
    val followerServerId = partitionInfo.replicas().map(_.id()).find(_ != leaderServerId).get
    val followerServer = servers.find(_.config.brokerId == followerServerId).get

    followerServer.replicaManager.markPartitionOffline(partition)
    // Send a message to another partition whose leader is the same as partition 0
    // so that ReplicaFetcherThread on the follower will get response from leader immediately
    val anotherPartitionWithTheSameLeader = (1 until partitionNum).find { i =>
      leaderServer.replicaManager.getPartition(new TopicPartition(topic, i)).flatMap(_.leaderReplicaIfLocal).isDefined
    }.get
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, anotherPartitionWithTheSameLeader, topic.getBytes, "message".getBytes)
    // When producer.send(...).get returns, it is guaranteed that ReplicaFetcherThread on the follower
    // has fetched from the leader and attempts to append to the offline replica.
    producer.send(record).get

    assertEquals(serverCount, leaderServer.replicaManager.getPartition(new TopicPartition(topic, anotherPartitionWithTheSameLeader)).get.inSyncReplicas.size)
    followerServer.replicaManager.replicaFetcherManager.fetcherThreadMap.values.foreach { thread =>
      assertFalse("ReplicaFetcherThread should still be working if its partition count > 0", thread.isShutdownComplete)
    }
  }

  def testProduceAfterLogDirFailureOnLeader(failureType: LogDirFailureType): Unit = {
    val consumer = consumers.head
    subscribeAndWaitForAssignment(topic, consumer)
    val producer = producers.head
    val partition = new TopicPartition(topic, 0)
    val record = new ProducerRecord(topic, 0, s"key".getBytes, s"value".getBytes)

    val leaderServerId = producer.partitionsFor(topic).asScala.find(_.partition() == 0).get.leader().id()
    val leaderServer = servers.find(_.config.brokerId == leaderServerId).get

    // The first send() should succeed
    producer.send(record).get()
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0).count() == 1
    }, "Expected the first message", 3000L)

    // Make log directory of the partition on the leader broker inaccessible by replacing it with a file
    val replica = leaderServer.replicaManager.getReplicaOrException(partition)
    val logDir = replica.log.get.dir.getParentFile
    CoreUtils.swallow(Utils.delete(logDir), this)
    logDir.createNewFile()
    assertTrue(logDir.isFile)

    if (failureType == Roll) {
      try {
        leaderServer.replicaManager.getLog(partition).get.roll()
        fail("Log rolling should fail with KafkaStorageException")
      } catch {
        case e: KafkaStorageException => // This is expected
      }
    } else if (failureType == Checkpoint) {
      leaderServer.replicaManager.checkpointHighWatermarks()
    }

    // Wait for ReplicaHighWatermarkCheckpoint to happen so that the log directory of the topic will be offline
    TestUtils.waitUntilTrue(() => !leaderServer.logManager.isLogDirOnline(logDir.getAbsolutePath), "Expected log directory offline", 3000L)
    assertTrue(leaderServer.replicaManager.getReplica(partition).isEmpty)

    // The second send() should fail due to either KafkaStorageException or NotLeaderForPartitionException
    try {
      producer.send(record).get(6000, TimeUnit.MILLISECONDS)
      fail("send() should fail with either KafkaStorageException or NotLeaderForPartitionException")
    } catch {
      case e: ExecutionException =>
        e.getCause match {
          case t: KafkaStorageException =>
          case t: NotLeaderForPartitionException => // This may happen if ProduceRequest version <= 3
          case t: Throwable => fail(s"send() should fail with either KafkaStorageException or NotLeaderForPartitionException instead of ${t.toString}")
        }
      case e: Throwable => fail(s"send() should fail with either KafkaStorageException or NotLeaderForPartitionException instead of ${e.toString}")
    }

    // Wait for producer to update metadata for the partition
    TestUtils.waitUntilTrue(() => {
      // ProduceResponse may contain KafkaStorageException and trigger metadata update
      producer.send(record)
      producer.partitionsFor(topic).asScala.find(_.partition() == 0).get.leader().id() != leaderServerId
    }, "Expected new leader for the partition", 6000L)

    // Consumer should receive some messages
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0).count() > 0
    }, "Expected some messages", 3000L)

    // There should be no remaining LogDirEventNotification znode
    assertTrue(zkClient.getAllLogDirEventNotifications.isEmpty)

    // The controller should have marked the replica on the original leader as offline
    val controllerServer = servers.find(_.kafkaController.isActive).get
    val offlineReplicas = controllerServer.kafkaController.replicaStateMachine.replicasInState(topic, OfflineReplica)
    assertTrue(offlineReplicas.contains(PartitionAndReplica(new TopicPartition(topic, 0), leaderServerId)))
  }

  private def subscribeAndWaitForAssignment(topic: String, consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Unit = {
    consumer.subscribe(Collections.singletonList(topic))
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0)
      !consumer.assignment.isEmpty
    }, "Expected non-empty assignment")
  }

}

object LogDirFailureTest {
  sealed trait LogDirFailureType
  case object Roll extends LogDirFailureType
  case object Checkpoint extends LogDirFailureType
}


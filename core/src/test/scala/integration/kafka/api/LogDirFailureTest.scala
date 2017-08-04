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
package kafka.api

import java.util.Collections
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.controller.{OfflineReplica, PartitionAndReplica}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.errors.{KafkaStorageException, NotLeaderForPartitionException}
import org.junit.{Before, Test}
import org.junit.Assert.assertTrue

/**
  * Test whether clients can producer and consume when there is log directory failure
  */
class LogDirFailureTest extends IntegrationTestHarness {

  import kafka.api.LogDirFailureTest._

  val producerCount: Int = 1
  val consumerCount: Int = 1
  val serverCount: Int = 2
  private val topic = "topic"

  this.logDirCount = 2
  this.producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "0")
  this.producerConfig.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")
  this.serverConfig.setProperty(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp, "60000")


  @Before
  override def setUp() {
    super.setUp()
    TestUtils.createTopic(zkUtils, topic, 1, 2, servers = servers)
  }

  @Test
  def testIOExceptionDuringLogRoll() {
    testProduceAfterLogDirFailure(Roll)
  }

  @Test
  def testIOExceptionDuringCheckpoint() {
    testProduceAfterLogDirFailure(Checkpoint)
  }

  def testProduceAfterLogDirFailure(failureType: LogDirFailureType) {
    val consumer = consumers.head
    subscribeAndWaitForAssignment(topic, consumer)
    val producer = producers.head
    val partition = new TopicPartition(topic, 0)
    val record = new ProducerRecord(topic, 0, s"key".getBytes, s"value".getBytes)

    val leaderServerId = producer.partitionsFor(topic).get(0).leader().id()
    val leaderServer = servers.find(_.config.brokerId == leaderServerId).get

    // The first send() should succeed
    producer.send(record).get()
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0).count() == 1
    }, "Expected the first message", 3000L)

    // Make log directory of the partition on the leader broker inaccessible by replacing it with a file
    val replica = leaderServer.replicaManager.getReplica(partition)
    val logDir = replica.get.log.get.dir.getParentFile
    CoreUtils.swallow(Utils.delete(logDir))
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
    TestUtils.waitUntilTrue(() => !leaderServer.logManager.liveLogDirs.contains(logDir), "Expected log directory offline", 3000L)
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
      producer.partitionsFor(topic).get(0).leader().id() != leaderServerId
    }, "Expected new leader for the partition", 6000L)

    // Consumer should receive some messages
    TestUtils.waitUntilTrue(() => {
      consumer.poll(0).count() > 0
    }, "Expected some messages", 3000L)

    // There should be no remaining LogDirEventNotification znode
    assertTrue(zkUtils.getChildrenParentMayNotExist(ZkUtils.LogDirEventNotificationPath).isEmpty)

    // The controller should have marked the replica on the original leader as offline
    val controllerServer = servers.find(_.kafkaController.isActive).get
    val offlineReplicas = controllerServer.kafkaController.replicaStateMachine.replicasInState(topic, OfflineReplica)
    assertTrue(offlineReplicas.contains(PartitionAndReplica(topic, 0, leaderServerId)))
  }

  private def subscribeAndWaitForAssignment(topic: String, consumer: KafkaConsumer[Array[Byte], Array[Byte]]) {
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


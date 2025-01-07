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
import kafka.api.IntegrationTestHarness
import kafka.utils.TestUtils.{Checkpoint, LogDirFailureType, Roll, waitUntilTrue}
import kafka.utils.{CoreUtils, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{KafkaStorageException, NotLeaderOrFollowerException}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.ParameterizedTest

import java.nio.file.Files
import scala.jdk.CollectionConverters._

/**
  * Test whether clients can produce and consume when there is log directory failure
  */
class LogDirFailureTest extends IntegrationTestHarness {

  val producerCount: Int = 1
  val consumerCount: Int = 1
  val brokerCount: Int = 2
  private val topic = "topic"
  private val partitionNum = 12
  override val logDirCount = 3

  this.serverConfig.setProperty(ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG, "60000")
  this.serverConfig.setProperty(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, "1")
  this.serverConfig.setProperty(ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_CONFIG, "5000")
  this.serverConfig.setProperty(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(topic, partitionNum, brokerCount)
    ensureConsistentKRaftMetadata()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProduceErrorFromFailureOnLogRoll(quorum: String, groupProtocol: String): Unit = {
    testProduceErrorsFromLogDirFailureOnLeader(Roll)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testLogDirNotificationTimeout(quorum: String, groupProtocol: String): Unit = {
    // Disable retries to allow exception to bubble up for validation
    this.producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "0")
    this.producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    val producer = createProducer()

    val partition = new TopicPartition(topic, 0)

    val leaderServerId = producer.partitionsFor(topic).asScala.find(_.partition() == 0).get.leader().id()
    val leaderServer = brokers.find(_.config.brokerId == leaderServerId).get

    // shut down the controller to simulate the case where the broker is not able to send the log dir notification
    controllerServer.shutdown()
    controllerServer.awaitShutdown()

    causeLogDirFailure(Checkpoint, leaderServer, partition)

    TestUtils.waitUntilTrue(() => leaderServer.brokerState == BrokerState.SHUTTING_DOWN,
      s"Expected broker to be in NOT_RUNNING state but was ${leaderServer.brokerState}", 15000)
    // wait for actual shutdown (by default max 5 minutes for graceful shutdown)
    leaderServer.awaitShutdown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testIOExceptionDuringLogRoll(quorum: String, groupProtocol: String): Unit = {
    testProduceAfterLogDirFailureOnLeader(Roll, quorum)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testProduceErrorFromFailureOnCheckpoint(quorum: String, groupProtocol: String): Unit = {
    testProduceErrorsFromLogDirFailureOnLeader(Checkpoint)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testIOExceptionDuringCheckpoint(quorum: String, groupProtocol: String): Unit = {
    testProduceAfterLogDirFailureOnLeader(Checkpoint, quorum)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testReplicaFetcherThreadAfterLogDirFailureOnFollower(quorum: String, groupProtocol: String): Unit = {
    this.producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "0")
    this.producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    val producer = createProducer()
    val partition = new TopicPartition(topic, 0)

    val partitionInfo = producer.partitionsFor(topic).asScala.find(_.partition() == 0).get
    val leaderServerId = partitionInfo.leader().id()
    val leaderServer = brokers.find(_.config.brokerId == leaderServerId).get
    val followerServerId = partitionInfo.replicas().map(_.id()).find(_ != leaderServerId).get
    val followerServer = brokers.find(_.config.brokerId == followerServerId).get

    followerServer.replicaManager.markPartitionOffline(partition)
    // Send a message to another partition whose leader is the same as partition 0
    // so that ReplicaFetcherThread on the follower will get response from leader immediately
    val anotherPartitionWithTheSameLeader = (1 until partitionNum).find { i =>
      leaderServer.replicaManager.onlinePartition(new TopicPartition(topic, i))
        .flatMap(_.leaderLogIfLocal).isDefined
    }.get
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, anotherPartitionWithTheSameLeader, topic.getBytes, "message".getBytes)
    // When producer.send(...).get returns, it is guaranteed that ReplicaFetcherThread on the follower
    // has fetched from the leader and attempts to append to the offline replica.
    producer.send(record).get

    assertEquals(brokerCount, leaderServer.replicaManager.onlinePartition(new TopicPartition(topic, anotherPartitionWithTheSameLeader))
      .get.inSyncReplicaIds.size)
    followerServer.replicaManager.replicaFetcherManager.fetcherThreadMap.values.foreach { thread =>
      assertFalse(thread.isShutdownComplete, "ReplicaFetcherThread should still be working if its partition count > 0")
    }
  }

  def testProduceErrorsFromLogDirFailureOnLeader(failureType: LogDirFailureType): Unit = {
    // Disable retries to allow exception to bubble up for validation
    this.producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "0")
    this.producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    val producer = createProducer()

    val partition = new TopicPartition(topic, 0)
    val record = new ProducerRecord(topic, 0, s"key".getBytes, s"value".getBytes)

    val leaderServerId = producer.partitionsFor(topic).asScala.find(_.partition() == 0).get.leader().id()
    val leaderServer = brokers.find(_.config.brokerId == leaderServerId).get

    causeLogDirFailure(failureType, leaderServer, partition)

    // send() should fail due to either KafkaStorageException or NotLeaderOrFollowerException
    val e = assertThrows(classOf[ExecutionException], () => producer.send(record).get(6000, TimeUnit.MILLISECONDS))
    assertTrue(e.getCause.isInstanceOf[KafkaStorageException] ||
      // This may happen if ProduceRequest version <= 3
      e.getCause.isInstanceOf[NotLeaderOrFollowerException])
  }

  def testProduceAfterLogDirFailureOnLeader(failureType: LogDirFailureType, quorum: String): Unit = {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()

    val partition = new TopicPartition(topic, 0)
    val record = new ProducerRecord(topic, 0, s"key".getBytes, s"value".getBytes)

    val originalLeaderServerId = producer.partitionsFor(topic).asScala.find(_.partition() == 0).get.leader().id()
    val originalLeaderServer = brokers.find(_.config.brokerId == originalLeaderServerId).get

    // The first send() should succeed
    producer.send(record).get()
    TestUtils.consumeRecords(consumer, 1)

    val failedLogDir = causeLogDirFailure(failureType, originalLeaderServer, partition)

    TestUtils.waitUntilTrue(() => {
      // ProduceResponse may contain KafkaStorageException and trigger metadata update
      producer.send(record)
      producer.partitionsFor(topic).asScala.find(_.partition() == 0).get.leader().id() != originalLeaderServerId
    }, "Expected new leader for the partition")

    // Block on send to ensure that new leader accepts a message.
    producer.send(record).get(6000L, TimeUnit.MILLISECONDS)

    // Consumer should receive some messages
    TestUtils.pollUntilAtLeastNumRecords(consumer, 1)

    waitUntilTrue(() => {
      // get the broker with broker.nodeId == originalLeaderServerId
      val brokerWithDirFail = brokers.find(_.config.nodeId == originalLeaderServerId).map(_.asInstanceOf[BrokerServer])
      // check if the broker has the offline log dir
      val hasOfflineDir = brokerWithDirFail.exists(_.logDirFailureChannel.hasOfflineLogDir(failedLogDir.toPath.toString))
      // check if the broker has the offline replica
      hasOfflineDir && brokerWithDirFail.exists(broker =>
        broker.replicaManager.metadataCache
          .getClusterMetadata(broker.clusterId, broker.config.interBrokerListenerName)
          .partition(new TopicPartition(topic, 0)).offlineReplicas().map(_.id()).contains(originalLeaderServerId))
    }, "Expected to find an offline log dir")
  }


  private def subscribeAndWaitForAssignment(topic: String, consumer: Consumer[Array[Byte], Array[Byte]]): Unit = {
    consumer.subscribe(Collections.singletonList(topic))
    TestUtils.pollUntilTrue(consumer, () => !consumer.assignment.isEmpty, "Expected non-empty assignment")
  }

  def causeLogDirFailure(failureType: LogDirFailureType, leaderBroker: KafkaBroker, partition: TopicPartition): File = {
    // Make log directory of the partition on the leader broker inaccessible by replacing it with a file
    val localLog = leaderBroker.replicaManager.localLogOrException(partition)
    val logDir = localLog.dir.getParentFile
    CoreUtils.swallow(Utils.delete(logDir), this)
    Files.createFile(logDir.toPath)
    assertTrue(logDir.isFile)

    if (failureType == Roll) {
      assertThrows(classOf[KafkaStorageException], () => leaderBroker.replicaManager.getLog(partition).get.roll())
    } else if (failureType == Checkpoint) {
      leaderBroker.replicaManager.checkpointHighWatermarks()
    }

    // Wait for ReplicaHighWatermarkCheckpoint to happen so that the log directory of the topic will be offline
    waitUntilTrue(() => !leaderBroker.logManager.isLogDirOnline(logDir.getAbsolutePath), "Expected log directory offline", 3000L)
    assertTrue(leaderBroker.replicaManager.localLog(partition).isEmpty)
    logDir
  }

}

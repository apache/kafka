/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.UnexpectedAppendOffsetException
import kafka.log.{Log, LogConfig, LogManager, CleanerConfig}
import kafka.server._
import kafka.utils.{MockTime, TestUtils, MockScheduler}
import kafka.utils.timer.MockTimer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ReplicaNotAvailableException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.record._
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.scalatest.Assertions.assertThrows
import scala.collection.JavaConverters._

class PartitionTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val metrics = new Metrics

  var tmpDir: File = _
  var logDir: File = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = _
  var logConfig: LogConfig = _

  @Before
  def setup(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    logDir = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir), defaultConfig = logConfig, CleanerConfig(enableCleaner = false), time)
    logManager.startup()

    val brokerProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    brokerProps.put("log.dir", logDir.getAbsolutePath)
    val brokerConfig = KafkaConfig.fromProps(brokerProps)
    replicaManager = new ReplicaManager(
      config = brokerConfig, metrics, time, zkClient = null, new MockScheduler(time),
      logManager, new AtomicBoolean(false), QuotaFactory.instantiate(brokerConfig, metrics, time, ""),
      brokerTopicStats, new MetadataCache(brokerId), new LogDirFailureChannel(brokerConfig.logDirs.size))
  }

  @After
  def tearDown(): Unit = {
    brokerTopicStats.close()
    metrics.close()

    logManager.shutdown()
    Utils.delete(tmpDir)
    logManager.liveLogDirs.foreach(Utils.delete)
    replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testAppendRecordsAsFollowerBelowLogStartOffset(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)
    partition.addReplicaIfNotExists(replica)
    assertEquals(Some(replica), partition.getReplica(replica.brokerId))

    val initialLogStartOffset = 5L
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(s"Log end offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, replica.logStartOffset)

    // verify that we cannot append records that do not contain log start offset even if the log is empty
    assertThrows[UnexpectedAppendOffsetException] {
      // append one record with offset = 3
      partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 3L), isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", initialLogStartOffset, replica.logEndOffset.messageOffset)

    // verify that we can append records that contain log start offset, even when first
    // offset < log start offset if the log is empty
    val newLogStartOffset = 4L
    val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                     new SimpleRecord("k2".getBytes, "v2".getBytes),
                                     new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                baseOffset = newLogStartOffset)
    partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
    assertEquals(s"Log end offset after append of 3 records with base offset $newLogStartOffset:", 7L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset after append of 3 records with base offset $newLogStartOffset:", newLogStartOffset, replica.logStartOffset)

    // and we can append more records after that
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 7L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 7:", 8L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, replica.logStartOffset)

    // but we cannot append to offset < log start if the log is not empty
    assertThrows[UnexpectedAppendOffsetException] {
      val records2 = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                        new SimpleRecord("k2".getBytes, "v2".getBytes)),
                                   baseOffset = 3L)
      partition.appendRecordsToFollowerOrFutureReplica(records2, isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", 8L, replica.logEndOffset.messageOffset)

    // we still can append to next offset
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 8L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 8:", 9L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, replica.logStartOffset)
  }

  @Test
  def testGetReplica(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val partition = new
        Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)

    assertEquals(None, partition.getReplica(brokerId))
    assertThrows[ReplicaNotAvailableException] {
      partition.getReplicaOrException(brokerId)
    }

    partition.addReplicaIfNotExists(replica)
    assertEquals(replica, partition.getReplicaOrException(brokerId))
  }

  @Test
  def testAppendRecordsToFollowerWithNoReplicaThrowsException(): Unit = {
    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)
    assertThrows[ReplicaNotAvailableException] {
      partition.appendRecordsToFollowerOrFutureReplica(
           createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 0L), isFuture = false)
    }
  }

  def createRecords(records: Iterable[SimpleRecord], baseOffset: Long, partitionLeaderEpoch: Int = 0): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(
      buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.LOG_APPEND_TIME,
      baseOffset, time.milliseconds, partitionLeaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

}

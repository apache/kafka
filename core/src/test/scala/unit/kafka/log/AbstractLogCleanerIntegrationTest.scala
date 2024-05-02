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
package kafka.log

import java.io.File
import java.nio.file.Files
import java.util.Properties
import kafka.server.BrokerTopicStats
import kafka.utils.{Pool, TestUtils}
import kafka.utils.Implicits._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig, LogDirFailureChannel, ProducerStateManagerConfig}
import org.junit.jupiter.api.{AfterEach, Tag}

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.util.Random

@Tag("integration")
abstract class AbstractLogCleanerIntegrationTest {

  var cleaner: LogCleaner = _
  val logDir = TestUtils.tempDir()

  private val logs = ListBuffer.empty[UnifiedLog]
  private val defaultMaxMessageSize = 128
  private val defaultMinCleanableDirtyRatio = 0.0F
  private val defaultMinCompactionLagMS = 0L
  private val defaultDeleteDelay = 1000
  private val defaultSegmentSize = 2048
  private val defaultMaxCompactionLagMs = Long.MaxValue

  def time: MockTime

  @AfterEach
  def teardown(): Unit = {
    if (cleaner != null)
      cleaner.shutdown()
    time.scheduler.shutdown()
    logs.foreach(_.close())
    Utils.delete(logDir)
  }

  def logConfigProperties(propertyOverrides: Properties = new Properties(),
                          maxMessageSize: Int,
                          minCleanableDirtyRatio: Float = defaultMinCleanableDirtyRatio,
                          minCompactionLagMs: Long = defaultMinCompactionLagMS,
                          deleteDelay: Int = defaultDeleteDelay,
                          segmentSize: Int = defaultSegmentSize,
                          maxCompactionLagMs: Long = defaultMaxCompactionLagMs): Properties = {
    val props = new Properties()
    props.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageSize: java.lang.Integer)
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, segmentSize: java.lang.Integer)
    props.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 100*1024: java.lang.Integer)
    props.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, deleteDelay: java.lang.Integer)
    props.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    props.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, minCleanableDirtyRatio: java.lang.Float)
    props.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, minCompactionLagMs: java.lang.Long)
    props.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, maxCompactionLagMs: java.lang.Long)
    props ++= propertyOverrides
    props
  }

  def makeCleaner(partitions: Iterable[TopicPartition],
                  minCleanableDirtyRatio: Float = defaultMinCleanableDirtyRatio,
                  numThreads: Int = 1,
                  backoffMs: Long = 15000L,
                  maxMessageSize: Int = defaultMaxMessageSize,
                  minCompactionLagMs: Long = defaultMinCompactionLagMS,
                  deleteDelay: Int = defaultDeleteDelay,
                  segmentSize: Int = defaultSegmentSize,
                  maxCompactionLagMs: Long = defaultMaxCompactionLagMs,
                  cleanerIoBufferSize: Option[Int] = None,
                  propertyOverrides: Properties = new Properties()): LogCleaner = {

    val logMap = new Pool[TopicPartition, UnifiedLog]()
    for (partition <- partitions) {
      val dir = new File(logDir, s"${partition.topic}-${partition.partition}")
      Files.createDirectories(dir.toPath)

      val logConfig = new LogConfig(logConfigProperties(propertyOverrides,
        maxMessageSize = maxMessageSize,
        minCleanableDirtyRatio = minCleanableDirtyRatio,
        minCompactionLagMs = minCompactionLagMs,
        deleteDelay = deleteDelay,
        segmentSize = segmentSize,
        maxCompactionLagMs = maxCompactionLagMs))
      val log = UnifiedLog(
        dir = dir,
        config = logConfig,
        logStartOffset = 0L,
        recoveryPoint = 0L,
        scheduler = time.scheduler,
        time = time,
        brokerTopicStats = new BrokerTopicStats,
        maxTransactionTimeoutMs = 5 * 60 * 1000,
        producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
        producerIdExpirationCheckIntervalMs = TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
        logDirFailureChannel = new LogDirFailureChannel(10),
        topicId = None,
        keepPartitionMetadataFile = true)
      logMap.put(partition, log)
      this.logs += log
    }

    val cleanerConfig = new CleanerConfig(
      numThreads,
      4 * 1024 * 1024L,
      0.9,
      cleanerIoBufferSize.getOrElse(maxMessageSize / 2),
      maxMessageSize,
      Double.MaxValue,
      backoffMs,
      true)
    new LogCleaner(cleanerConfig,
      logDirs = Array(logDir),
      logs = logMap,
      logDirFailureChannel = new LogDirFailureChannel(1),
      time = time)
  }

  private var ctr = 0
  def counter: Int = ctr
  def incCounter(): Unit = ctr += 1

  def writeDups(numKeys: Int, numDups: Int, log: UnifiedLog, codec: CompressionType,
                startKey: Int = 0, magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE): Seq[(Int, String, Long)] = {
    for (_ <- 0 until numDups; key <- startKey until (startKey + numKeys)) yield {
      val value = counter.toString
      val appendInfo = log.appendAsLeader(TestUtils.singletonRecords(value = value.getBytes, codec = codec,
        key = key.toString.getBytes, magicValue = magicValue), leaderEpoch = 0)
      // move LSO forward to increase compaction bound
      log.updateHighWatermark(log.logEndOffset)
      incCounter()
      (key, value, appendInfo.firstOffset)
    }
  }

  def createLargeSingleMessageSet(key: Int, messageFormatVersion: Byte, codec: CompressionType): (String, MemoryRecords) = {
    def messageValue(length: Int): String = {
      val random = new Random(0)
      new String(random.alphanumeric.take(length).toArray)
    }
    val value = messageValue(128)
    val messageSet = TestUtils.singletonRecords(value = value.getBytes, codec = codec, key = key.toString.getBytes,
      magicValue = messageFormatVersion)
    (value, messageSet)
  }

  def closeLog(log: UnifiedLog): Unit = {
    log.close()
    logs -= log
  }
}

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

import java.util.Properties
import java.util.concurrent.{Callable, Executors}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel, ProducerStateManagerConfig}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.mutable.ListBuffer
import scala.util.Random

class LogConcurrencyTest {
  private val brokerTopicStats = new BrokerTopicStats
  private val random = new Random()
  private val scheduler = new KafkaScheduler(1)
  private val tmpDir = TestUtils.tempDir()
  private val logDir = TestUtils.randomPartitionLogDir(tmpDir)

  @BeforeEach
  def setup(): Unit = {
    scheduler.startup()
  }

  @AfterEach
  def shutdown(): Unit = {
    scheduler.shutdown()
    Utils.delete(tmpDir)
  }

  @Test
  def testUncommittedDataNotConsumed(): Unit = {
    testUncommittedDataNotConsumed(createLog())
  }

  @Test
  def testUncommittedDataNotConsumedFrequentSegmentRolls(): Unit = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 237: Integer)
    val logConfig = new LogConfig(logProps)
    testUncommittedDataNotConsumed(createLog(logConfig))
  }

  def testUncommittedDataNotConsumed(log: UnifiedLog): Unit = {
    val executor = Executors.newFixedThreadPool(2)
    try {
      val maxOffset = 5000
      val consumer = new ConsumerTask(log, maxOffset)
      val appendTask = new LogAppendTask(log, maxOffset)

      val consumerFuture = executor.submit(consumer)
      val fetcherTaskFuture = executor.submit(appendTask)

      fetcherTaskFuture.get()
      consumerFuture.get()

      validateConsumedData(log, consumer.consumedBatches)
    } finally executor.shutdownNow()
  }

  /**
   * Simple consumption task which reads the log in ascending order and collects
   * consumed batches for validation
   */
  private class ConsumerTask(log: UnifiedLog, lastOffset: Int) extends Callable[Unit] {
    val consumedBatches = ListBuffer.empty[FetchedBatch]

    override def call(): Unit = {
      var fetchOffset = 0L
      while (log.highWatermark < lastOffset) {
        val readInfo = log.read(
          startOffset = fetchOffset,
          maxLength = 1,
          isolation = FetchIsolation.HIGH_WATERMARK,
          minOneMessage = true
        )
        readInfo.records.batches().forEach { batch =>
          consumedBatches += FetchedBatch(batch.baseOffset, batch.partitionLeaderEpoch)
          fetchOffset = batch.lastOffset + 1
        }
      }
    }
  }

  /**
   * This class simulates basic leader/follower behavior.
   */
  private class LogAppendTask(log: UnifiedLog, lastOffset: Long) extends Callable[Unit] {
    override def call(): Unit = {
      var leaderEpoch = 1
      var isLeader = true

      while (log.highWatermark < lastOffset) {
        random.nextInt(2) match {
          case 0 =>
            val logEndOffsetMetadata = log.logEndOffsetMetadata
            val logEndOffset = logEndOffsetMetadata.messageOffset
            val batchSize = random.nextInt(9) + 1
            val records = (0 to batchSize).map(i => new SimpleRecord(s"$i".getBytes))

            if (isLeader) {
              log.appendAsLeader(TestUtils.records(records), leaderEpoch)
              log.maybeIncrementHighWatermark(logEndOffsetMetadata)
            } else {
              log.appendAsFollower(TestUtils.records(records,
                baseOffset = logEndOffset,
                partitionLeaderEpoch = leaderEpoch))
              log.updateHighWatermark(logEndOffset)
            }

          case 1 =>
            isLeader = !isLeader
            leaderEpoch += 1

            if (!isLeader) {
              log.truncateTo(log.highWatermark)
            }
        }
      }
    }
  }

  private def createLog(config: LogConfig = new LogConfig(new Properties())): UnifiedLog = {
    UnifiedLog(dir = logDir,
      config = config,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = scheduler,
      brokerTopicStats = brokerTopicStats,
      time = Time.SYSTEM,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
      producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
      logDirFailureChannel = new LogDirFailureChannel(10),
      topicId = None,
      keepPartitionMetadataFile = true
    )
  }

  private def validateConsumedData(log: UnifiedLog, consumedBatches: Iterable[FetchedBatch]): Unit = {
    val iter = consumedBatches.iterator
    log.logSegments.forEach { segment =>
      segment.log.batches.forEach { batch =>
        if (iter.hasNext) {
          val consumedBatch = iter.next()
          try {
            assertEquals(batch.partitionLeaderEpoch,
              consumedBatch.epoch, "Consumed batch with unexpected leader epoch")
            assertEquals(batch.baseOffset,
              consumedBatch.baseOffset, "Consumed batch with unexpected base offset")
          } catch {
            case t: Throwable =>
              throw new AssertionError(s"Consumed batch $consumedBatch " +
                s"does not match next expected batch in log $batch", t)
          }
        }
      }
    }
  }

  private case class FetchedBatch(baseOffset: Long, epoch: Int) {
    override def toString: String = {
      s"FetchedBatch(baseOffset=$baseOffset, epoch=$epoch)"
    }
  }

}

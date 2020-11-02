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

import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils.{Scheduler, TestUtils, Throttler}
import org.apache.kafka.common.record.{CompressionType, FileRecords, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.TopicPartition

object LogUtils {
  /**
    *  Create a segment with the given base offset
    */
  def createSegment(offset: Long,
                    logDir: File,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    val ms = FileRecords.open(Log.logFile(logDir, offset))
    val idx = LazyIndex.forOffset(Log.offsetIndexFile(logDir, offset), offset, maxIndexSize = 1000)
    val timeIdx = LazyIndex.forTime(Log.timeIndexFile(logDir, offset), offset, maxIndexSize = 1500)
    val txnIndex = new TransactionIndex(offset, Log.transactionIndexFile(logDir, offset))

    new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time)
  }

  def createLog(dir: File, config: LogConfig, time: Time, scheduler: Scheduler, recoveryPoint: Long = 0L) =
    Log(dir = dir, config = config, logStartOffset = 0L, recoveryPoint = recoveryPoint, scheduler = scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10))

  def record(key: Int, value: Int,
             producerId: Long = RecordBatch.NO_PRODUCER_ID,
             producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
             sequence: Int = RecordBatch.NO_SEQUENCE,
             partitionLeaderEpoch: Int = RecordBatch.NO_PARTITION_LEADER_EPOCH): MemoryRecords = {
    MemoryRecords.withIdempotentRecords(RecordBatch.CURRENT_MAGIC_VALUE, 0L, CompressionType.NONE, producerId, producerEpoch, sequence,
      partitionLeaderEpoch, new SimpleRecord(key.toString.getBytes, value.toString.getBytes))
  }

  def record(key: Int, value: Array[Byte]): MemoryRecords =
    TestUtils.singletonRecords(key = key.toString.getBytes, value = value)

  def createCleaner(capacity: Int,time: Time , throttler: Throttler,
                  checkDone: TopicPartition => Unit = _ => (), maxMessageSize: Int = 64*1024) =
    new Cleaner(id = 0,
      offsetMap = new FakeOffsetMap(capacity),
      ioBufferSize = maxMessageSize,
      maxIoBufferSize = maxMessageSize,
      dupBufferLoadFactor = 0.75,
      throttler = throttler,
      time = time,
      checkDone = checkDone)
}

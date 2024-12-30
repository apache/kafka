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
package kafka.coordinator.group

import kafka.server.ReplicaManager
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.record.{ControlRecordType, FileRecords, MemoryRecords}
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader.{LoadSummary, UnknownRecordTypeException}
import org.apache.kafka.coordinator.common.runtime.{CoordinatorLoader, CoordinatorPlayback, Deserializer}
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.KafkaScheduler

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._

/**
 * Coordinator loader which reads records from a partition and replays them
 * to a group coordinator.
 *
 * @param replicaManager  The replica manager.
 * @param deserializer    The deserializer to use.
 * @param loadBufferSize  The load buffer size.
 * @tparam T The record type.
 */
class CoordinatorLoaderImpl[T](
  time: Time,
  replicaManager: ReplicaManager,
  deserializer: Deserializer[T],
  loadBufferSize: Int
) extends CoordinatorLoader[T] with Logging {
  private val isRunning = new AtomicBoolean(true)
  private val scheduler = new KafkaScheduler(1)
  scheduler.startup()

  /**
   * Loads the coordinator by reading all the records from the TopicPartition
   * and applying them to the Replayable object.
   *
   * @param tp          The TopicPartition to read from.
   * @param coordinator The object to apply records to.
   */
  override def load(
    tp: TopicPartition,
    coordinator: CoordinatorPlayback[T]
): CompletableFuture[LoadSummary] = {
    val future = new CompletableFuture[LoadSummary]()
    val startTimeMs = time.milliseconds()
    val result = scheduler.scheduleOnce(s"Load coordinator from $tp",
      () => doLoad(tp, coordinator, future, startTimeMs))
    if (result.isCancelled) {
      future.completeExceptionally(new RuntimeException("Coordinator loader is closed."))
    }
    future
  }

  private def doLoad(
    tp: TopicPartition,
    coordinator: CoordinatorPlayback[T],
    future: CompletableFuture[LoadSummary],
    startTimeMs: Long
  ): Unit = {
    val schedulerQueueTimeMs = time.milliseconds() - startTimeMs
    try {
      replicaManager.getLog(tp) match {
        case None =>
          future.completeExceptionally(new NotLeaderOrFollowerException(
            s"Could not load records from $tp because the log does not exist."))

        case Some(log) =>
          def logEndOffset: Long = replicaManager.getLogEndOffset(tp).getOrElse(-1L)

          // Buffer may not be needed if records are read from memory.
          var buffer = ByteBuffer.allocate(0)
          // Loop breaks if leader changes at any time during the load, since logEndOffset is -1.
          var currentOffset = log.logStartOffset
          // Loop breaks if no records have been read, since the end of the log has been reached.
          // This is to ensure that the loop breaks even if the current offset remains smaller than
          // the log end offset but the log is empty. This could happen with compacted topics.
          var readAtLeastOneRecord = true

          var previousHighWatermark = -1L
          var numRecords = 0L
          var numBytes = 0L
          while (currentOffset < logEndOffset && readAtLeastOneRecord && isRunning.get) {
            val fetchDataInfo = log.read(
              startOffset = currentOffset,
              maxLength = loadBufferSize,
              isolation = FetchIsolation.LOG_END,
              minOneMessage = true
            )

            readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes > 0

            val memoryRecords = (fetchDataInfo.records: @unchecked) match {
              case records: MemoryRecords =>
                records

              case fileRecords: FileRecords =>
                val sizeInBytes = fileRecords.sizeInBytes
                val bytesNeeded = Math.max(loadBufferSize, sizeInBytes)

                // "minOneMessage = true in the above log.read() means that the buffer may need to
                // be grown to ensure progress can be made.
                if (buffer.capacity < bytesNeeded) {
                  if (loadBufferSize < bytesNeeded)
                    warn(s"Loaded metadata from $tp with buffer larger ($bytesNeeded bytes) than " +
                      s"configured buffer size ($loadBufferSize bytes).")

                  buffer = ByteBuffer.allocate(bytesNeeded)
                } else {
                  buffer.clear()
                }

                fileRecords.readInto(buffer, 0)
                MemoryRecords.readableRecords(buffer)
            }

            memoryRecords.batches.forEach { batch =>
              if (batch.isControlBatch) {
                batch.asScala.foreach { record =>
                  val controlRecord = ControlRecordType.parse(record.key)
                  if (controlRecord == ControlRecordType.COMMIT) {
                    if (isTraceEnabled) {
                      trace(s"Replaying end transaction marker from $tp at offset ${record.offset} to commit transaction " +
                        s"with producer id ${batch.producerId} and producer epoch ${batch.producerEpoch}.")
                    }
                    coordinator.replayEndTransactionMarker(
                      batch.producerId,
                      batch.producerEpoch,
                      TransactionResult.COMMIT
                    )
                  } else if (controlRecord == ControlRecordType.ABORT) {
                    if (isTraceEnabled) {
                      trace(s"Replaying end transaction marker from $tp at offset ${record.offset} to abort transaction " +
                        s"with producer id ${batch.producerId} and producer epoch ${batch.producerEpoch}.")
                    }
                    coordinator.replayEndTransactionMarker(
                      batch.producerId,
                      batch.producerEpoch,
                      TransactionResult.ABORT
                    )
                  }
                }
              } else {
                batch.asScala.foreach { record =>
                  numRecords = numRecords + 1

                  val coordinatorRecordOpt = {
                    try {
                      Some(deserializer.deserialize(record.key, record.value))
                    } catch {
                      case ex: UnknownRecordTypeException =>
                        warn(s"Unknown record type ${ex.unknownType} while loading offsets and group metadata " +
                          s"from $tp. Ignoring it. It could be a left over from an aborted upgrade.")
                        None
                      case ex: RuntimeException =>
                        val msg = s"Deserializing record $record from $tp failed due to: ${ex.getMessage}"
                        error(s"$msg.")
                        throw new RuntimeException(msg, ex)
                    }
                  }

                  coordinatorRecordOpt.foreach { coordinatorRecord =>
                    try {
                      if (isTraceEnabled) {
                        trace(s"Replaying record $coordinatorRecord from $tp at offset ${record.offset()} " +
                          s"with producer id ${batch.producerId} and producer epoch ${batch.producerEpoch}.")
                      }
                      coordinator.replay(
                        record.offset(),
                        batch.producerId,
                        batch.producerEpoch,
                        coordinatorRecord
                      )
                    } catch {
                      case ex: RuntimeException =>
                        val msg = s"Replaying record $coordinatorRecord from $tp at offset ${record.offset()} " +
                          s"with producer id ${batch.producerId} and producer epoch ${batch.producerEpoch} " +
                          s"failed due to: ${ex.getMessage}"
                        error(s"$msg.")
                        throw new RuntimeException(msg, ex)
                    }
                  }
                }
              }

              // Note that the high watermark can be greater than the current offset but as we load more records
              // the current offset will eventually surpass the high watermark. Also note that the high watermark
              // will continue to advance while loading.
              currentOffset = batch.nextOffset
              val currentHighWatermark = log.highWatermark
              if (currentOffset >= currentHighWatermark) {
                coordinator.updateLastWrittenOffset(currentOffset)

                if (currentHighWatermark > previousHighWatermark) {
                  coordinator.updateLastCommittedOffset(currentHighWatermark)
                  previousHighWatermark = currentHighWatermark
                }
              }
            }
            numBytes = numBytes + memoryRecords.sizeInBytes()
          }

          val endTimeMs = time.milliseconds()

          if (logEndOffset == -1L) {
            future.completeExceptionally(new NotLeaderOrFollowerException(
              s"Stopped loading records from $tp because the partition is not online or is no longer the leader."
            ))
          } else if (isRunning.get) {
            future.complete(new LoadSummary(startTimeMs, endTimeMs, schedulerQueueTimeMs, numRecords, numBytes))
          } else {
            future.completeExceptionally(new RuntimeException("Coordinator loader is closed."))
          }
      }
    } catch {
      case ex: Throwable =>
        future.completeExceptionally(ex)
    }
  }

  /**
   * Closes the loader.
   */
  override def close(): Unit = {
    if (!isRunning.compareAndSet(true, false)) {
      warn("Coordinator loader is already shutting down.")
      return
    }
    scheduler.shutdown()
  }
}

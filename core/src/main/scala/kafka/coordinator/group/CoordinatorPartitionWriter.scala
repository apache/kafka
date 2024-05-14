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

import kafka.cluster.PartitionListener
import kafka.server.{ActionQueue, ReplicaManager, RequestLocal, defaultError, genericError}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, EndTransactionMarker, MemoryRecords, RecordBatch, TimestampType}
import org.apache.kafka.common.record.Record.EMPTY_HEADERS
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{BufferSupplier, Time}
import org.apache.kafka.coordinator.group.runtime.PartitionWriter
import org.apache.kafka.storage.internals.log.{AppendOrigin, VerificationGuard}

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.Map

/**
 * ListenerAdapter adapts the PartitionListener interface to the
 * PartitionWriter.Listener interface.
 */
private[group] class ListenerAdapter(
  val listener: PartitionWriter.Listener
) extends PartitionListener {
  override def onHighWatermarkUpdated(
    tp: TopicPartition,
    offset: Long
  ): Unit = {
    listener.onHighWatermarkUpdated(tp, offset)
  }

  override def equals(that: Any): Boolean = that match {
    case other: ListenerAdapter => listener.equals(other.listener)
    case _ => false
  }

  override def hashCode(): Int = {
    listener.hashCode()
  }

  override def toString: String = {
    s"ListenerAdapter(listener=$listener)"
  }
}

class CoordinatorPartitionWriter[T](
  replicaManager: ReplicaManager,
  serializer: PartitionWriter.Serializer[T],
  compressionType: CompressionType,
  time: Time
) extends PartitionWriter[T] {
  private val threadLocalBufferSupplier = ThreadLocal.withInitial(
    () => new BufferSupplier.GrowableBufferSupplier()
  )

  // We use an action queue which directly executes actions. This is possible
  // here because we don't hold any conflicting locks.
  private val directActionQueue = new ActionQueue {
    override def add(action: () => Unit): Unit = {
      action()
    }

    override def tryCompleteActions(): Unit = {}
  }

  /**
   * {@inheritDoc}
   */
  override def registerListener(
    tp: TopicPartition,
    listener: PartitionWriter.Listener
  ): Unit = {
    replicaManager.maybeAddListener(tp, new ListenerAdapter(listener))
  }

  /**
   * {@inheritDoc}
   */
  override def deregisterListener(
    tp: TopicPartition,
    listener: PartitionWriter.Listener
  ): Unit = {
    replicaManager.removeListener(tp, new ListenerAdapter(listener))
  }

  /**
   * {@inheritDoc}
   */
  override def append(
    tp: TopicPartition,
    producerId: Long,
    producerEpoch: Short,
    verificationGuard: VerificationGuard,
    records: util.List[T]
  ): Long = {
    if (records.isEmpty) throw new IllegalStateException("records must be non-empty.")

    replicaManager.getLogConfig(tp) match {
      case Some(logConfig) =>
        val magic = logConfig.recordVersion.value
        val maxBatchSize = logConfig.maxMessageSize
        val currentTimeMs = time.milliseconds()
        val bufferSupplier = threadLocalBufferSupplier.get()
        val buffer = bufferSupplier.get(math.min(16384, maxBatchSize))

        try {
          val recordsBuilder = MemoryRecords.builder(
            buffer,
            magic,
            compressionType,
            TimestampType.CREATE_TIME,
            0L,
            time.milliseconds(),
            producerId,
            producerEpoch,
            0,
            producerId != RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
          )

          records.forEach { record =>
            val keyBytes = serializer.serializeKey(record)
            val valueBytes = serializer.serializeValue(record)

            if (recordsBuilder.hasRoomFor(currentTimeMs, keyBytes, valueBytes, EMPTY_HEADERS)) recordsBuilder.append(
              currentTimeMs,
              keyBytes,
              valueBytes,
              EMPTY_HEADERS
            ) else throw new RecordTooLargeException(s"Message batch size is ${recordsBuilder.estimatedSizeInBytes()} bytes " +
              s"in append to partition $tp which exceeds the maximum configured size of $maxBatchSize.")
          }

          internalAppend(tp, recordsBuilder.build(), verificationGuard)
        } finally {
          bufferSupplier.release(buffer)
        }

      case None =>
        throw Errors.NOT_LEADER_OR_FOLLOWER.exception()
    }
  }

  /**
   * {@inheritDoc}
   */
  override def appendEndTransactionMarker(
    tp: TopicPartition,
    producerId: Long,
    producerEpoch: Short,
    coordinatorEpoch: Int,
    result: TransactionResult
  ): Long = {
    val controlRecordType = result match {
      case TransactionResult.COMMIT => ControlRecordType.COMMIT
      case TransactionResult.ABORT => ControlRecordType.ABORT
    }

    internalAppend(tp, MemoryRecords.withEndTransactionMarker(
      producerId,
      producerEpoch,
      new EndTransactionMarker(controlRecordType, coordinatorEpoch)
    ))
  }

  /**
   * {@inheritDoc}
   */
  override def maybeStartTransactionVerification(
    tp: TopicPartition,
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    apiVersion: Short
  ): CompletableFuture[VerificationGuard] = {
    val transactionSupportedOperation = if (apiVersion >= 4) genericError else defaultError
    val future = new CompletableFuture[VerificationGuard]()
    replicaManager.maybeStartTransactionVerificationForPartition(
      topicPartition = tp,
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      baseSequence = RecordBatch.NO_SEQUENCE,
      callback = errorAndGuard => {
        val (error, verificationGuard) = errorAndGuard
        if (error != Errors.NONE) {
          future.completeExceptionally(error.exception)
        } else {
          future.complete(verificationGuard)
        }
      },
      transactionSupportedOperation
    )
    future
  }

  private def internalAppend(
    tp: TopicPartition,
    memoryRecords: MemoryRecords,
    verificationGuard: VerificationGuard = VerificationGuard.SENTINEL
  ): Long = {
    var appendResults: Map[TopicPartition, PartitionResponse] = Map.empty
    replicaManager.appendRecords(
      timeout = 0L,
      requiredAcks = 1,
      internalTopicsAllowed = true,
      origin = AppendOrigin.COORDINATOR,
      entriesPerPartition = Map(tp -> memoryRecords),
      responseCallback = results => appendResults = results,
      requestLocal = RequestLocal.NoCaching,
      verificationGuards = Map(tp -> verificationGuard),
      delayedProduceLock = None,
      // We can directly complete the purgatories here because we don't hold
      // any conflicting locks.
      actionQueue = directActionQueue
    )

    val partitionResult = appendResults.getOrElse(tp,
      throw new IllegalStateException(s"Append status $appendResults should have partition $tp."))

    if (partitionResult.error != Errors.NONE) {
      throw partitionResult.error.exception()
    }

    // Required offset.
    partitionResult.lastOffset + 1
  }
}

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
import kafka.server.{ReplicaManager, RequestLocal}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, TimestampType}
import org.apache.kafka.common.record.Record.EMPTY_HEADERS
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.runtime.PartitionWriter
import org.apache.kafka.storage.internals.log.AppendOrigin

import java.nio.ByteBuffer
import java.util
import scala.collection.Map

private[group] class ListenerAdaptor(
  val listener: PartitionWriter.Listener
) extends PartitionListener {
  override def onHighWatermarkUpdated(
    tp: TopicPartition,
    offset: Long
  ): Unit = {
    listener.onHighWatermarkUpdated(tp, offset)
  }

  override def equals(that: Any): Boolean = that match {
    case other: ListenerAdaptor => listener.equals(other.listener)
    case _ => false
  }

  override def hashCode(): Int = {
    listener.hashCode()
  }

  override def toString: String = {
    s"ListenerAdaptor(listener=$listener)"
  }
}

class PartitionWriterImpl[T](
  replicaManager: ReplicaManager,
  serializer: PartitionWriter.Serializer[T],
  compressionType: CompressionType,
  time: Time
) extends PartitionWriter[T] {

  override def registerListener(
    tp: TopicPartition,
    listener: PartitionWriter.Listener
  ): Unit = {
    replicaManager.maybeAddListener(tp, new ListenerAdaptor(listener))
  }

  override def deregisterListener(
    tp: TopicPartition,
    listener: PartitionWriter.Listener
  ): Unit = {
    replicaManager.removeListener(tp, new ListenerAdaptor(listener))
  }

  override def append(
    tp: TopicPartition,
    records: util.List[T]
  ): Long = {
    if (records.isEmpty) {
      throw new IllegalStateException("records must be non-empty.")
    }

    replicaManager.getLogConfig(tp) match {
      case Some(logConfig) =>
        val magic = logConfig.recordVersion.value
        val maxBatchSize = logConfig.maxMessageSize
        val currentTimeMs = time.milliseconds()

        val recordsBuilder = MemoryRecords.builder(
          ByteBuffer.allocate(math.min(16384, maxBatchSize)),
          magic,
          compressionType,
          TimestampType.CREATE_TIME,
          0L,
          maxBatchSize
        )

        records.forEach { record =>
          val keyBytes = serializer.serializeKey(record)
          val valueBytes = serializer.serializeValue(record)

          if (recordsBuilder.hasRoomFor(currentTimeMs, keyBytes, valueBytes, EMPTY_HEADERS)) {
            recordsBuilder.append(
              currentTimeMs,
              keyBytes,
              valueBytes,
              EMPTY_HEADERS
            )
          } else {
            throw new RecordTooLargeException(s"Message batch size is ${recordsBuilder.estimatedSizeInBytes()} bytes " +
              s"in append to partition $tp which exceeds the maximum configured size of $maxBatchSize.")
          }
        }

        val appendResults = replicaManager.appendToLocalLog(
          internalTopicsAllowed = true,
          origin = AppendOrigin.COORDINATOR,
          entriesPerPartition = Map(tp -> recordsBuilder.build()),
          requiredAcks = 1,
          requestLocal = RequestLocal.NoCaching
        )

        val partitionResult = appendResults.getOrElse(tp,
          throw new IllegalStateException("Append status %s should only have one partition %s"
            .format(appendResults, tp)))

        // Complete delayed operations.
        replicaManager.maybeCompletePurgatories(
          tp,
          partitionResult.info.leaderHwChange
        )

        // Required offset.
        partitionResult.info.lastOffset + 1

      case None =>
        throw Errors.NOT_LEADER_OR_FOLLOWER.exception()
    }
  }
}

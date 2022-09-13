/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.metadata

import java.util.concurrent.RejectedExecutionException
import kafka.utils.Logging
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.metadata.util.SnapshotReason
import org.apache.kafka.queue.{EventQueue, KafkaEventQueue}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.snapshot.SnapshotWriter

import java.util.function.Consumer

trait SnapshotWriterBuilder {
  def build(committedOffset: Long,
            committedEpoch: Int,
            lastContainedLogTime: Long): Option[SnapshotWriter[ApiMessageAndVersion]]
}

/**
 * The RecordListConsumer takes as input a potentially long list of records, and feeds the
 * SnapshotWriter a series of smaller lists of records.
 *
 * Note: from the perspective of Kafka, the snapshot file is really just a list of records,
 * and we don't care about batches. Batching is irrelevant to the meaning of the snapshot.
 */
class RecordListConsumer(
  val maxRecordsInBatch: Int,
  val writer: SnapshotWriter[ApiMessageAndVersion]
) extends Consumer[java.util.List[ApiMessageAndVersion]] {
  override def accept(messages: java.util.List[ApiMessageAndVersion]): Unit = {
    var i = 0
    while (i < messages.size()) {
      writer.append(messages.subList(i, Math.min(i + maxRecordsInBatch, messages.size())));
      i += maxRecordsInBatch
    }
  }
}

class BrokerMetadataSnapshotter(
  brokerId: Int,
  val time: Time,
  threadNamePrefix: Option[String],
  writerBuilder: SnapshotWriterBuilder
) extends Logging with MetadataSnapshotter {
  /**
   * The maximum number of records we will put in each batch.
   *
   * From the perspective of the Raft layer, the limit on batch size is specified in terms of
   * bytes, not number of records. @See {@link KafkaRaftClient#MAX_BATCH_SIZE_BYTES} for details.
   * However, it's more convenient to limit the batch size here in terms of number of records.
   * So we chose a low number that will not cause problems.
   */
  private val maxRecordsInBatch = 1024

  private val logContext = new LogContext(s"[BrokerMetadataSnapshotter id=$brokerId] ")
  logIdent = logContext.logPrefix()

  /**
   * The offset of the snapshot in progress, or -1 if there isn't one. Accessed only under
   * the object lock.
   */
  private var _currentSnapshotOffset = -1L

  /**
   * The event queue which runs this listener.
   */
  val eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix.getOrElse(""))

  override def maybeStartSnapshot(lastContainedLogTime: Long, image: MetadataImage, snapshotReasons: Set[SnapshotReason]): Boolean = synchronized {
    if (_currentSnapshotOffset != -1) {
      info(s"Declining to create a new snapshot at ${image.highestOffsetAndEpoch()} because " +
        s"there is already a snapshot in progress at offset ${_currentSnapshotOffset}")
      false
    } else {
      val writer = writerBuilder.build(
        image.highestOffsetAndEpoch().offset,
        image.highestOffsetAndEpoch().epoch,
        lastContainedLogTime
      )
      if (writer.nonEmpty) {
        _currentSnapshotOffset = image.highestOffsetAndEpoch().offset

        info(s"Creating a new snapshot at offset ${_currentSnapshotOffset} because, ${snapshotReasons.mkString(" and ")}")
        eventQueue.append(new CreateSnapshotEvent(image, writer.get))
        true
      } else {
        info(s"Declining to create a new snapshot at ${image.highestOffsetAndEpoch()} because " +
          s"there is already a snapshot at offset ${image.highestOffsetAndEpoch().offset}")
        false
      }
    }
  }

  class CreateSnapshotEvent(image: MetadataImage,
                            writer: SnapshotWriter[ApiMessageAndVersion])
        extends EventQueue.Event {

    override def run(): Unit = {
      try {
        val consumer = new RecordListConsumer(maxRecordsInBatch, writer)
        image.write(consumer)
        writer.freeze()
      } finally {
        try {
          writer.close()
        } finally {
          BrokerMetadataSnapshotter.this.synchronized {
            _currentSnapshotOffset = -1L
          }
        }
      }
    }

    override def handleException(e: Throwable): Unit = {
      e match {
        case _: RejectedExecutionException =>
          info("Not processing CreateSnapshotEvent because the event queue is closed.")
        case _ => error("Unexpected error handling CreateSnapshotEvent", e)
      }
      writer.close()
    }
  }

  def beginShutdown(): Unit = {
    eventQueue.beginShutdown("beginShutdown", new ShutdownEvent())
  }

  class ShutdownEvent() extends EventQueue.Event {
    override def run(): Unit = {
    }
  }

  def close(): Unit = {
    beginShutdown()
    eventQueue.close()
  }
}

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

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Consumer

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.queue.{EventQueue, KafkaEventQueue}
import org.apache.kafka.raft.{Batch, BatchReader, LeaderAndEpoch, RaftClient}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.snapshot.SnapshotReader


object BrokerMetadataListener {
  val MetadataBatchProcessingTimeUs = "MetadataBatchProcessingTimeUs"
  val MetadataBatchSizes = "MetadataBatchSizes"
}

class BrokerMetadataListener(
  val brokerId: Int,
  time: Time,
  threadNamePrefix: Option[String],
  val maxBytesBetweenSnapshots: Long,
  val snapshotter: Option[MetadataSnapshotter]
) extends RaftClient.Listener[ApiMessageAndVersion] with KafkaMetricsGroup {
  private val logContext = new LogContext(s"[BrokerMetadataListener id=${brokerId}] ")
  private val log = logContext.logger(classOf[BrokerMetadataListener])
  logIdent = logContext.logPrefix()

  /**
   * A histogram tracking the time in microseconds it took to process batches of events.
   */
  private val batchProcessingTimeHist = newHistogram(BrokerMetadataListener.MetadataBatchProcessingTimeUs)

  /**
   * A histogram tracking the sizes of batches that we have processed.
   */
  private val metadataBatchSizeHist = newHistogram(BrokerMetadataListener.MetadataBatchSizes)

  /**
   * The highest metadata offset that we've seen.  Written only from the event queue thread.
   */
  @volatile var _highestMetadataOffset = -1L

  /**
   * The highest metadata log epoch that we've seen. Written only from the event queue thread.
   */
  private var _highestEpoch = -1

  /**
   * The highest metadata log time that we've seen. Written only from the event queue thread.
   */
  private var _highestTimestamp = -1L

  /**
   * The current broker metadata image. Accessed only from the event queue thread.
   */
  private var _image = MetadataImage.EMPTY

  /**
   * The current metadata delta. Accessed only from the event queue thread.
   */
  private var _delta = new MetadataDelta(_image)

  /**
   * The object to use to publish new metadata changes, or None if this listener has not
   * been activated yet. Accessed only from the event queue thread.
   */
  private var _publisher: Option[MetadataPublisher] = None

  /**
   * The number of bytes of records that we have read  since the last snapshot we took.
   * This does not include records we read from a snapshot.
   * Accessed only from the event queue thread.
   */
  private var _bytesSinceLastSnapshot: Long = 0L

  /**
   * The event queue which runs this listener.
   */
  val eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix.getOrElse(""))

  /**
   * Returns the highest metadata-offset. Thread-safe.
   */
  def highestMetadataOffset(): Long = _highestMetadataOffset

  /**
   * Handle new metadata records.
   */
  override def handleCommit(reader: BatchReader[ApiMessageAndVersion]): Unit =
    eventQueue.append(new HandleCommitsEvent(reader))

  class HandleCommitsEvent(reader: BatchReader[ApiMessageAndVersion])
      extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      val results = try {
        val loadResults = loadBatches(_delta, reader)
        if (isDebugEnabled) {
          debug(s"Loaded new commits: ${loadResults}")
        }
        loadResults
      } finally {
        reader.close()
      }
      _publisher.foreach(publish(_, results.highestMetadataOffset))

      snapshotter.foreach { snapshotter =>
        _bytesSinceLastSnapshot = _bytesSinceLastSnapshot + results.numBytes
        if (shouldSnapshot()) {
          if (snapshotter.maybeStartSnapshot(results.highestMetadataOffset,
            _highestEpoch,
            _highestTimestamp,
            _delta.apply())) {
            _bytesSinceLastSnapshot = 0L
          }
        }
      }
    }
  }

  private def shouldSnapshot(): Boolean = {
    _bytesSinceLastSnapshot >= maxBytesBetweenSnapshots
  }

  /**
   * Handle metadata snapshots
   */
  override def handleSnapshot(reader: SnapshotReader[ApiMessageAndVersion]): Unit =
    eventQueue.append(new HandleSnapshotEvent(reader))

  class HandleSnapshotEvent(reader: SnapshotReader[ApiMessageAndVersion])
    extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      val results = try {
        info(s"Loading snapshot ${reader.snapshotId().offset}-${reader.snapshotId().epoch}.")
        _delta = new MetadataDelta(_image) // Discard any previous deltas.
        val loadResults = loadBatches(_delta, reader)
        _delta.finishSnapshot()
        info(s"Loaded snapshot ${reader.snapshotId().offset}-${reader.snapshotId().epoch}: " +
          s"${loadResults}")
        loadResults
      } finally {
        reader.close()
      }
      _publisher.foreach(publish(_, results.highestMetadataOffset))
    }
  }

  case class BatchLoadResults(numBatches: Int,
                              numRecords: Int,
                              elapsedUs: Long,
                              numBytes: Long,
                              highestMetadataOffset: Long) {
    override def toString(): String = {
      s"${numBatches} batch(es) with ${numRecords} record(s) in ${numBytes} bytes " +
        s"ending at offset ${highestMetadataOffset} in ${elapsedUs} microseconds"
    }
  }

  private def loadBatches(delta: MetadataDelta,
                          iterator: util.Iterator[Batch[ApiMessageAndVersion]]): BatchLoadResults = {
    val startTimeNs = time.nanoseconds()
    var numBatches = 0
    var numRecords = 0
    var batch: Batch[ApiMessageAndVersion] = null
    var numBytes = 0L
    while (iterator.hasNext()) {
      batch = iterator.next()
      var index = 0
      batch.records().forEach { messageAndVersion =>
        if (isTraceEnabled) {
          trace("Metadata batch %d: processing [%d/%d]: %s.".format(batch.lastOffset, index + 1,
            batch.records().size(), messageAndVersion.message().toString()))
        }
        delta.replay(messageAndVersion.message())
        numRecords += 1
        index += 1
      }
      numBytes = numBytes + batch.sizeInBytes()
      metadataBatchSizeHist.update(batch.records().size())
      numBatches = numBatches + 1
    }
    val newHighestMetadataOffset = if (batch == null) {
      _highestMetadataOffset
    } else {
      _highestMetadataOffset = batch.lastOffset()
      _highestEpoch = batch.epoch()
      _highestTimestamp = batch.appendTimestamp()
      batch.lastOffset()
    }
    val endTimeNs = time.nanoseconds()
    val elapsedUs = TimeUnit.MICROSECONDS.convert(endTimeNs - startTimeNs, TimeUnit.NANOSECONDS)
    batchProcessingTimeHist.update(elapsedUs)
    BatchLoadResults(numBatches, numRecords, elapsedUs, numBytes, newHighestMetadataOffset)
  }

  def startPublishing(publisher: MetadataPublisher): CompletableFuture[Void] = {
    val event = new StartPublishingEvent(publisher)
    eventQueue.append(event)
    event.future
  }

  class StartPublishingEvent(publisher: MetadataPublisher)
      extends EventQueue.FailureLoggingEvent(log) {
    val future = new CompletableFuture[Void]()

    override def run(): Unit = {
      _publisher = Some(publisher)
      log.info(s"Starting to publish metadata events at offset ${_highestMetadataOffset}.")
      try {
        publish(publisher, _highestMetadataOffset)
        future.complete(null)
      } catch {
        case e: Throwable =>
          future.completeExceptionally(e)
          throw e
      }
    }
  }

  private def publish(publisher: MetadataPublisher,
                      newHighestMetadataOffset: Long): Unit = {
    val delta = _delta
    _image = _delta.apply()
    _delta = new MetadataDelta(_image)
    publisher.publish(newHighestMetadataOffset, delta, _image)
  }

  override def handleLeaderChange(leaderAndEpoch: LeaderAndEpoch): Unit = {
    // Nothing to do.
  }

  override def beginShutdown(): Unit = {
    eventQueue.beginShutdown("beginShutdown", new ShutdownEvent())
  }

  class ShutdownEvent() extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      removeMetric(BrokerMetadataListener.MetadataBatchProcessingTimeUs)
      removeMetric(BrokerMetadataListener.MetadataBatchSizes)
    }
  }

  def close(): Unit = {
    beginShutdown()
    eventQueue.close()
  }

  // VisibleForTesting
  private[kafka] def getImageRecords(): CompletableFuture[util.List[ApiMessageAndVersion]] = {
    val future = new CompletableFuture[util.List[ApiMessageAndVersion]]()
    eventQueue.append(new GetImageRecordsEvent(future))
    future
  }

  class GetImageRecordsEvent(future: CompletableFuture[util.List[ApiMessageAndVersion]])
      extends EventQueue.FailureLoggingEvent(log) with Consumer[util.List[ApiMessageAndVersion]] {
    val records = new util.ArrayList[ApiMessageAndVersion]()
    override def accept(batch: util.List[ApiMessageAndVersion]): Unit = {
      records.addAll(batch)
    }

    override def run(): Unit = {
      _image.write(this)
      future.complete(records)
    }
  }
}

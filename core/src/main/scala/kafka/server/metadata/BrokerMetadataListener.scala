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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CompletableFuture
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.image.writer.{ImageWriterOptions, RecordListWriter}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.util.SnapshotReason
import org.apache.kafka.queue.{EventQueue, KafkaEventQueue}
import org.apache.kafka.raft.{Batch, BatchReader, LeaderAndEpoch, RaftClient}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.fault.FaultHandler
import org.apache.kafka.snapshot.SnapshotReader

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.compat.java8.OptionConverters._


class BrokerMetadataListener(
  val brokerId: Int,
  time: Time,
  threadNamePrefix: Option[String],
  val maxBytesBetweenSnapshots: Long,
  val snapshotter: Option[MetadataSnapshotter],
  brokerMetrics: BrokerServerMetrics,
  _metadataLoadingFaultHandler: FaultHandler
) extends RaftClient.Listener[ApiMessageAndVersion] with KafkaMetricsGroup {

  private val metadataFaultOccurred = new AtomicBoolean(false)
  private val metadataLoadingFaultHandler: FaultHandler = new FaultHandler() {
    override def handleFault(failureMessage: String, cause: Throwable): RuntimeException = {
      // If the broker has any kind of error handling metadata records or publishing a new image
      // we will disable taking new snapshots in order to preserve the local metadata log. Once we
      // encounter a metadata processing error, the broker will be in an undetermined state.
      if (metadataFaultOccurred.compareAndSet(false, true)) {
        error("Disabling metadata snapshots until this broker is restarted.")
      }
      _metadataLoadingFaultHandler.handleFault(failureMessage, cause)
    }
  }

  private val logContext = new LogContext(s"[BrokerMetadataListener id=$brokerId] ")
  private val log = logContext.logger(classOf[BrokerMetadataListener])
  logIdent = logContext.logPrefix()

  /**
   * The highest metadata offset that we've seen.  Written only from the event queue thread.
   */
  @volatile var _highestOffset = -1L

  /**
   * The highest metadata epoch that we've seen.  Written only from the event queue thread.
   */
  private var _highestEpoch = -1

  /**
   * The highest metadata log time that we've seen. Written only from the event queue thread.
   */
  private var _highestTimestamp = -1L

  private def provenance(): MetadataProvenance =
    new MetadataProvenance(_highestOffset, _highestEpoch, _highestTimestamp)

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
  def highestMetadataOffset: Long = _highestOffset

  /**
   * Handle new metadata records.
   */
  override def handleCommit(reader: BatchReader[ApiMessageAndVersion]): Unit =
    eventQueue.append(new HandleCommitsEvent(reader))

  class HandleCommitsEvent(reader: BatchReader[ApiMessageAndVersion])
      extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      val results = try {
        val loadResults = loadBatches(_delta, reader, None, None, None, None)
        if (isDebugEnabled) {
          debug(s"Loaded new commits: $loadResults")
        }
        loadResults
      } catch {
        case e: Throwable =>
          metadataLoadingFaultHandler.handleFault(s"Unable to load metadata commits " +
            s"from the BatchReader starting at base offset ${reader.baseOffset()}", e)
          return
      } finally {
        reader.close()
      }

      _bytesSinceLastSnapshot = _bytesSinceLastSnapshot + results.numBytes
      
      val shouldTakeSnapshot: Set[SnapshotReason] = shouldSnapshot()
      if (shouldTakeSnapshot.nonEmpty) {
        maybeStartSnapshot(shouldTakeSnapshot)
      }

      _publisher.foreach(publish)
    }
  }

  private def shouldSnapshot(): Set[SnapshotReason] = {
    val maybeMetadataVersionChanged = metadataVersionChanged.toSet

    if (_bytesSinceLastSnapshot >= maxBytesBetweenSnapshots) {
      maybeMetadataVersionChanged + SnapshotReason.maxBytesExceeded(_bytesSinceLastSnapshot, maxBytesBetweenSnapshots)
    } else {
      maybeMetadataVersionChanged
    }
  }

  private def metadataVersionChanged: Option[SnapshotReason] = {
    // The _publisher is empty before starting publishing, and we won't compute feature delta
    // until we starting publishing
    if (_publisher.nonEmpty) {
      Option(_delta.featuresDelta()).flatMap { featuresDelta =>
        featuresDelta
          .metadataVersionChange()
          .asScala
          .map(SnapshotReason.metadataVersionChanged)
      }
    } else {
      None
    }
  }

  private def maybeStartSnapshot(reason: Set[SnapshotReason]): Unit = {
    snapshotter.foreach { snapshotter =>
      if (metadataFaultOccurred.get()) {
        trace("Not starting metadata snapshot since we previously had an error")
      } else if (snapshotter.maybeStartSnapshot(_highestTimestamp, _delta.apply(provenance()), reason)) {
        _bytesSinceLastSnapshot = 0L
      }
    }
  }

  /**
   * Handle metadata snapshots
   */
  override def handleSnapshot(reader: SnapshotReader[ApiMessageAndVersion]): Unit =
    eventQueue.append(new HandleSnapshotEvent(reader))

  class HandleSnapshotEvent(reader: SnapshotReader[ApiMessageAndVersion])
    extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      val snapshotName = s"${reader.snapshotId().offset}-${reader.snapshotId().epoch}"
      try {
        info(s"Loading snapshot ${snapshotName}")
        _delta = new MetadataDelta(_image) // Discard any previous deltas.
        val loadResults = loadBatches(_delta,
          reader,
          Some(reader.lastContainedLogTimestamp),
          Some(reader.lastContainedLogOffset),
          Some(reader.lastContainedLogEpoch),
          Some(snapshotName))
        try {
          _delta.finishSnapshot()
        } catch {
          case e: Throwable => metadataLoadingFaultHandler.handleFault(
              s"Error finishing snapshot ${snapshotName}", e)
        }
        info(s"Loaded snapshot ${snapshotName}: ${loadResults}")
      } catch {
        case t: Throwable => metadataLoadingFaultHandler.handleFault("Uncaught exception while " +
          s"loading broker metadata from Metadata snapshot ${snapshotName}", t)
      } finally {
        reader.close()
      }
      _publisher.foreach(publish)
    }
  }

  case class BatchLoadResults(numBatches: Int, numRecords: Int, elapsedUs: Long, numBytes: Long) {
    override def toString: String = {
      s"$numBatches batch(es) with $numRecords record(s) in $numBytes bytes " +
        s"ending at offset $highestMetadataOffset in $elapsedUs microseconds"
    }
  }

  /**
   * Load and replay the batches to the metadata delta.
   *
   * When loading and replay a snapshot the appendTimestamp and snapshotId parameter should be provided.
   * In a snapshot the append timestamp, offset and epoch reported by the batch is independent of the ones
   * reported by the metadata log.
   *
   * @param delta metadata delta on which to replay the records
   * @param iterator sequence of metadata record bacthes to replay
   * @param lastAppendTimestamp optional append timestamp to use instead of the batches timestamp
   * @param lastCommittedOffset optional offset to use instead of the batches offset
   * @param lastCommittedEpoch optional epoch to use instead of the batches epoch
   */
  private def loadBatches(
    delta: MetadataDelta,
    iterator: util.Iterator[Batch[ApiMessageAndVersion]],
    lastAppendTimestamp: Option[Long],
    lastCommittedOffset: Option[Long],
    lastCommittedEpoch: Option[Int],
    snapshotName: Option[String]
  ): BatchLoadResults = {
    val startTimeNs = time.nanoseconds()
    var numBatches = 0
    var numRecords = 0
    var numBytes = 0L

    while (iterator.hasNext) {
      val batch = iterator.next()

      _highestEpoch = lastCommittedEpoch.getOrElse(batch.epoch())
      _highestTimestamp = lastAppendTimestamp.getOrElse(batch.appendTimestamp())

      var index = 0
      batch.records().forEach { messageAndVersion =>
        if (isTraceEnabled) {
          trace(s"Metadata batch ${batch.lastOffset}: processing [${index + 1}/${batch.records.size}]:" +
            s" ${messageAndVersion.message}")
        }
        _highestOffset = lastCommittedOffset.getOrElse(batch.baseOffset() + index)
        try {
          delta.replay(messageAndVersion.message())
        } catch {
          case e: Throwable => snapshotName match {
            case None => metadataLoadingFaultHandler.handleFault(
              s"Error replaying metadata log record at offset ${_highestOffset}", e)
            case Some(name) => metadataLoadingFaultHandler.handleFault(
              s"Error replaying record ${index} from snapshot ${name} at offset ${_highestOffset}", e)
          }
        } finally {
          numRecords += 1
          index += 1
        }
      }
      numBytes = numBytes + batch.sizeInBytes()
      brokerMetrics.updateBatchSize(batch.records().size())
      numBatches = numBatches + 1
    }

    val endTimeNs = time.nanoseconds()
    val elapsedNs = endTimeNs - startTimeNs
    brokerMetrics.updateBatchProcessingTime(elapsedNs)
    BatchLoadResults(numBatches, numRecords, NANOSECONDS.toMicros(elapsedNs), numBytes)
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
      log.info(s"Starting to publish metadata events at offset $highestMetadataOffset.")
      try {
        // Generate a snapshot if the metadata version changed
        metadataVersionChanged.foreach(reason => maybeStartSnapshot(Set(reason)))
        publish(publisher)
        future.complete(null)
      } catch {
        case e: Throwable =>
          future.completeExceptionally(e)
          throw e
      }
    }
  }

  // This is used in tests to alter the publisher that is in use by the broker.
  def alterPublisher(publisher: MetadataPublisher): CompletableFuture[Void] = {
    val event = new AlterPublisherEvent(publisher)
    eventQueue.append(event)
    event.future
  }

  class AlterPublisherEvent(publisher: MetadataPublisher)
    extends EventQueue.FailureLoggingEvent(log) {
    val future = new CompletableFuture[Void]()

    override def run(): Unit = {
      _publisher = Some(publisher)
      log.info(s"Set publisher to ${publisher}")
      future.complete(null)
    }
  }

  private def publish(publisher: MetadataPublisher): Unit = {
    val delta = _delta
    try {
      _image = _delta.apply(provenance())
    } catch {
      case t: Throwable =>
        // If we cannot apply the delta, this publish event will throw and we will not publish a new image.
        // The broker will continue applying metadata records and attempt to publish again.
        throw metadataLoadingFaultHandler.handleFault(s"Error applying metadata delta $delta", t)
    }

    _delta = new MetadataDelta(_image)
    if (isDebugEnabled) {
      debug(s"Publishing new metadata delta $delta at offset ${_image.highestOffsetAndEpoch().offset}.")
    }

    // This publish call is done with its own try-catch and fault handler
    publisher.publish(delta, _image)

    // Update the metrics since the publisher handled the lastest image
    brokerMetrics.updateLastAppliedImageProvenance(_image.provenance())
  }

  override def handleLeaderChange(leaderAndEpoch: LeaderAndEpoch): Unit = {
    // Nothing to do.
  }

  override def beginShutdown(): Unit = {
    eventQueue.beginShutdown("beginShutdown")
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
      extends EventQueue.FailureLoggingEvent(log) {
    override def run(): Unit = {
      val writer = new RecordListWriter()
      val options = new ImageWriterOptions.Builder().
        setMetadataVersion(_image.features().metadataVersion()).
        build()
      try {
        _image.write(writer, options)
      } finally {
        writer.close()
      }
      future.complete(writer.records())
    }
  }
}

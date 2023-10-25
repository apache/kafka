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
package kafka.coordinator.transaction

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.server.{Defaults, ReplicaManager, RequestLocal}
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.{Logging, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.ListTransactionsResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.{Avg, Max}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, MemoryRecordsBuilder, Record, SimpleRecord, TimestampType}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchIsolation}

import scala.jdk.CollectionConverters._
import scala.collection.mutable


object TransactionStateManager {
  // default transaction management config values
  val DefaultTransactionsMaxTimeoutMs: Int = TimeUnit.MINUTES.toMillis(15).toInt
  val DefaultTransactionalIdExpirationMs: Int = TimeUnit.DAYS.toMillis(7).toInt
  val DefaultAbortTimedOutTransactionsIntervalMs: Int = TimeUnit.SECONDS.toMillis(10).toInt
  val DefaultRemoveExpiredTransactionalIdsIntervalMs: Int = TimeUnit.HOURS.toMillis(1).toInt

  val MetricsGroup: String = "transaction-coordinator-metrics"
  val LoadTimeSensor: String = "TransactionsPartitionLoadTime"
}

/**
 * Transaction state manager is part of the transaction coordinator, it manages:
 *
 * 1. the transaction log, which is a special internal topic.
 * 2. the transaction metadata including its ongoing transaction status.
 * 3. the background expiration of the transaction as well as the transactional id.
 *
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in TransactionStateManager use individual operation locks.
 * Delayed callbacks may acquire `stateLock.readLock` or any of the `txnMetadata` locks,
 * but we always require that `stateLock.readLock` be acquired first. In particular:
 * <ul>
 * <li>`stateLock.readLock` must never be acquired while holding `txnMetadata` lock.</li>
 * <li>`txnMetadata` lock must never be acquired while holding `stateLock.writeLock`.</li>
 * <li>`ReplicaManager.appendRecords` should never be invoked while holding a `txnMetadata` lock.</li>
 * </ul>
 */
class TransactionStateManager(brokerId: Int,
                              scheduler: Scheduler,
                              replicaManager: ReplicaManager,
                              config: TransactionConfig,
                              time: Time,
                              metrics: Metrics) extends Logging {

  this.logIdent = "[Transaction State Manager " + brokerId + "]: "

  type SendTxnMarkersCallback = (Int, TransactionResult, TransactionMetadata, TxnTransitMetadata) => Unit

  /** shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /** lock protecting access to the transactional metadata cache, including loading and leaving partition sets */
  private val stateLock = new ReentrantReadWriteLock()

  /** partitions of transaction topic that are being loaded, state lock should be called BEFORE accessing this set */
  private[transaction] val loadingPartitions: mutable.Set[TransactionPartitionAndLeaderEpoch] = mutable.Set()

  /** transaction metadata cache indexed by assigned transaction topic partition ids */
  private[transaction] val transactionMetadataCache: mutable.Map[Int, TxnMetadataCacheEntry] = mutable.Map()

  /** number of partitions for the transaction log topic */
  private var retrieveTransactionTopicPartitionCount: () => Int = _
  @volatile private var transactionTopicPartitionCount: Int = _

  /** setup metrics*/
  private val partitionLoadSensor = metrics.sensor(TransactionStateManager.LoadTimeSensor)

  partitionLoadSensor.add(metrics.metricName("partition-load-time-max",
    TransactionStateManager.MetricsGroup,
    "The max time it took to load the partitions in the last 30sec"), new Max())
  partitionLoadSensor.add(metrics.metricName("partition-load-time-avg",
    TransactionStateManager.MetricsGroup,
    "The avg time it took to load the partitions in the last 30sec"), new Avg())

  // visible for testing only
  private[transaction] def addLoadingPartition(partitionId: Int, coordinatorEpoch: Int): Unit = {
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)
    inWriteLock(stateLock) {
      loadingPartitions.add(partitionAndLeaderEpoch)
    }
  }

  // this is best-effort expiration of an ongoing transaction which has been open for more than its
  // txn timeout value, we do not need to grab the lock on the metadata object upon checking its state
  // since the timestamp is volatile and we will get the lock when actually trying to transit the transaction
  // metadata to abort later.
  def timedOutTransactions(): Iterable[TransactionalIdAndProducerIdEpoch] = {
    val now = time.milliseconds()
    inReadLock(stateLock) {
      transactionMetadataCache.flatMap { case (_, entry) =>
        entry.metadataPerTransactionalId.filter { case (_, txnMetadata) =>
          if (txnMetadata.pendingTransitionInProgress) {
            false
          } else {
            txnMetadata.state match {
              case Ongoing =>
                txnMetadata.txnStartTimestamp + txnMetadata.txnTimeoutMs < now
              case _ => false
            }
          }
        }.map { case (txnId, txnMetadata) =>
          TransactionalIdAndProducerIdEpoch(txnId, txnMetadata.producerId, txnMetadata.producerEpoch)
        }
      }
    }
  }

  private def removeExpiredTransactionalIds(
    transactionPartition: TopicPartition,
    txnMetadataCacheEntry: TxnMetadataCacheEntry,
  ): Unit = {
    inReadLock(stateLock) {
      replicaManager.getLogConfig(transactionPartition) match {
        case Some(logConfig) =>
          val currentTimeMs = time.milliseconds()
          val maxBatchSize = logConfig.maxMessageSize
          val expired = mutable.ListBuffer.empty[TransactionalIdCoordinatorEpochAndMetadata]
          var recordsBuilder: MemoryRecordsBuilder = null
          val stateEntries = txnMetadataCacheEntry.metadataPerTransactionalId.values.iterator.buffered

          def flushRecordsBuilder(): Unit = {
            writeTombstonesForExpiredTransactionalIds(
              transactionPartition,
              expired.toSeq,
              recordsBuilder.build()
            )
            expired.clear()
            recordsBuilder = null
          }

          while (stateEntries.hasNext) {
            val txnMetadata = stateEntries.head
            val transactionalId = txnMetadata.transactionalId
            var fullBatch = false

            txnMetadata.inLock {
              if (txnMetadata.pendingState.isEmpty && shouldExpire(txnMetadata, currentTimeMs)) {
                if (recordsBuilder == null) {
                  recordsBuilder = MemoryRecords.builder(
                    ByteBuffer.allocate(math.min(16384, maxBatchSize)),
                    TransactionLog.EnforcedCompressionType,
                    TimestampType.CREATE_TIME,
                    0L,
                    maxBatchSize
                  )
                }

                if (maybeAppendExpiration(txnMetadata, recordsBuilder, currentTimeMs)) {
                  val transitMetadata = txnMetadata.prepareDead()
                  expired += TransactionalIdCoordinatorEpochAndMetadata(
                    transactionalId,
                    txnMetadataCacheEntry.coordinatorEpoch,
                    transitMetadata
                  )
                } else {
                  fullBatch = true
                }
              }
            }

            if (fullBatch) {
              flushRecordsBuilder()
            } else {
              // Advance the iterator if we do not need to retry the append
              stateEntries.next()
            }
          }

          if (expired.nonEmpty) {
            flushRecordsBuilder()
          }

        case None =>
          warn(s"Transaction expiration for partition $transactionPartition failed because the log " +
            "config was not available, which likely means the partition is not online or is no longer local.")
      }
    }
  }

  private def shouldExpire(
    txnMetadata: TransactionMetadata,
    currentTimeMs: Long
  ): Boolean = {
    txnMetadata.state.isExpirationAllowed &&
      txnMetadata.txnLastUpdateTimestamp <= currentTimeMs - config.transactionalIdExpirationMs
  }

  private def maybeAppendExpiration(
    txnMetadata: TransactionMetadata,
    recordsBuilder: MemoryRecordsBuilder,
    currentTimeMs: Long,
  ): Boolean = {
    val keyBytes = TransactionLog.keyToBytes(txnMetadata.transactionalId)
    if (recordsBuilder.hasRoomFor(currentTimeMs, keyBytes, null, Record.EMPTY_HEADERS)) {
      recordsBuilder.append(currentTimeMs, keyBytes, null, Record.EMPTY_HEADERS)
      true
    } else {
      false
    }
  }

  private[transaction] def removeExpiredTransactionalIds(): Unit = {
    inReadLock(stateLock) {
      transactionMetadataCache.forKeyValue { (partitionId, partitionCacheEntry) =>
        val transactionPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
        removeExpiredTransactionalIds(transactionPartition, partitionCacheEntry)
      }
    }
  }

  private def writeTombstonesForExpiredTransactionalIds(
    transactionPartition: TopicPartition,
    expiredForPartition: Iterable[TransactionalIdCoordinatorEpochAndMetadata],
    tombstoneRecords: MemoryRecords
  ): Unit = {
    def removeFromCacheCallback(responses: collection.Map[TopicPartition, PartitionResponse]): Unit = {
      responses.forKeyValue { (topicPartition, response) =>
        inReadLock(stateLock) {
          transactionMetadataCache.get(topicPartition.partition).foreach { txnMetadataCacheEntry =>
            expiredForPartition.foreach { idCoordinatorEpochAndMetadata =>
              val transactionalId = idCoordinatorEpochAndMetadata.transactionalId
              val txnMetadata = txnMetadataCacheEntry.metadataPerTransactionalId.get(transactionalId)
              txnMetadata.inLock {
                if (txnMetadataCacheEntry.coordinatorEpoch == idCoordinatorEpochAndMetadata.coordinatorEpoch
                  && txnMetadata.pendingState.contains(Dead)
                  && txnMetadata.producerEpoch == idCoordinatorEpochAndMetadata.transitMetadata.producerEpoch
                  && response.error == Errors.NONE) {
                  txnMetadataCacheEntry.metadataPerTransactionalId.remove(transactionalId)
                } else {
                  warn(s"Failed to remove expired transactionalId: $transactionalId" +
                    s" from cache. Tombstone append error code: ${response.error}," +
                    s" pendingState: ${txnMetadata.pendingState}, producerEpoch: ${txnMetadata.producerEpoch}," +
                    s" expected producerEpoch: ${idCoordinatorEpochAndMetadata.transitMetadata.producerEpoch}," +
                    s" coordinatorEpoch: ${txnMetadataCacheEntry.coordinatorEpoch}, expected coordinatorEpoch: " +
                    s"${idCoordinatorEpochAndMetadata.coordinatorEpoch}")
                  txnMetadata.pendingState = None
                }
              }
            }
          }
        }
      }
    }

    inReadLock(stateLock) {
      replicaManager.appendRecords(
        config.requestTimeoutMs,
        TransactionLog.EnforcedRequiredAcks,
        internalTopicsAllowed = true,
        origin = AppendOrigin.COORDINATOR,
        entriesPerPartition = Map(transactionPartition -> tombstoneRecords),
        removeFromCacheCallback,
        requestLocal = RequestLocal.NoCaching)
    }
  }

  def enableTransactionalIdExpiration(): Unit = {
    scheduler.schedule(
      "transactionalId-expiration",
      () => removeExpiredTransactionalIds(),
      config.removeExpiredTransactionalIdsIntervalMs,
      config.removeExpiredTransactionalIdsIntervalMs
    )
  }

  def getTransactionState(transactionalId: String): Either[Errors, Option[CoordinatorEpochAndTxnMetadata]] = {
    getAndMaybeAddTransactionState(transactionalId, None)
  }

  def putTransactionStateIfNotExists(txnMetadata: TransactionMetadata): Either[Errors, CoordinatorEpochAndTxnMetadata] = {
    getAndMaybeAddTransactionState(txnMetadata.transactionalId, Some(txnMetadata)).map(_.getOrElse(
      throw new IllegalStateException(s"Unexpected empty transaction metadata returned while putting $txnMetadata")))
  }

  def listTransactionStates(
    filterProducerIds: Set[Long],
    filterStateNames: Set[String]
  ): ListTransactionsResponseData = {
    inReadLock(stateLock) {
      val response = new ListTransactionsResponseData()
      if (loadingPartitions.nonEmpty) {
        response.setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
      } else {
        val filterStates = mutable.Set.empty[TransactionState]
        filterStateNames.foreach { stateName =>
          TransactionState.fromName(stateName) match {
            case Some(state) => filterStates += state
            case None => response.unknownStateFilters.add(stateName)
          }
        }

        def shouldInclude(txnMetadata: TransactionMetadata): Boolean = {
          if (txnMetadata.state == Dead) {
            // We filter the `Dead` state since it is a transient state which
            // indicates that the transactionalId and its metadata are in the
            // process of expiration and removal.
            false
          } else if (filterProducerIds.nonEmpty && !filterProducerIds.contains(txnMetadata.producerId)) {
            false
          } else if (filterStateNames.nonEmpty && !filterStates.contains(txnMetadata.state)) {
            false
          } else {
            true
          }
        }

        val states = new java.util.ArrayList[ListTransactionsResponseData.TransactionState]
        transactionMetadataCache.forKeyValue { (_, cache) =>
          cache.metadataPerTransactionalId.values.foreach { txnMetadata =>
            txnMetadata.inLock {
              if (shouldInclude(txnMetadata)) {
                states.add(new ListTransactionsResponseData.TransactionState()
                  .setTransactionalId(txnMetadata.transactionalId)
                  .setProducerId(txnMetadata.producerId)
                  .setTransactionState(txnMetadata.state.name)
                )
              }
            }
          }
        }
        response.setErrorCode(Errors.NONE.code)
          .setTransactionStates(states)
      }
    }
  }

  /**
   * Get the transaction metadata associated with the given transactional id, or an error if
   * the coordinator does not own the transaction partition or is still loading it; if not found
   * either return None or create a new metadata and added to the cache
   *
   * This function is covered by the state read lock
   */
  private def getAndMaybeAddTransactionState(transactionalId: String,
                                             createdTxnMetadataOpt: Option[TransactionMetadata]): Either[Errors, Option[CoordinatorEpochAndTxnMetadata]] = {
    inReadLock(stateLock) {
      val partitionId = partitionFor(transactionalId)
      if (loadingPartitions.exists(_.txnPartitionId == partitionId))
        Left(Errors.COORDINATOR_LOAD_IN_PROGRESS)
      else {
        transactionMetadataCache.get(partitionId) match {
          case Some(cacheEntry) =>
            val txnMetadata = Option(cacheEntry.metadataPerTransactionalId.get(transactionalId)).orElse {
              createdTxnMetadataOpt.map { createdTxnMetadata =>
                Option(cacheEntry.metadataPerTransactionalId.putIfNotExists(transactionalId, createdTxnMetadata))
                  .getOrElse(createdTxnMetadata)
              }
            }
            Right(txnMetadata.map(CoordinatorEpochAndTxnMetadata(cacheEntry.coordinatorEpoch, _)))

          case None =>
            Left(Errors.NOT_COORDINATOR)
        }
      }
    }
  }

  /**
   * Validate the given transaction timeout value
   */
  def validateTransactionTimeoutMs(txnTimeoutMs: Int): Boolean =
    txnTimeoutMs <= config.transactionMaxTimeoutMs && txnTimeoutMs > 0

  def transactionTopicConfigs: Properties = {
    val props = new Properties

    // enforce disabled unclean leader election, no compression types, and compact cleanup policy
    props.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
    props.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.UNCOMPRESSED.name)
    props.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    props.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, config.transactionLogMinInsyncReplicas.toString)
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, config.transactionLogSegmentBytes.toString)

    props
  }

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  private def loadTransactionMetadata(topicPartition: TopicPartition, coordinatorEpoch: Int): Pool[String, TransactionMetadata] =  {
    def logEndOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)

    val loadedTransactions = new Pool[String, TransactionMetadata]

    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load transaction metadata from $topicPartition, but found no log")

      case Some(log) =>
        // buffer may not be needed if records are read from memory
        var buffer = ByteBuffer.allocate(0)

        // loop breaks if leader changes at any time during the load, since logEndOffset is -1
        var currOffset = log.logStartOffset

        // loop breaks if no records have been read, since the end of the log has been reached
        var readAtLeastOneRecord = true

        try {
          while (currOffset < logEndOffset && readAtLeastOneRecord && !shuttingDown.get() && inReadLock(stateLock) {
            loadingPartitions.exists { idAndEpoch: TransactionPartitionAndLeaderEpoch =>
              idAndEpoch.txnPartitionId == topicPartition.partition && idAndEpoch.coordinatorEpoch == coordinatorEpoch}}) {
            val fetchDataInfo = log.read(currOffset,
              maxLength = config.transactionLogLoadBufferSize,
              isolation = FetchIsolation.LOG_END,
              minOneMessage = true)

            readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes > 0

            val memRecords = (fetchDataInfo.records: @unchecked) match {
              case records: MemoryRecords => records
              case fileRecords: FileRecords =>
                val sizeInBytes = fileRecords.sizeInBytes
                val bytesNeeded = Math.max(config.transactionLogLoadBufferSize, sizeInBytes)

                // minOneMessage = true in the above log.read means that the buffer may need to be grown to ensure progress can be made
                if (buffer.capacity < bytesNeeded) {
                  if (config.transactionLogLoadBufferSize < bytesNeeded)
                    warn(s"Loaded transaction metadata from $topicPartition with buffer larger ($bytesNeeded bytes) than " +
                      s"configured transaction.state.log.load.buffer.size (${config.transactionLogLoadBufferSize} bytes)")

                  buffer = ByteBuffer.allocate(bytesNeeded)
                } else {
                  buffer.clear()
                }
                buffer.clear()
                fileRecords.readInto(buffer, 0)
                MemoryRecords.readableRecords(buffer)
            }

            memRecords.batches.forEach { batch =>
              for (record <- batch.asScala) {
                require(record.hasKey, "Transaction state log's key should not be null")
                TransactionLog.readTxnRecordKey(record.key) match {
                  case txnKey: TxnKey =>
                    // load transaction metadata along with transaction state
                    val transactionalId = txnKey.transactionalId
                    TransactionLog.readTxnRecordValue(transactionalId, record.value) match {
                      case None =>
                        loadedTransactions.remove(transactionalId)
                      case Some(txnMetadata) =>
                        loadedTransactions.put(transactionalId, txnMetadata)
                    }
                    currOffset = batch.nextOffset

                  case unknownKey: UnknownKey =>
                    warn(s"Unknown message key with version ${unknownKey.version}" +
                      s" while loading transaction state from $topicPartition. Ignoring it. " +
                      "It could be a left over from an aborted upgrade.")
                }
              }
            }
          }
        } catch {
          case t: Throwable => error(s"Error loading transactions from transaction log $topicPartition", t)
        }
    }

    loadedTransactions
  }

  /**
   * Add a transaction topic partition into the cache
   */
  private[transaction] def addLoadedTransactionsToCache(txnTopicPartition: Int,
                                                        coordinatorEpoch: Int,
                                                        loadedTransactions: Pool[String, TransactionMetadata]): Unit = {
    val txnMetadataCacheEntry = TxnMetadataCacheEntry(coordinatorEpoch, loadedTransactions)
    val previousTxnMetadataCacheEntryOpt = transactionMetadataCache.put(txnTopicPartition, txnMetadataCacheEntry)

    previousTxnMetadataCacheEntryOpt.foreach { previousTxnMetadataCacheEntry =>
      warn(s"Unloaded transaction metadata $previousTxnMetadataCacheEntry from $txnTopicPartition as part of " +
        s"loading metadata at epoch $coordinatorEpoch")
    }
  }

  /**
   * When this broker becomes a leader for a transaction log partition, load this partition and populate the transaction
   * metadata cache with the transactional ids. This operation must be resilient to any partial state left off from
   * the previous loading / unloading operation.
   */
  def loadTransactionsForTxnTopicPartition(partitionId: Int, coordinatorEpoch: Int, sendTxnMarkers: SendTxnMarkersCallback): Unit = {
    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      loadingPartitions.add(partitionAndLeaderEpoch)
    }

    def loadTransactions(startTimeMs: java.lang.Long): Unit = {
      val schedulerTimeMs = time.milliseconds() - startTimeMs
      info(s"Loading transaction metadata from $topicPartition at epoch $coordinatorEpoch")
      validateTransactionTopicPartitionCountIsStable()

      val loadedTransactions = loadTransactionMetadata(topicPartition, coordinatorEpoch)
      val endTimeMs = time.milliseconds()
      val totalLoadingTimeMs = endTimeMs - startTimeMs
      partitionLoadSensor.record(totalLoadingTimeMs.toDouble, endTimeMs, false)
      info(s"Finished loading ${loadedTransactions.size} transaction metadata from $topicPartition in " +
        s"$totalLoadingTimeMs milliseconds, of which $schedulerTimeMs milliseconds was spent in the scheduler.")

      inWriteLock(stateLock) {
        if (loadingPartitions.contains(partitionAndLeaderEpoch)) {
          addLoadedTransactionsToCache(topicPartition.partition, coordinatorEpoch, loadedTransactions)

          val transactionsPendingForCompletion = new mutable.ListBuffer[TransactionalIdCoordinatorEpochAndTransitMetadata]
          loadedTransactions.foreach {
            case (transactionalId, txnMetadata) =>
              txnMetadata.inLock {
                // if state is PrepareCommit or PrepareAbort we need to complete the transaction
                txnMetadata.state match {
                  case PrepareAbort =>
                    transactionsPendingForCompletion +=
                      TransactionalIdCoordinatorEpochAndTransitMetadata(transactionalId, coordinatorEpoch, TransactionResult.ABORT, txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                  case PrepareCommit =>
                    transactionsPendingForCompletion +=
                      TransactionalIdCoordinatorEpochAndTransitMetadata(transactionalId, coordinatorEpoch, TransactionResult.COMMIT, txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                  case _ =>
                    // nothing needs to be done
                }
              }
          }

          // we first remove the partition from loading partition then send out the markers for those pending to be
          // completed transactions, so that when the markers get sent the attempt of appending the complete transaction
          // log would not be blocked by the coordinator loading error
          loadingPartitions.remove(partitionAndLeaderEpoch)

          transactionsPendingForCompletion.foreach { txnTransitMetadata =>
            sendTxnMarkers(txnTransitMetadata.coordinatorEpoch, txnTransitMetadata.result,
              txnTransitMetadata.txnMetadata, txnTransitMetadata.transitMetadata)
          }
        }
      }

      info(s"Completed loading transaction metadata from $topicPartition for coordinator epoch $coordinatorEpoch")
    }

    val scheduleStartMs = time.milliseconds()
    scheduler.scheduleOnce(s"load-txns-for-partition-$topicPartition", () => loadTransactions(scheduleStartMs))
  }

  def removeTransactionsForTxnTopicPartition(partitionId: Int): Unit = {
    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
    inWriteLock(stateLock) {
      loadingPartitions --= loadingPartitions.filter(_.txnPartitionId == partitionId)
      transactionMetadataCache.remove(partitionId).foreach { txnMetadataCacheEntry =>
        info(s"Unloaded transaction metadata $txnMetadataCacheEntry for $topicPartition following " +
          s"local partition deletion")
      }
    }
  }

  /**
   * When this broker becomes a follower for a transaction log partition, clear out the cache for corresponding transactional ids
   * that belong to that partition.
   */
  def removeTransactionsForTxnTopicPartition(partitionId: Int, coordinatorEpoch: Int): Unit = {
    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      loadingPartitions.remove(partitionAndLeaderEpoch)
      transactionMetadataCache.remove(partitionId) match {
        case Some(txnMetadataCacheEntry) =>
          info(s"Unloaded transaction metadata $txnMetadataCacheEntry for $topicPartition on become-follower transition")

        case None =>
          info(s"No cached transaction metadata found for $topicPartition during become-follower transition")
      }
    }
  }

  private def validateTransactionTopicPartitionCountIsStable(): Unit = {
    val previouslyDeterminedPartitionCount = transactionTopicPartitionCount
    val curTransactionTopicPartitionCount = retrieveTransactionTopicPartitionCount()
    if (previouslyDeterminedPartitionCount != curTransactionTopicPartitionCount)
      throw new KafkaException(s"Transaction topic number of partitions has changed from $previouslyDeterminedPartitionCount to $curTransactionTopicPartitionCount")
  }

  def appendTransactionToLog(transactionalId: String,
                             coordinatorEpoch: Int,
                             newMetadata: TxnTransitMetadata,
                             responseCallback: Errors => Unit,
                             retryOnError: Errors => Boolean = _ => false,
                             requestLocal: RequestLocal): Unit = {

    // generate the message for this transaction metadata
    val keyBytes = TransactionLog.keyToBytes(transactionalId)
    val valueBytes = TransactionLog.valueToBytes(newMetadata)
    val timestamp = time.milliseconds()

    val records = MemoryRecords.withRecords(TransactionLog.EnforcedCompressionType, new SimpleRecord(timestamp, keyBytes, valueBytes))
    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionFor(transactionalId))
    val recordsPerPartition = Map(topicPartition -> records)

    // set the callback function to update transaction status in cache after log append completed
    def updateCacheCallback(responseStatus: collection.Map[TopicPartition, PartitionResponse]): Unit = {
      // the append response should only contain the topics partition
      if (responseStatus.size != 1 || !responseStatus.contains(topicPartition))
        throw new IllegalStateException("Append status %s should only have one partition %s"
          .format(responseStatus, topicPartition))

      val status = responseStatus(topicPartition)

      var responseError = if (status.error == Errors.NONE) {
        Errors.NONE
      } else {
        debug(s"Appending $transactionalId's new metadata $newMetadata failed due to ${status.error.exceptionName}")

        // transform the log append error code to the corresponding coordinator error code
        status.error match {
          case Errors.UNKNOWN_TOPIC_OR_PARTITION
               | Errors.NOT_ENOUGH_REPLICAS
               | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND
               | Errors.REQUEST_TIMED_OUT => // note that for timed out request we return NOT_AVAILABLE error code to let client retry
            Errors.COORDINATOR_NOT_AVAILABLE

          case Errors.NOT_LEADER_OR_FOLLOWER
               | Errors.KAFKA_STORAGE_ERROR =>
            Errors.NOT_COORDINATOR

          case Errors.MESSAGE_TOO_LARGE
               | Errors.RECORD_LIST_TOO_LARGE =>
            Errors.UNKNOWN_SERVER_ERROR

          case other =>
            other
        }
      }

      if (responseError == Errors.NONE) {
        // now try to update the cache: we need to update the status in-place instead of
        // overwriting the whole object to ensure synchronization
        getTransactionState(transactionalId) match {

          case Left(err) =>
            info(s"Accessing the cached transaction metadata for $transactionalId returns $err error; " +
              s"aborting transition to the new metadata and setting the error in the callback")
            responseError = err
          case Right(Some(epochAndMetadata)) =>
            val metadata = epochAndMetadata.transactionMetadata

            metadata.inLock {
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
                // the cache may have been changed due to txn topic partition emigration and immigration,
                // in this case directly return NOT_COORDINATOR to client and let it to re-discover the transaction coordinator
                info(s"The cached coordinator epoch for $transactionalId has changed to ${epochAndMetadata.coordinatorEpoch} after appended its new metadata $newMetadata " +
                  s"to the transaction log (txn topic partition ${partitionFor(transactionalId)}) while it was $coordinatorEpoch before appending; " +
                  s"aborting transition to the new metadata and returning ${Errors.NOT_COORDINATOR} in the callback")
                responseError = Errors.NOT_COORDINATOR
              } else {
                metadata.completeTransitionTo(newMetadata)
                debug(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId succeeded")
              }
            }

          case Right(None) =>
            // this transactional id no longer exists, maybe the corresponding partition has already been migrated out.
            // return NOT_COORDINATOR to let the client re-discover the transaction coordinator
            info(s"The cached coordinator metadata does not exist in the cache anymore for $transactionalId after appended its new metadata $newMetadata " +
              s"to the transaction log (txn topic partition ${partitionFor(transactionalId)}) while it was $coordinatorEpoch before appending; " +
              s"aborting transition to the new metadata and returning ${Errors.NOT_COORDINATOR} in the callback")
            responseError = Errors.NOT_COORDINATOR
        }
      } else {
        // Reset the pending state when returning an error, since there is no active transaction for the transactional id at this point.
        getTransactionState(transactionalId) match {
          case Right(Some(epochAndTxnMetadata)) =>
            val metadata = epochAndTxnMetadata.transactionMetadata
            metadata.inLock {
              if (epochAndTxnMetadata.coordinatorEpoch == coordinatorEpoch) {
                if (retryOnError(responseError)) {
                  info(s"TransactionalId ${metadata.transactionalId} append transaction log for $newMetadata transition failed due to $responseError, " +
                    s"not resetting pending state ${metadata.pendingState} but just returning the error in the callback to let the caller retry")
                } else {
                  info(s"TransactionalId ${metadata.transactionalId} append transaction log for $newMetadata transition failed due to $responseError, " +
                    s"resetting pending state from ${metadata.pendingState}, aborting state transition and returning $responseError in the callback")

                  metadata.pendingState = None
                }
              } else {
                info(s"TransactionalId ${metadata.transactionalId} append transaction log for $newMetadata transition failed due to $responseError, " +
                  s"aborting state transition and returning the error in the callback since the coordinator epoch has changed from ${epochAndTxnMetadata.coordinatorEpoch} to $coordinatorEpoch")
              }
            }

          case Right(None) =>
            // Do nothing here, since we want to return the original append error to the user.
            info(s"TransactionalId $transactionalId append transaction log for $newMetadata transition failed due to $responseError, " +
              s"aborting state transition and returning the error in the callback since metadata is not available in the cache anymore")

          case Left(error) =>
            // Do nothing here, since we want to return the original append error to the user.
            info(s"TransactionalId $transactionalId append transaction log for $newMetadata transition failed due to $responseError, " +
              s"aborting state transition and returning the error in the callback since retrieving metadata returned $error")
        }

      }

      responseCallback(responseError)
    }

    inReadLock(stateLock) {
      // we need to hold the read lock on the transaction metadata cache until appending to local log returns;
      // this is to avoid the case where an emigration followed by an immigration could have completed after the check
      // returns and before appendRecords() is called, since otherwise entries with a high coordinator epoch could have
      // been appended to the log in between these two events, and therefore appendRecords() would append entries with
      // an old coordinator epoch that can still be successfully replicated on followers and make the log in a bad state.
      getTransactionState(transactionalId) match {
        case Left(err) =>
          responseCallback(err)

        case Right(None) =>
          // the coordinator metadata has been removed, reply to client immediately with NOT_COORDINATOR
          responseCallback(Errors.NOT_COORDINATOR)

        case Right(Some(epochAndMetadata)) =>
          val metadata = epochAndMetadata.transactionMetadata

          val append: Boolean = metadata.inLock {
            if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
              // the coordinator epoch has changed, reply to client immediately with NOT_COORDINATOR
              responseCallback(Errors.NOT_COORDINATOR)
              false
            } else {
              // do not need to check the metadata object itself since no concurrent thread should be able to modify it
              // under the same coordinator epoch, so directly append to txn log now
              true
            }
          }
          if (append) {
            replicaManager.appendRecords(
                newMetadata.txnTimeoutMs.toLong,
                TransactionLog.EnforcedRequiredAcks,
                internalTopicsAllowed = true,
                origin = AppendOrigin.COORDINATOR,
                recordsPerPartition,
                updateCacheCallback,
                requestLocal = requestLocal)

              trace(s"Appending new metadata $newMetadata for transaction id $transactionalId with coordinator epoch $coordinatorEpoch to the local transaction log")
          }
      }
    }
  }

  def startup(retrieveTransactionTopicPartitionCount: () => Int, enableTransactionalIdExpiration: Boolean): Unit = {
    this.retrieveTransactionTopicPartitionCount = retrieveTransactionTopicPartitionCount
    transactionTopicPartitionCount = retrieveTransactionTopicPartitionCount()
    if (enableTransactionalIdExpiration)
      this.enableTransactionalIdExpiration()
  }

  def shutdown(): Unit = {
    shuttingDown.set(true)
    loadingPartitions.clear()
    transactionMetadataCache.clear()

    info("Shutdown complete")
  }
}


private[transaction] case class TxnMetadataCacheEntry(coordinatorEpoch: Int,
                                                      metadataPerTransactionalId: Pool[String, TransactionMetadata]) {
  override def toString: String = {
    s"TxnMetadataCacheEntry(coordinatorEpoch=$coordinatorEpoch, numTransactionalEntries=${metadataPerTransactionalId.size})"
  }
}

private[transaction] case class CoordinatorEpochAndTxnMetadata(coordinatorEpoch: Int,
                                                               transactionMetadata: TransactionMetadata)

private[transaction] case class TransactionConfig(transactionalIdExpirationMs: Int = TransactionStateManager.DefaultTransactionalIdExpirationMs,
                                                  transactionMaxTimeoutMs: Int = TransactionStateManager.DefaultTransactionsMaxTimeoutMs,
                                                  transactionLogNumPartitions: Int = TransactionLog.DefaultNumPartitions,
                                                  transactionLogReplicationFactor: Short = TransactionLog.DefaultReplicationFactor,
                                                  transactionLogSegmentBytes: Int = TransactionLog.DefaultSegmentBytes,
                                                  transactionLogLoadBufferSize: Int = TransactionLog.DefaultLoadBufferSize,
                                                  transactionLogMinInsyncReplicas: Int = TransactionLog.DefaultMinInSyncReplicas,
                                                  abortTimedOutTransactionsIntervalMs: Int = TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs,
                                                  removeExpiredTransactionalIdsIntervalMs: Int = TransactionStateManager.DefaultRemoveExpiredTransactionalIdsIntervalMs,
                                                  requestTimeoutMs: Int = Defaults.RequestTimeoutMs)

case class TransactionalIdAndProducerIdEpoch(transactionalId: String, producerId: Long, producerEpoch: Short) {
  override def toString: String = {
    s"(transactionalId=$transactionalId, producerId=$producerId, producerEpoch=$producerEpoch)"
  }
}

case class TransactionPartitionAndLeaderEpoch(txnPartitionId: Int, coordinatorEpoch: Int)

case class TransactionalIdCoordinatorEpochAndMetadata(transactionalId: String, coordinatorEpoch: Int, transitMetadata: TxnTransitMetadata)

case class TransactionalIdCoordinatorEpochAndTransitMetadata(transactionalId: String, coordinatorEpoch: Int, result: TransactionResult, txnMetadata: TransactionMetadata, transitMetadata: TxnTransitMetadata)

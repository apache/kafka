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

import kafka.log.LogConfig
import kafka.message.UncompressedCodec
import kafka.server.Defaults
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.{Logging, Pool, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.mutable
import scala.collection.JavaConverters._


object TransactionStateManager {
  // default transaction management config values
  val DefaultTransactionsMaxTimeoutMs: Int = TimeUnit.MINUTES.toMillis(15).toInt
  val DefaultTransactionalIdExpirationMs: Int = TimeUnit.DAYS.toMillis(7).toInt
  val DefaultAbortTimedOutTransactionsIntervalMs: Int = TimeUnit.MINUTES.toMillis(1).toInt
  val DefaultRemoveExpiredTransactionalIdsIntervalMs: Int = TimeUnit.HOURS.toMillis(1).toInt
}

/**
 * Transaction state manager is part of the transaction coordinator, it manages:
 *
 * 1. the transaction log, which is a special internal topic.
 * 2. the transaction metadata including its ongoing transaction status.
 * 3. the background expiration of the transaction as well as the transactional id.
 *
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in TransactionStateManager use `stateLock.readLock` as the delayed operation
 * lock. Delayed operations are completed only if `stateLock.readLock` can be acquired.
 * Delayed callbacks may acquire `stateLock.readLock` or any of the `txnMetadata` locks.
 * <ul>
 * <li>`stateLock.readLock` must never be acquired while holding `txnMetadata` lock.</li>
 * <li>`txnMetadata` lock must never be acquired while holding `stateLock.writeLock`.</li>
 * <li>`ReplicaManager.appendRecords` should never be invoked while holding a `txnMetadata` lock.</li>
 * </ul>
 */
class TransactionStateManager(brokerId: Int,
                              zkClient: KafkaZkClient,
                              scheduler: Scheduler,
                              replicaManager: ReplicaManager,
                              config: TransactionConfig,
                              time: Time) extends Logging {

  this.logIdent = "[Transaction State Manager " + brokerId + "]: "

  type SendTxnMarkersCallback = (String, Int, TransactionResult, TransactionMetadata, TxnTransitMetadata) => Unit

  /** shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /** lock protecting access to the transactional metadata cache, including loading and leaving partition sets */
  private val stateLock = new ReentrantReadWriteLock()

  /** partitions of transaction topic that are being loaded, state lock should be called BEFORE accessing this set */
  private val loadingPartitions: mutable.Set[TransactionPartitionAndLeaderEpoch] = mutable.Set()

  /** partitions of transaction topic that are being removed, state lock should be called BEFORE accessing this set */
  private val leavingPartitions: mutable.Set[TransactionPartitionAndLeaderEpoch] = mutable.Set()

  /** transaction metadata cache indexed by assigned transaction topic partition ids */
  private val transactionMetadataCache: mutable.Map[Int, TxnMetadataCacheEntry] = mutable.Map()

  /** number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  // visible for testing only
  private[transaction] def addLoadingPartition(partitionId: Int, coordinatorEpoch: Int): Unit = {
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      leavingPartitions.remove(partitionAndLeaderEpoch)
      loadingPartitions.add(partitionAndLeaderEpoch)
    }
  }
  private[transaction] def stateReadLock = stateLock.readLock

  // this is best-effort expiration of an ongoing transaction which has been open for more than its
  // txn timeout value, we do not need to grab the lock on the metadata object upon checking its state
  // since the timestamp is volatile and we will get the lock when actually trying to transit the transaction
  // metadata to abort later.
  def timedOutTransactions(): Iterable[TransactionalIdAndProducerIdEpoch] = {
    val now = time.milliseconds()
    inReadLock(stateLock) {
      transactionMetadataCache.filter { case (txnPartitionId, _) =>
        !leavingPartitions.exists(_.txnPartitionId == txnPartitionId)
      }.flatMap { case (_, entry) =>
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

  def enableTransactionalIdExpiration() {
    scheduler.schedule("transactionalId-expiration", () => {
      val now = time.milliseconds()
      inReadLock(stateLock) {
        val transactionalIdByPartition: Map[Int, mutable.Iterable[TransactionalIdCoordinatorEpochAndMetadata]] =
          transactionMetadataCache.flatMap { case (_, entry) =>
            entry.metadataPerTransactionalId.filter { case (_, txnMetadata) => txnMetadata.state match {
              case Empty | CompleteCommit | CompleteAbort => true
              case _ => false
            }
            }.filter { case (_, txnMetadata) =>
              txnMetadata.txnLastUpdateTimestamp <= now - config.transactionalIdExpirationMs
            }.map { case (transactionalId, txnMetadata) =>
              val txnMetadataTransition = txnMetadata.inLock {
                txnMetadata.prepareDead()
              }
              TransactionalIdCoordinatorEpochAndMetadata(transactionalId, entry.coordinatorEpoch, txnMetadataTransition)
            }
          }.groupBy { transactionalIdCoordinatorEpochAndMetadata =>
            partitionFor(transactionalIdCoordinatorEpochAndMetadata.transactionalId)
          }

        val recordsPerPartition = transactionalIdByPartition
          .map { case (partition, transactionalIdCoordinatorEpochAndMetadatas) =>
            val deletes: Array[SimpleRecord] = transactionalIdCoordinatorEpochAndMetadatas.map { entry =>
              new SimpleRecord(now, TransactionLog.keyToBytes(entry.transactionalId), null)
            }.toArray
            val records = MemoryRecords.withRecords(TransactionLog.EnforcedCompressionType, deletes: _*)
            val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partition)
            (topicPartition, records)
          }


        def removeFromCacheCallback(responses: collection.Map[TopicPartition, PartitionResponse]): Unit = {
          responses.foreach { case (topicPartition, response) =>
            response.error match {
              case Errors.NONE =>
                inReadLock(stateLock) {
                  val toRemove = transactionalIdByPartition(topicPartition.partition())
                  transactionMetadataCache.get(topicPartition.partition)
                    .foreach { txnMetadataCacheEntry =>
                      toRemove.foreach { idCoordinatorEpochAndMetadata =>
                        val txnMetadata = txnMetadataCacheEntry.metadataPerTransactionalId.get(idCoordinatorEpochAndMetadata.transactionalId)
                        txnMetadata.inLock {
                          if (txnMetadataCacheEntry.coordinatorEpoch == idCoordinatorEpochAndMetadata.coordinatorEpoch
                            && txnMetadata.pendingState.contains(Dead)
                            && txnMetadata.producerEpoch == idCoordinatorEpochAndMetadata.transitMetadata.producerEpoch
                          )
                            txnMetadataCacheEntry.metadataPerTransactionalId.remove(idCoordinatorEpochAndMetadata.transactionalId)
                          else {
                             debug(s"failed to remove expired transactionalId: ${idCoordinatorEpochAndMetadata.transactionalId}" +
                               s" from cache. pendingState: ${txnMetadata.pendingState} producerEpoch: ${txnMetadata.producerEpoch}" +
                               s" expected producerEpoch: ${idCoordinatorEpochAndMetadata.transitMetadata.producerEpoch}" +
                               s" coordinatorEpoch: ${txnMetadataCacheEntry.coordinatorEpoch} expected coordinatorEpoch: " +
                               s"${idCoordinatorEpochAndMetadata.coordinatorEpoch}")
                            txnMetadata.pendingState = None
                          }
                        }
                      }
                    }
                }
              case _ =>
                debug(s"writing transactionalId tombstones for partition: ${topicPartition.partition} failed with error: ${response.error.message()}")
            }
          }
        }

        replicaManager.appendRecords(
          config.requestTimeoutMs,
          TransactionLog.EnforcedRequiredAcks,
          internalTopicsAllowed = true,
          isFromClient = false,
          recordsPerPartition,
          removeFromCacheCallback,
          Some(stateLock.readLock)
        )
      }

    }, delay = config.removeExpiredTransactionalIdsIntervalMs, period = config.removeExpiredTransactionalIdsIntervalMs)
  }

  def getTransactionState(transactionalId: String): Either[Errors, Option[CoordinatorEpochAndTxnMetadata]] =
    getAndMaybeAddTransactionState(transactionalId, None)

  def putTransactionStateIfNotExists(transactionalId: String,
                                     txnMetadata: TransactionMetadata): Either[Errors, CoordinatorEpochAndTxnMetadata] =
    getAndMaybeAddTransactionState(transactionalId, Some(txnMetadata))
      .right.map(_.getOrElse(throw new IllegalStateException(s"Unexpected empty transaction metadata returned while putting $txnMetadata")))

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
      else if (leavingPartitions.exists(_.txnPartitionId == partitionId))
        Left(Errors.NOT_COORDINATOR)
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
    props.put(LogConfig.UncleanLeaderElectionEnableProp, "false")
    props.put(LogConfig.CompressionTypeProp, UncompressedCodec.name)
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.MinInSyncReplicasProp, config.transactionLogMinInsyncReplicas.toString)
    props.put(LogConfig.SegmentBytesProp, config.transactionLogSegmentBytes.toString)

    props
  }

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  /**
   * Gets the partition count of the transaction log topic from ZooKeeper.
   * If the topic does not exist, the default partition count is returned.
   */
  private def getTransactionTopicPartitionCount: Int = {
    zkClient.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionLogNumPartitions)
  }

  private def loadTransactionMetadata(topicPartition: TopicPartition, coordinatorEpoch: Int): Pool[String, TransactionMetadata] =  {
    def logEndOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)

    val startMs = time.milliseconds()
    val loadedTransactions = new Pool[String, TransactionMetadata]

    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        lazy val buffer = ByteBuffer.allocate(config.transactionLogLoadBufferSize)

        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        var currOffset = log.logStartOffset

        try {
          while (currOffset < logEndOffset
            && !shuttingDown.get()
            && inReadLock(stateLock) {loadingPartitions.exists { idAndEpoch: TransactionPartitionAndLeaderEpoch =>
              idAndEpoch.txnPartitionId == topicPartition.partition && idAndEpoch.coordinatorEpoch == coordinatorEpoch}}) {
            val fetchDataInfo = log.read(currOffset, config.transactionLogLoadBufferSize, maxOffset = None,
              minOneMessage = true, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
            val memRecords = fetchDataInfo.records match {
              case records: MemoryRecords => records
              case fileRecords: FileRecords =>
                buffer.clear()
                fileRecords.readInto(buffer, 0)
                MemoryRecords.readableRecords(buffer)
            }

            memRecords.batches.asScala.foreach { batch =>
              for (record <- batch.asScala) {
                require(record.hasKey, "Transaction state log's key should not be null")
                val txnKey = TransactionLog.readTxnRecordKey(record.key)
                // load transaction metadata along with transaction state
                val transactionalId = txnKey.transactionalId
                if (!record.hasValue) {
                  loadedTransactions.remove(transactionalId)
                } else {
                  val txnMetadata = TransactionLog.readTxnRecordValue(transactionalId, record.value)
                  loadedTransactions.put(transactionalId, txnMetadata)
                }
                currOffset = batch.nextOffset
              }
            }

            info(s"Finished loading ${loadedTransactions.size} transaction metadata from $topicPartition in ${time.milliseconds() - startMs} milliseconds")
          }
        } catch {
          case t: Throwable => error(s"Error loading transactions from transaction log $topicPartition", t)
        }
    }

    loadedTransactions
  }

  /**
   * Add a transaction topic partition into the cache
   *
   * Make it package-private to be used only for unit tests.
   */
  private[transaction] def addLoadedTransactionsToCache(txnTopicPartition: Int, coordinatorEpoch: Int, metadataPerTransactionalId: Pool[String, TransactionMetadata]): Unit = {
    val txnMetadataCacheEntry = TxnMetadataCacheEntry(coordinatorEpoch, metadataPerTransactionalId)
    val currentTxnMetadataCacheEntry = transactionMetadataCache.put(txnTopicPartition, txnMetadataCacheEntry)

    if (currentTxnMetadataCacheEntry.isDefined) {
      val coordinatorEpoch = currentTxnMetadataCacheEntry.get.coordinatorEpoch
      val metadataPerTxnId = currentTxnMetadataCacheEntry.get.metadataPerTransactionalId
      val errorMsg = s"The metadata cache for txn partition $txnTopicPartition has already exist with epoch $coordinatorEpoch " +
        s"and ${metadataPerTxnId.size} entries while trying to add to it; " +
        s"this should not happen"
      fatal(errorMsg)
      throw new IllegalStateException(errorMsg)
    }
  }

  /**
   * When this broker becomes a leader for a transaction log partition, load this partition and
   * populate the transaction metadata cache with the transactional ids.
   */
  def loadTransactionsForTxnTopicPartition(partitionId: Int, coordinatorEpoch: Int, sendTxnMarkers: SendTxnMarkersCallback) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      leavingPartitions.remove(partitionAndLeaderEpoch)
      loadingPartitions.add(partitionAndLeaderEpoch)
    }

    def loadTransactions() {
      info(s"Loading transaction metadata from $topicPartition")
      val loadedTransactions = loadTransactionMetadata(topicPartition, coordinatorEpoch)

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
                    // nothing need to be done
                }
              }
          }

          // we first remove the partition from loading partition then send out the markers for those pending to be
          // completed transactions, so that when the markers get sent the attempt of appending the complete transaction
          // log would not be blocked by the coordinator loading error
          loadingPartitions.remove(partitionAndLeaderEpoch)

          transactionsPendingForCompletion.foreach { txnTransitMetadata =>
            sendTxnMarkers(txnTransitMetadata.transactionalId, txnTransitMetadata.coordinatorEpoch, txnTransitMetadata.result, txnTransitMetadata.txnMetadata, txnTransitMetadata.transitMetadata)
          }
        }
      }
    }

    scheduler.schedule(s"load-txns-for-partition-$topicPartition", loadTransactions)
  }

  /**
   * When this broker becomes a follower for a transaction log partition, clear out the cache for corresponding transactional ids
   * that belong to that partition.
   */
  def removeTransactionsForTxnTopicPartition(partitionId: Int, coordinatorEpoch: Int) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      loadingPartitions.remove(partitionAndLeaderEpoch)
      leavingPartitions.add(partitionAndLeaderEpoch)
    }

    def removeTransactions() {
      inWriteLock(stateLock) {
        if (leavingPartitions.contains(partitionAndLeaderEpoch)) {
          transactionMetadataCache.remove(partitionId) match {
            case Some(txnMetadataCacheEntry) =>
              info(s"Removed ${txnMetadataCacheEntry.metadataPerTransactionalId.size} cached transaction metadata for $topicPartition on follower transition")

            case None =>
              info(s"Trying to remove cached transaction metadata for $topicPartition on follower transition but there is no entries remaining; " +
                s"it is likely that another process for removing the cached entries has just executed earlier before")
          }

          leavingPartitions.remove(partitionAndLeaderEpoch)
        }
      }
    }

    scheduler.schedule(s"remove-txns-for-partition-$topicPartition", removeTransactions)
  }

  private def validateTransactionTopicPartitionCountIsStable(): Unit = {
    val curTransactionTopicPartitionCount = getTransactionTopicPartitionCount
    if (transactionTopicPartitionCount != curTransactionTopicPartitionCount)
      throw new KafkaException(s"Transaction topic number of partitions has changed from $transactionTopicPartitionCount to $curTransactionTopicPartitionCount")
  }

  def appendTransactionToLog(transactionalId: String,
                             coordinatorEpoch: Int,
                             newMetadata: TxnTransitMetadata,
                             responseCallback: Errors => Unit,
                             retryOnError: Errors => Boolean = _ => false): Unit = {

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

          case Errors.NOT_LEADER_FOR_PARTITION
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
                isFromClient = false,
                recordsPerPartition,
                updateCacheCallback,
                delayedProduceLock = Some(stateLock.readLock))

              trace(s"Appending new metadata $newMetadata for transaction id $transactionalId with coordinator epoch $coordinatorEpoch to the local transaction log")
          }
      }
    }
  }

  def shutdown() {
    shuttingDown.set(true)
    loadingPartitions.clear()
    transactionMetadataCache.clear()

    info("Shutdown complete")
  }
}


private[transaction] case class TxnMetadataCacheEntry(coordinatorEpoch: Int, metadataPerTransactionalId: Pool[String, TransactionMetadata])

private[transaction] case class CoordinatorEpochAndTxnMetadata(coordinatorEpoch: Int, transactionMetadata: TransactionMetadata)

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

case class TransactionalIdAndProducerIdEpoch(transactionalId: String, producerId: Long, producerEpoch: Short)

case class TransactionPartitionAndLeaderEpoch(txnPartitionId: Int, coordinatorEpoch: Int)

case class TransactionalIdCoordinatorEpochAndMetadata(transactionalId: String, coordinatorEpoch: Int, transitMetadata: TxnTransitMetadata)

case class TransactionalIdCoordinatorEpochAndTransitMetadata(transactionalId: String, coordinatorEpoch: Int, result: TransactionResult, txnMetadata: TransactionMetadata, transitMetadata: TxnTransitMetadata)

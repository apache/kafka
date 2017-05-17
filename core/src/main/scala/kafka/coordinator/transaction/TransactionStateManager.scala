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
import java.util.concurrent.locks.ReentrantLock

import kafka.common.KafkaException
import kafka.log.LogConfig
import kafka.message.UncompressedCodec
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils.{Logging, Pool, Scheduler, ZkUtils}
import org.apache.kafka.common.TopicPartition
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
  // TODO: this needs to be replaces by the config values
  val DefaultTransactionsMaxTimeoutMs: Int = TimeUnit.MINUTES.toMillis(15).toInt
  val DefaultTransactionalIdExpirationMs: Int = TimeUnit.DAYS.toMillis(7).toInt
  val DefaultRemoveExpiredTransactionsIntervalMs: Int = TimeUnit.MINUTES.toMillis(1).toInt
}

/**
 * Transaction state manager is part of the transaction coordinator, it manages:
 *
 * 1. the transaction log, which is a special internal topic.
 * 2. the transaction metadata including its ongoing transaction status.
 * 3. the background expiration of the transaction as well as the transactional id.
 */
class TransactionStateManager(brokerId: Int,
                              zkUtils: ZkUtils,
                              scheduler: Scheduler,
                              replicaManager: ReplicaManager,
                              config: TransactionConfig,
                              time: Time) extends Logging {

  this.logIdent = "[Transaction Log Manager " + brokerId + "]: "

  type SendTxnMarkersCallback = (String, Int, TransactionResult, TransactionMetadata, TransactionMetadataTransition) => Unit

  /** shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /** lock protecting access to loading and owned partition sets */
  private val stateLock = new ReentrantLock()

  /** partitions of transaction topic that are being loaded, partition lock should be called BEFORE accessing this set */
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /** transaction metadata cache indexed by assigned transaction topic partition ids */
  private val transactionMetadataCache: mutable.Map[Int, TxnMetadataCacheEntry] = mutable.Map()

  /** number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  // this is best-effort expiration and hence not grabing the lock on metadata upon checking its state
  // we will get the lock when actually trying to transit the transaction metadata to abort later.
  def transactionsToExpire(): Iterable[TransactionalIdAndProducerIdEpoch] = {
    val now = time.milliseconds()
    transactionMetadataCache.flatMap { case (_, entry) =>
        entry.metadataPerTransactionalId.filter { case (txnId, txnMetadata) =>
          if (isCoordinatorLoadingInProgress(txnId) || txnMetadata.pendingTransitionInProgress) {
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

  def enablePidExpiration() {
    // TODO: add producer id expiration logic
  }

  /**
   * Get the transaction metadata associated with the given transactional id, or null if not found
   */
  def getTransactionState(transactionalId: String): Option[CoordinatorEpochAndTxnMetadata] = {
    val partitionId = partitionFor(transactionalId)

    transactionMetadataCache.get(partitionId).flatMap { cacheEntry =>
      cacheEntry.metadataPerTransactionalId.get(transactionalId) match {
        case null => None
        case txnMetadata => Some(CoordinatorEpochAndTxnMetadata(cacheEntry.coordinatorEpoch, txnMetadata))
      }
    }
  }

  /**
   * Add a new transaction metadata, or retrieve the metadata if it already exists with the associated transactional id
   * along with the current coordinator epoch for that belonging transaction topic partition
   */
  def addTransaction(transactionalId: String, txnMetadata: TransactionMetadata): CoordinatorEpochAndTxnMetadata = {
    val partitionId = partitionFor(transactionalId)

    transactionMetadataCache.get(partitionId) match {
      case Some(txnMetadataCacheEntry) =>
        val currentTxnMetadata = txnMetadataCacheEntry.metadataPerTransactionalId.putIfNotExists(transactionalId, txnMetadata)
        if (currentTxnMetadata != null) {
          CoordinatorEpochAndTxnMetadata(txnMetadataCacheEntry.coordinatorEpoch, currentTxnMetadata)
        } else {
          CoordinatorEpochAndTxnMetadata(txnMetadataCacheEntry.coordinatorEpoch, txnMetadata)
        }

      case None =>
        throw new IllegalStateException(s"The metadata cache entry for txn partition $partitionId does not exist.")
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

  def isCoordinatorFor(txnTopicPartitionId: Int): Boolean = inLock(stateLock) {
    transactionMetadataCache.contains(txnTopicPartitionId)
  }

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(stateLock) {
    val partitionId = partitionFor(transactionalId)
    transactionMetadataCache.contains(partitionId)
  }

  def isCoordinatorLoadingInProgress(transactionalId: String): Boolean = inLock(stateLock) {
    val partitionId = partitionFor(transactionalId)
    loadingPartitions.contains(partitionId)
  }

  /**
   * Gets the partition count of the transaction log topic from ZooKeeper.
   * If the topic does not exist, the default partition count is returned.
   */
  private def getTransactionTopicPartitionCount: Int = {
    zkUtils.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionLogNumPartitions)
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
            && loadingPartitions.contains(topicPartition.partition())
            && !shuttingDown.get()) {
            val fetchDataInfo = log.read(currOffset, config.transactionLogLoadBufferSize, maxOffset = None,
              minOneMessage = true, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
            val memRecords = fetchDataInfo.records match {
              case records: MemoryRecords => records
              case fileRecords: FileRecords =>
                buffer.clear()
                val bufferRead = fileRecords.readInto(buffer, 0)
                MemoryRecords.readableRecords(bufferRead)
            }

            memRecords.batches.asScala.foreach { batch =>
              for (record <- batch.asScala) {
                require(record.hasKey, "Transaction state log's key should not be null")
                TransactionLog.readMessageKey(record.key) match {

                  case txnKey: TxnKey =>
                    // load transaction metadata along with transaction state
                    val transactionalId: String = txnKey.transactionalId
                    if (!record.hasValue) {
                      loadedTransactions.remove(transactionalId)
                    } else {
                      val txnMetadata = TransactionLog.readMessageValue(record.value)
                      loadedTransactions.put(transactionalId, txnMetadata)
                    }

                  case unknownKey =>
                    // TODO: Metrics
                    throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
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
    */
  def addLoadedTransactionsToCache(txnTopicPartition: Int, coordinatorEpoch: Int, metadataPerTransactionalId: Pool[String, TransactionMetadata]): Unit = {
    val txnMetadataCacheEntry = TxnMetadataCacheEntry(coordinatorEpoch, metadataPerTransactionalId)
    val currentTxnMetadataCacheEntry = transactionMetadataCache.put(txnTopicPartition, txnMetadataCacheEntry)

    if (currentTxnMetadataCacheEntry.isDefined) {
      val coordinatorEpoch = currentTxnMetadataCacheEntry.get.coordinatorEpoch
      val metadataPerTxnId = currentTxnMetadataCacheEntry.get.metadataPerTransactionalId
      info(s"The metadata cache for txn partition $txnTopicPartition has already exist with epoch $coordinatorEpoch " +
        s"and ${metadataPerTxnId.size} entries while trying to add to it; " +
        s"it is likely that another process for loading from the transaction log has just executed earlier before")

      throw new IllegalStateException(s"The metadata cache entry for txn partition $txnTopicPartition has already exist while trying to add to it.")
    }
  }

  /**
   * When this broker becomes a leader for a transaction log partition, load this partition and
   * populate the transaction metadata cache with the transactional ids.
   */
  def loadTransactionsForTxnTopicPartition(partitionId: Int, coordinatorEpoch: Int, sendTxnMarkers: SendTxnMarkersCallback) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)

    inLock(stateLock) {
      loadingPartitions.add(partitionId)
    }

    def loadTransactions() {
      info(s"Loading transaction metadata from $topicPartition")
      val loadedTransactions = loadTransactionMetadata(topicPartition, coordinatorEpoch)

      loadedTransactions.foreach {
        case (transactionalId, txnMetadata) =>
          val result = txnMetadata synchronized {
            // if state is PrepareCommit or PrepareAbort we need to complete the transaction
            txnMetadata.state match {
              case PrepareAbort =>
                Some(TransactionResult.ABORT, txnMetadata.prepareComplete(time.milliseconds()))
              case PrepareCommit =>
                Some(TransactionResult.COMMIT, txnMetadata.prepareComplete(time.milliseconds()))
              case _ =>
                // nothing need to be done
                None
            }
          }

          result.foreach { case (command, newMetadata) =>
            sendTxnMarkers(transactionalId, coordinatorEpoch, command, txnMetadata, newMetadata)
          }
      }

      inLock(stateLock) {
        addLoadedTransactionsToCache(topicPartition.partition, coordinatorEpoch, loadedTransactions)
        loadingPartitions.remove(partitionId)
      }
    }

    scheduler.schedule(s"load-txns-for-partition-$topicPartition", loadTransactions _)
  }

  /**
   * When this broker becomes a follower for a transaction log partition, clear out the cache for corresponding transactional ids
   * that belong to that partition.
   */
  def removeTransactionsForTxnTopicPartition(partitionId: Int) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)

    def removeTransactions() {
      inLock(stateLock) {
        transactionMetadataCache.remove(partitionId) match {
          case Some(txnMetadataCacheEntry) =>
            info(s"Removed ${txnMetadataCacheEntry.metadataPerTransactionalId.size} cached transaction metadata for $topicPartition on follower transition")

          case None =>
            info(s"Trying to remove cached transaction metadata for $topicPartition on follower transition but there is no entries remaining; " +
              s"it is likely that another process for removing the cached entries has just executed earlier before")
        }

        loadingPartitions.remove(partitionId)
      }
    }

    scheduler.schedule(s"remove-txns-for-partition-$topicPartition", removeTransactions _)
  }

  private def validateTransactionTopicPartitionCountIsStable(): Unit = {
    val curTransactionTopicPartitionCount = getTransactionTopicPartitionCount
    if (transactionTopicPartitionCount != curTransactionTopicPartitionCount)
      throw new KafkaException(s"Transaction topic number of partitions has changed from $transactionTopicPartitionCount to $curTransactionTopicPartitionCount")
  }

  // TODO: check broker message format and error if < V2
  def appendTransactionToLog(transactionalId: String,
                             coordinatorEpoch: Int,
                             newMetadata: TransactionMetadataTransition,
                             responseCallback: Errors => Unit): Unit = {

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
        debug(s"Transaction state update $newMetadata for $transactionalId failed when appending to log " +
          s"due to ${status.error.exceptionName}")

        // transform the log append error code to the corresponding coordinator error code
        status.error match {
          case Errors.UNKNOWN_TOPIC_OR_PARTITION
               | Errors.NOT_ENOUGH_REPLICAS
               | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND
               | Errors.REQUEST_TIMED_OUT => // note that for timed out request we return NOT_AVAILABLE error code to let client retry

            info(s"Appending transaction message $newMetadata for $transactionalId failed due to " +
              s"${status.error.exceptionName}, returning ${Errors.COORDINATOR_NOT_AVAILABLE} to the client")

            Errors.COORDINATOR_NOT_AVAILABLE

          case Errors.NOT_LEADER_FOR_PARTITION =>

            info(s"Appending transaction message $newMetadata for $transactionalId failed due to " +
              s"${status.error.exceptionName}, returning ${Errors.NOT_COORDINATOR} to the client")

            Errors.NOT_COORDINATOR

          case Errors.MESSAGE_TOO_LARGE
               | Errors.RECORD_LIST_TOO_LARGE =>

            error(s"Appending transaction message $newMetadata for $transactionalId failed due to " +
              s"${status.error.exceptionName}, returning UNKNOWN error code to the client")

            Errors.UNKNOWN

          case other =>
            error(s"Appending metadata message $newMetadata for $transactionalId failed due to " +
              s"unexpected error: ${status.error.message}")

            other
        }
      }

      if (responseError == Errors.NONE) {
        // now try to update the cache: we need to update the status in-place instead of
        // overwriting the whole object to ensure synchronization
        getTransactionState(transactionalId) match {
          case Some(epochAndMetadata) =>
            val metadata = epochAndMetadata.transactionMetadata

            metadata synchronized {
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
                // the cache may have been changed due to txn topic partition emigration and immigration,
                // in this case directly return NOT_COORDINATOR to client and let it to re-discover the transaction coordinator
                info(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed after the transaction message " +
                  s"has been appended to the log. The cached coordinator epoch has changed to ${epochAndMetadata.coordinatorEpoch}")

                responseError = Errors.NOT_COORDINATOR
              } else {
                metadata.completeTransitionTo(newMetadata)

                debug(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId succeeded")
              }
            }

          case None =>
            // this transactional id no longer exists, maybe the corresponding partition has already been migrated out.
            // return NOT_COORDINATOR to let the client re-discover the transaction coordinator
            info(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed after the transaction message " +
              s"has been appended to the log. The partition ${partitionFor(transactionalId)} may have migrated as the metadata is no longer in the cache")

            responseError = Errors.NOT_COORDINATOR
        }
      }

      responseCallback(responseError)
    }

    replicaManager.appendRecords(
      newMetadata.txnTimeoutMs.toLong,
      TransactionLog.EnforcedRequiredAcks,
      internalTopicsAllowed = true,
      isFromClient = false,
      recordsPerPartition,
      updateCacheCallback)
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
                                                  removeExpiredTransactionsIntervalMs: Int = TransactionStateManager.DefaultRemoveExpiredTransactionsIntervalMs)

case class TransactionalIdAndProducerIdEpoch(transactionalId: String, producerId: Long, producerEpoch: Short)

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

import kafka.common.{KafkaException, Topic}
import kafka.log.LogConfig
import kafka.message.NoCompressionCodec
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils.{Logging, Pool, Scheduler, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.mutable
import scala.collection.JavaConverters._


object TransactionManager {
  // default transaction management config values
  val DefaultTransactionalIdExpirationMs = TimeUnit.DAYS.toMillis(7).toInt
  val DefaultTransactionsMaxTimeoutMs = TimeUnit.MINUTES.toMillis(15).toInt
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

  type WriteTxnMarkers = WriteTxnMarkerArgs => Unit

  /** shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /** lock protecting access to loading and owned partition sets */
  private val stateLock = new ReentrantLock()

  /** partitions of transaction topic that are assigned to this manager, partition lock should be called BEFORE accessing this set */
  private val ownedPartitions: mutable.Map[Int, Int] = mutable.Map()

  /** partitions of transaction topic that are being loaded, partition lock should be called BEFORE accessing this set */
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /** transaction metadata cache indexed by transactional id */
  private val transactionMetadataCache = new Pool[String, TransactionMetadata]

  /** number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  def enablePidExpiration() {
    scheduler.startup()

    // TODO: add transaction and pid expiration logic
  }

  /**
   * Get the transaction metadata associated with the given transactional id, or null if not found
   */
  def getTransactionState(transactionalId: String): Option[TransactionMetadata] = {
    Option(transactionMetadataCache.get(transactionalId))
  }

  /**
   * Add a new transaction metadata, or retrieve the metadata if it already exists with the associated transactional id
   */
  def addTransaction(transactionalId: String, txnMetadata: TransactionMetadata): TransactionMetadata = {
    val currentTxnMetadata = transactionMetadataCache.putIfNotExists(transactionalId, txnMetadata)
    if (currentTxnMetadata != null) {
      currentTxnMetadata
    } else {
      txnMetadata
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
    props.put(LogConfig.CompressionTypeProp, NoCompressionCodec)
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)

    props.put(LogConfig.MinInSyncReplicasProp, config.transactionLogMinInsyncReplicas.toString)
    props.put(LogConfig.SegmentBytesProp, config.transactionLogSegmentBytes.toString)

    props
  }

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  def coordinatorEpochFor(transactionId: String): Option[Int] = inLock (stateLock) {
    ownedPartitions.get(partitionFor(transactionId))
  }

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(stateLock) {
    val partitionId = partitionFor(transactionalId)
    ownedPartitions.contains(partitionId)
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
    zkUtils.getTopicPartitionCount(Topic.TransactionStateTopicName).getOrElse(config.transactionLogNumPartitions)
  }

  private def loadTransactionMetadata(topicPartition: TopicPartition, writeTxnMarkers: WriteTxnMarkers) {
    def highWaterMark = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)

    val startMs = time.milliseconds()
    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        val buffer = ByteBuffer.allocate(config.transactionLogLoadBufferSize)

        val loadedTransactions = mutable.Map.empty[String, TransactionMetadata]
        val removedTransactionalIds = mutable.Set.empty[String]

        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        var currOffset = log.logStartOffset
        while (currOffset < highWaterMark
                && loadingPartitions.contains(topicPartition.partition())
                && !shuttingDown.get()) {
          buffer.clear()
          val fileRecords = log.read(currOffset, config.transactionLogLoadBufferSize, maxOffset = None, minOneMessage = true)
            .records.asInstanceOf[FileRecords]
          val bufferRead = fileRecords.readInto(buffer, 0)

          MemoryRecords.readableRecords(bufferRead).batches.asScala.foreach { batch =>
            for (record <- batch.asScala) {
              require(record.hasKey, "Transaction state log's key should not be null")
              TransactionLog.readMessageKey(record.key) match {

                case txnKey: TxnKey =>
                  // load transaction metadata along with transaction state
                  val transactionalId: String = txnKey.transactionalId
                  if (!record.hasValue) {
                    loadedTransactions.remove(transactionalId)
                    removedTransactionalIds.add(transactionalId)
                  } else {
                    val txnMetadata = TransactionLog.readMessageValue(record.value)
                    loadedTransactions.put(transactionalId, txnMetadata)
                    removedTransactionalIds.remove(transactionalId)
                  }

                case unknownKey =>
                  // TODO: Metrics
                  throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
              }

              currOffset = batch.nextOffset
            }
          }

          loadedTransactions.foreach {
            case (transactionalId, txnMetadata) =>
              val currentTxnMetadata = addTransaction(transactionalId, txnMetadata)
              if (!txnMetadata.eq(currentTxnMetadata)) {
                // treat this as a fatal failure as this should never happen
                fatal(s"Attempt to load $transactionalId's metadata $txnMetadata failed " +
                  s"because there is already a different cached transaction metadata $currentTxnMetadata.")

                throw new KafkaException("Loading transaction topic partition failed.")
              }
              // if state is PrepareCommit or PrepareAbort we need to complete the transaction
              if (currentTxnMetadata.state == PrepareCommit || currentTxnMetadata.state == PrepareAbort) {
                writeTxnMarkers(WriteTxnMarkerArgs(transactionalId,
                  txnMetadata.pid,
                  txnMetadata.producerEpoch,
                  txnMetadata.state,
                  txnMetadata,
                  coordinatorEpochFor(transactionalId).get
                ))
              }
          }

          removedTransactionalIds.foreach { transactionalId =>
            if (transactionMetadataCache.contains(transactionalId)) {
              // the cache already contains a transaction which should be removed,
              // treat this as a fatal failure as this should never happen
              fatal(s"Unexpected to see $transactionalId's metadata while " +
                s"loading partition $topicPartition since its latest state is a tombstone")

              throw new KafkaException("Loading transaction topic partition failed.")
            }
          }

          info(s"Finished loading ${loadedTransactions.size} transaction metadata from $topicPartition in ${time.milliseconds() - startMs} milliseconds")
        }
    }
  }

  /**
   * When this broker becomes a leader for a transaction log partition, load this partition and
   * populate the transaction metadata cache with the transactional ids.
   */
  def loadTransactionsForPartition(partition: Int, coordinatorEpoch: Int, writeTxnMarkers: WriteTxnMarkers) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partition)

    inLock(stateLock) {
      ownedPartitions.put(partition, coordinatorEpoch)
      loadingPartitions.add(partition)
    }

    def loadTransactions() {
      info(s"Loading transaction metadata from $topicPartition")
      try {
        loadTransactionMetadata(topicPartition, writeTxnMarkers)
      } catch {
        case t: Throwable => error(s"Error loading transactions from transaction log $topicPartition", t)
      } finally {
        inLock(stateLock) {
          loadingPartitions.remove(partition)
        }
      }
    }

    scheduler.schedule(topicPartition.toString, loadTransactions _)
  }

  /**
   * When this broker becomes a follower for a transaction log partition, clear out the cache for corresponding transactional ids
   * that belong to that partition.
   */
  def removeTransactionsForPartition(partition: Int) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partition)

    inLock(stateLock) {
      ownedPartitions.remove(partition)
      loadingPartitions.remove(partition)
    }

    def removeTransactions() {
      var numTxnsRemoved = 0

      inLock(stateLock) {
        for (transactionalId <- transactionMetadataCache.keys) {
          if (partitionFor(transactionalId) == partition) {
            // we do not need to worry about whether the transactional id has any ongoing transaction or not since
            // the new leader will handle it
            transactionMetadataCache.remove(transactionalId)
            numTxnsRemoved += 1
          }
        }

        if (numTxnsRemoved > 0)
          info(s"Removed $numTxnsRemoved cached transaction metadata for $topicPartition on follower transition")
      }
    }

    scheduler.schedule(topicPartition.toString, removeTransactions _)
  }

  private def validateTransactionTopicPartitionCountIsStable(): Unit = {
    val curTransactionTopicPartitionCount = getTransactionTopicPartitionCount
    if (transactionTopicPartitionCount != curTransactionTopicPartitionCount)
      throw new KafkaException(s"Transaction topic number of partitions has changed from $transactionTopicPartitionCount to $curTransactionTopicPartitionCount")
  }

  // TODO: check broker message format and error if < V2
  def appendTransactionToLog(transactionalId: String,
                             txnMetadata: TransactionMetadata,
                             responseCallback: Errors => Unit) {

    // generate the message for this transaction metadata
    val keyBytes = TransactionLog.keyToBytes(transactionalId)
    val valueBytes = TransactionLog.valueToBytes(txnMetadata)
    val timestamp = time.milliseconds()

    val records = MemoryRecords.withRecords(TransactionLog.EnforcedCompressionType, new SimpleRecord(timestamp, keyBytes, valueBytes))

    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partitionFor(transactionalId))
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
        debug(s"Transaction state update $txnMetadata for $transactionalId failed when appending to log " +
          s"due to ${status.error.exceptionName}")

        // transform the log append error code to the corresponding coordinator error code
        status.error match {
          case Errors.UNKNOWN_TOPIC_OR_PARTITION
               | Errors.NOT_ENOUGH_REPLICAS
               | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND
               | Errors.REQUEST_TIMED_OUT => // note that for timed out request we return NOT_AVAILABLE error code to let client retry

            debug(s"Appending transaction message $txnMetadata for $transactionalId failed due to " +
              s"${status.error.exceptionName}, returning ${Errors.COORDINATOR_NOT_AVAILABLE} to the client")

            Errors.COORDINATOR_NOT_AVAILABLE

          case Errors.NOT_LEADER_FOR_PARTITION =>

            debug(s"Appending transaction message $txnMetadata for $transactionalId failed due to " +
              s"${status.error.exceptionName}, returning ${Errors.NOT_COORDINATOR} to the client")

            Errors.NOT_COORDINATOR

          case Errors.MESSAGE_TOO_LARGE
               | Errors.RECORD_LIST_TOO_LARGE =>

            error(s"Appending transaction message $txnMetadata for $transactionalId failed due to " +
              s"${status.error.exceptionName}, returning UNKNOWN error code to the client")

            Errors.UNKNOWN

          case other =>
            error(s"Appending metadata message $txnMetadata for $transactionalId failed due to " +
              s"unexpected error: ${status.error.message}")

            other
        }
      }

      if (responseError == Errors.NONE) {
        def completeStateTransition(metadata: TransactionMetadata, newState: TransactionState): Boolean = {
          // there is no transition in this case
          if (metadata.state == Empty && newState == Empty)
            true
          else
            metadata.completeTransitionTo(txnMetadata.state)
        }
        // now try to update the cache: we need to update the status in-place instead of
        // overwriting the whole object to ensure synchronization
          getTransactionState(transactionalId) match {
            case Some(metadata) =>
              metadata synchronized {
                if (metadata.pid == txnMetadata.pid &&
                  metadata.producerEpoch == txnMetadata.producerEpoch &&
                  metadata.txnTimeoutMs == txnMetadata.txnTimeoutMs &&
                  completeStateTransition(metadata, txnMetadata.state)) {
                  // only topic-partition lists could possibly change (state should have transited in the above condition)
                  metadata.addPartitions(txnMetadata.topicPartitions.toSet)
                } else {
                  throw new IllegalStateException(s"Completing transaction state transition to $txnMetadata while its current state is $metadata.")
                }
              }

            case None =>
              // this transactional id no longer exists, maybe the corresponding partition has already been migrated out.
              // return NOT_COORDINATOR to let the client retry
              debug(s"Updating $transactionalId's transaction state to $txnMetadata for $transactionalId failed after the transaction message " +
                s"has been appended to the log. The partition for $transactionalId may have migrated as the metadata is no longer in the cache")

              responseError = Errors.NOT_COORDINATOR
          }
      }

      responseCallback(responseError)
    }

    replicaManager.appendRecords(
      txnMetadata.txnTimeoutMs.toLong,
      TransactionLog.EnforcedRequiredAcks,
      internalTopicsAllowed = true,
      recordsPerPartition,
      updateCacheCallback)
  }

  def shutdown() {
    shuttingDown.set(true)
    if (scheduler.isStarted)
      scheduler.shutdown()

    transactionMetadataCache.clear()

    ownedPartitions.clear()
    loadingPartitions.clear()

    info("Shutdown complete")
  }
}

private[transaction] case class TransactionConfig(transactionalIdExpirationMs: Int = TransactionManager.DefaultTransactionalIdExpirationMs,
                                                  transactionMaxTimeoutMs: Int = TransactionManager.DefaultTransactionsMaxTimeoutMs,
                                                  transactionLogNumPartitions: Int = TransactionLog.DefaultNumPartitions,
                                                  transactionLogReplicationFactor: Short = TransactionLog.DefaultReplicationFactor,
                                                  transactionLogSegmentBytes: Int = TransactionLog.DefaultSegmentBytes,
                                                  transactionLogLoadBufferSize: Int = TransactionLog.DefaultLoadBufferSize,
                                                  transactionLogMinInsyncReplicas: Int = TransactionLog.DefaultMinInSyncReplicas)

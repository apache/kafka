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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import kafka.common.{KafkaException, Topic}
import kafka.log.LogConfig
import kafka.message.NoCompressionCodec
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils.{Logging, Pool, Scheduler, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{FileRecords, MemoryRecords}
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.mutable
import scala.collection.JavaConverters._

/*
 * Transaction manager is part of the transaction coordinator, it manages:
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

  /* number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of transaction topic that are assigned to this manager, partition lock should be called BEFORE accessing this set */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* partitions of transaction topic that are being loaded, partition lock should be called BEFORE accessing this set */
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /* transaction metadata cache indexed by transactional id */
  private val transactionMetadataCache = new Pool[String, TransactionMetadata]

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  def enablePidExpiration() {
    scheduler.startup()

    // TODO: add transaction and pid expiration logic
  }

  /**
    * Get the transaction metadata associated with the given transactionalId, or null if not found
    */
  def getTransaction(transactionalId: String): Option[TransactionMetadata] = {
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

  def transactionTopicConfigs: Properties = {
    val props = new Properties

    // enforce disabled unclean leader election, no compression types, and compact cleanup policy
    props.put(LogConfig.UncleanLeaderElectionEnableProp, "false")
    props.put(LogConfig.CompressionTypeProp, NoCompressionCodec)
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)

    props.put(LogConfig.MinInSyncReplicasProp, config.minInsyncReplicas.toString)
    props.put(LogConfig.SegmentBytesProp, config.segmentBytes.toString)

    props
  }

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(partitionLock) {
    val partitionId = partitionFor(transactionalId)

    // partition id should be within the owned list and NOT in the corrupted list
    ownedPartitions.contains(partitionId)
  }

  def isCoordinatorLoadingInProgress(transactionalId: String): Boolean = inLock(partitionLock) {
    val partitionId = partitionFor(transactionalId)

    loadingPartitions.contains(partitionId)
  }

  /**
    * Gets the partition count of the transaction log topic from ZooKeeper.
    * If the topic does not exist, the default partition count is returned.
    */
  private def getTransactionTopicPartitionCount: Int = {
    zkUtils.getTopicPartitionCount(Topic.TransactionStateTopicName).getOrElse(config.numPartitions)
  }

  private def loadTransactionMetadata(topicPartition: TopicPartition) {
    def highWaterMark = replicaManager.getHighWatermark(topicPartition).getOrElse(-1L)

    val startMs = time.milliseconds()
    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        val buffer = ByteBuffer.allocate(config.loadBufferSize)

        val loadedTransactions = mutable.Map.empty[String, TransactionMetadata]
        val removedTransactionalIds = mutable.Set.empty[String]

        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        var currOffset = log.logStartOffset.getOrElse(throw new IllegalStateException(s"Could not find log start offset for $topicPartition"))
        while (currOffset < highWaterMark && !shuttingDown.get()) {
          buffer.clear()
          val fileRecords = log.read(currOffset, config.loadBufferSize, maxOffset = None, minOneMessage = true)
            .records.asInstanceOf[FileRecords]
          val bufferRead = fileRecords.readInto(buffer, 0)

          MemoryRecords.readableRecords(bufferRead).batches.asScala.foreach { batch =>
            for (record <- batch.asScala) {
              require(record.hasKey, "Transaction state log's key should not be null")
              TransactionLog.readMessageKey(record.key) match {

                case txnKey: TxnKey =>
                  // load transaction metadata along with transaction state
                  val transactionalId: String = txnKey.key
                  if (!record.hasValue) {
                    loadedTransactions.remove(transactionalId)
                    removedTransactionalIds.add(transactionalId)
                  } else {
                    val txnMetadata = TransactionLog.readMessageValue(record.value)
                    loadedTransactions.put(transactionalId, txnMetadata)
                    removedTransactionalIds.remove(transactionalId)
                  }

                case unknownKey =>
                  throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
              }

              currOffset = batch.nextOffset
            }
          }

          loadedTransactions.foreach {
            case (transactionalId, txnMetadata) =>
              val currentTxnMetadata = addTransaction(transactionalId, txnMetadata)
              if (!txnMetadata.equals(currentTxnMetadata)) {
                // treat this as a fatal failure as this should never happen
                fatal(s"Attempt to load $transactionalId's metadata $txnMetadata failed " +
                  s"because there is already a different cached transaction metadata $currentTxnMetadata.")

                throw new KafkaException("Loading transaction topic partition failed.")
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
  def loadTransactionsForPartition(partition: Int) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partition)

    def loadTransactions() {
      info(s"Loading transaction metadata from $topicPartition")

      inLock(partitionLock) {
        if (loadingPartitions.contains(partition)) {
          // with background scheduler containing one thread, this should never happen,
          // but just in case we change it in the future.
          info(s"Transaction status loading from $topicPartition already in progress.")
          return
        } else {
          loadingPartitions.add(partition)
        }
      }

      try {
        loadTransactionMetadata(topicPartition)
      } catch {
        case t: Throwable => error(s"Error loading transactions from transaction log $topicPartition", t)
      } finally {
        inLock(partitionLock) {
          ownedPartitions.add(partition)
          loadingPartitions.remove(partition)
        }
      }
    }

    scheduler.schedule(topicPartition.toString, loadTransactions)
  }

  /**
    * When this broker becomes a follower for a transaction log partition, clear out the cache for corresponding transactional ids
    * that belong to that partition.
    */
  def removeTransactionsForPartition(partition: Int) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partition)

    def removeTransactions() {
      var numTxnsRemoved = 0

      inLock(partitionLock) {
        if (!ownedPartitions.contains(partition)) {
          // with background scheduler containing one thread, this should never happen,
          // but just in case we change it in the future.
          info(s"Partition $topicPartition has already been removed.")
          return
        } else {
          ownedPartitions.remove(partition)
        }

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

    scheduler.schedule(topicPartition.toString, removeTransactions)
  }

  private def validateTransactionTopicPartitionCountIsStable(): Unit = {
    val curTransactionTopicPartitionCount = getTransactionTopicPartitionCount
    if (transactionTopicPartitionCount != curTransactionTopicPartitionCount)
      throw new KafkaException(s"Transaction topic number of partitions has changed from $transactionTopicPartitionCount to $curTransactionTopicPartitionCount")
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

private case class TransactionConfig(numPartitions: Int = TransactionLog.DefaultNumPartitions,
                                     replicationFactor: Short = TransactionLog.DefaultReplicationFactor,
                                     segmentBytes: Int = TransactionLog.DefaultSegmentBytes,
                                     loadBufferSize: Int = TransactionLog.DefaultLoadBufferSize,
                                     minInsyncReplicas: Int = TransactionLog.DefaultMinInSyncReplicas)

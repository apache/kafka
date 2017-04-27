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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.{Logging, Scheduler, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.Time
import kafka.utils.CoreUtils.inWriteLock


object TransactionCoordinator {

  def apply(config: KafkaConfig,
            replicaManager: ReplicaManager,
            scheduler: Scheduler,
            zkUtils: ZkUtils,
            metrics: Metrics,
            metadataCache: MetadataCache,
            time: Time): TransactionCoordinator = {

    val txnConfig = TransactionConfig(config.transactionalIdExpirationMs,
      config.transactionMaxTimeoutMs,
      config.transactionTopicPartitions,
      config.transactionTopicReplicationFactor,
      config.transactionTopicSegmentBytes,
      config.transactionsLoadBufferSize,
      config.transactionTopicMinISR)

    val pidManager = new ProducerIdManager(config.brokerId, zkUtils)
    val logManager = new TransactionStateManager(config.brokerId, zkUtils, scheduler, replicaManager, txnConfig, time)
    val txnMarkerPurgatory = DelayedOperationPurgatory[DelayedTxnMarker]("txn-marker-purgatory", config.brokerId)
    val transactionMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnMarkerPurgatory, time)

    new TransactionCoordinator(config.brokerId, pidManager, logManager, transactionMarkerChannelManager, txnMarkerPurgatory, time)
  }

  private def initTransactionError(error: Errors): InitPidResult = {
    InitPidResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TransactionMetadata): InitPidResult = {
    InitPidResult(txnMetadata.pid, txnMetadata.producerEpoch, Errors.NONE)
  }
}

/**
 * Transaction coordinator handles message transactions sent by producers and communicate with brokers
 * to update ongoing transaction's status.
 *
 * Each Kafka server instantiates a transaction coordinator which is responsible for a set of
 * producers. Producers with specific transactional ids are assigned to their corresponding coordinators;
 * Producers with no specific transactional id may talk to a random broker as their coordinators.
 */
class TransactionCoordinator(brokerId: Int,
                             pidManager: ProducerIdManager,
                             txnManager: TransactionStateManager,
                             txnMarkerChannelManager: TransactionMarkerChannelManager,
                             txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
                             time: Time) extends Logging {
  this.logIdent = "[Transaction Coordinator " + brokerId + "]: "

  import TransactionCoordinator._

  type InitPidCallback = InitPidResult => Unit
  type TxnMetadataUpdateCallback = Errors => Unit
  type EndTxnCallback = Errors => Unit

  /* Active flag of the coordinator */
  private val isActive = new AtomicBoolean(false)

  private val coordinatorLock = new ReentrantReadWriteLock

  def handleInitPid(transactionalId: String,
                    transactionTimeoutMs: Int,
                    responseCallback: InitPidCallback): Unit = {
      if (transactionalId == null || transactionalId.isEmpty) {
        // if the transactional id is not specified, then always blindly accept the request
        // and return a new pid from the pid manager
        val pid = pidManager.nextPid()
        responseCallback(InitPidResult(pid, epoch = 0, Errors.NONE))
      } else if (!txnManager.isCoordinatorFor(transactionalId)) {
        // check if it is the assigned coordinator for the transactional id
        responseCallback(initTransactionError(Errors.NOT_COORDINATOR))
      } else if (txnManager.isCoordinatorLoadingInProgress(transactionalId)) {
        responseCallback(initTransactionError(Errors.COORDINATOR_LOAD_IN_PROGRESS))
      } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
        // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
        responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
      } else {
        // only try to get a new pid and update the cache if the transactional id is unknown
        txnManager.getTransactionState(transactionalId) match {
          case None =>
            val pid = pidManager.nextPid()
            val newMetadata: TransactionMetadata = new TransactionMetadata(pid = pid,
              producerEpoch = 0,
              txnTimeoutMs = transactionTimeoutMs,
              state = Empty,
              topicPartitions = collection.mutable.Set.empty[TopicPartition],
              lastUpdateTimestamp = time.milliseconds())

            val metadata = txnManager.addTransaction(transactionalId, newMetadata)

            // there might be a concurrent thread that has just updated the mapping
            // with the transactional id at the same time; in this case we will
            // treat it as the metadata has existed and update it accordingly
            metadata synchronized {
              if (!metadata.eq(newMetadata))
                initPidWithExistingMetadata(transactionalId, transactionTimeoutMs, responseCallback, metadata)
              else
                appendMetadataToLog(transactionalId, metadata, responseCallback)

            }
          case Some(metadata) =>
            initPidWithExistingMetadata(transactionalId, transactionTimeoutMs, responseCallback, metadata)
        }
      }
  }

  private def appendMetadataToLog(transactionalId: String,
                             metadata: TransactionMetadata,
                             initPidCallback: InitPidCallback): Unit ={
    def callback(errors: Errors): Unit = {
      if (errors == Errors.NONE)
        initPidCallback(initTransactionMetadata(metadata))
      else
        initPidCallback(initTransactionError(errors))
    }
    appendToLogInReadLock(transactionalId, metadata, callback)
  }


  private def initPidWithExistingMetadata(transactionalId: String,
                                          transactionTimeoutMs: Int,
                                          responseCallback: InitPidCallback,
                                          metadata: TransactionMetadata) = {

    metadata synchronized {
      if (metadata.state == Ongoing) {
        // abort the ongoing transaction
        handleEndTransaction(transactionalId,
          metadata.pid,
          metadata.producerEpoch,
          TransactionResult.ABORT,
          (errors: Errors) => {
            if (errors != Errors.NONE) {
              responseCallback(initTransactionError(errors))
            } else {
              // init pid again
              handleInitPid(transactionalId, transactionTimeoutMs, responseCallback)
            }
          })
      } else if (metadata.state == PrepareAbort || metadata.state == PrepareCommit) {
        // wait for the commit to complete and then init pid again
        txnMarkerPurgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata, (errors: Errors) => {
          if (errors != Errors.NONE)
            responseCallback(initTransactionError(errors))
          else
            handleInitPid(transactionalId, transactionTimeoutMs, responseCallback)
        }), Seq(metadata.pid))
      } else {
        metadata.producerEpoch = (metadata.producerEpoch + 1).toShort
        metadata.txnTimeoutMs = transactionTimeoutMs
        metadata.topicPartitions.clear()
        metadata.lastUpdateTimestamp = time.milliseconds()
        metadata.state = Empty
        appendMetadataToLog(transactionalId, metadata, responseCallback)
      }
    }
  }

  private def validateTransactionalId(transactionalId: String): Errors =
    if (transactionalId == null || transactionalId.isEmpty)
      Errors.INVALID_REQUEST
    else if (!txnManager.isCoordinatorFor(transactionalId))
      Errors.NOT_COORDINATOR
    else if (txnManager.isCoordinatorLoadingInProgress(transactionalId))
      Errors.COORDINATOR_LOAD_IN_PROGRESS
    else
      Errors.NONE


  def handleAddPartitionsToTransaction(transactionalId: String,
                                       pid: Long,
                                       epoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: TxnMetadataUpdateCallback): Unit = {
    val errors = validateTransactionalId(transactionalId)
    if (errors != Errors.NONE)
      responseCallback(errors)
    else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid pid mapping error.
      val (error, newMetadata) = txnManager.getTransactionState(transactionalId) match {
        case None =>
          (Errors.INVALID_PID_MAPPING, null)

        case Some(metadata) =>
          // generate the new transaction metadata with added partitions
          metadata synchronized {
            if (metadata.pid != pid) {
              (Errors.INVALID_PID_MAPPING, null)
            } else if (metadata.producerEpoch != epoch) {
              (Errors.INVALID_PRODUCER_EPOCH, null)
            } else if (metadata.pendingState.isDefined) {
              // return a retriable exception to let the client backoff and retry
              (Errors.CONCURRENT_TRANSACTIONS, null)
            } else if (metadata.state != Empty && metadata.state != Ongoing) {
              (Errors.INVALID_TXN_STATE, null)
            } else if (partitions.subsetOf(metadata.topicPartitions)) {
              // this is an optimization: if the partitions are already in the metadata reply OK immediately
              (Errors.NONE, null)
            } else {
              val now = time.milliseconds()
              val newMetadata = new TransactionMetadata(pid,
                epoch,
                metadata.txnTimeoutMs,
                Ongoing,
                metadata.topicPartitions ++ partitions,
                if (metadata.state == Empty) now else metadata.transactionStartTime,
                now)
              metadata.prepareTransitionTo(Ongoing)
              (Errors.NONE, newMetadata)
            }
          }
      }

      if (newMetadata != null) {
        appendToLogInReadLock(transactionalId, newMetadata, responseCallback)
      } else {
        responseCallback(error)
      }
    }
  }

  def handleTxnImmigration(transactionStateTopicPartitionId: Int, coordinatorEpoch: Int) {
    inWriteLock(coordinatorLock) {
      txnManager.loadTransactionsForPartition(transactionStateTopicPartitionId, coordinatorEpoch)
    }
  }

  def handleTxnEmigration(transactionStateTopicPartitionId: Int) {
    inWriteLock(coordinatorLock) {
      txnManager.removeTransactionsForPartition(transactionStateTopicPartitionId)
      txnMarkerChannelManager.removeStateForPartition(transactionStateTopicPartitionId)
    }
  }

  def handleEndTransaction(transactionalId: String,
                           pid: Long,
                           epoch: Short,
                           command: TransactionResult,
                           responseCallback: EndTxnCallback): Unit = {
    val errors = validateTransactionalId(transactionalId)
    if (errors != Errors.NONE)
      responseCallback(errors)
    else
      txnManager.getTransactionState(transactionalId) match {
        case None =>
          responseCallback(Errors.INVALID_PID_MAPPING)
        case Some(metadata) =>
          metadata synchronized {
            if (metadata.pid != pid)
              responseCallback(Errors.INVALID_PID_MAPPING)
            else if (metadata.producerEpoch != epoch)
              responseCallback(Errors.INVALID_PRODUCER_EPOCH)
            else metadata.state match {
              case Ongoing =>
                commitOrAbort(transactionalId, pid, epoch, command, responseCallback, metadata)
              case CompleteCommit =>
                if (command == TransactionResult.COMMIT)
                  responseCallback(Errors.NONE)
                else
                  responseCallback(Errors.INVALID_TXN_STATE)
              case CompleteAbort =>
                if (command == TransactionResult.ABORT)
                  responseCallback(Errors.NONE)
                else
                  responseCallback(Errors.INVALID_TXN_STATE)
              case _ =>
                responseCallback(Errors.INVALID_TXN_STATE)
            }
          }
      }
  }

  private def appendToLogInReadLock(transactionalId: String,
                                   metadata: TransactionMetadata,
                                   callback: Errors =>Unit): Unit = {
    def unlockCallback(errors:Errors): Unit = {
      coordinatorLock.readLock().unlock()
      callback(errors)
    }
    coordinatorLock.readLock().lock()
    try {
      txnManager.appendTransactionToLog(transactionalId,
        metadata,
        unlockCallback)
    } catch {
      case _:Throwable => coordinatorLock.readLock().unlock()
    }

  }
  private def commitOrAbort(transactionalId: String,
                            pid: Long,
                            epoch: Short,
                            command: TransactionResult,
                            responseCallback: EndTxnCallback,
                            metadata: TransactionMetadata) = {
    val nextState = if (command == TransactionResult.COMMIT) PrepareCommit else PrepareAbort
    val newMetadata = new TransactionMetadata(pid,
      epoch,
      metadata.txnTimeoutMs,
      nextState,
      metadata.topicPartitions,
      metadata.transactionStartTime,
      time.milliseconds())
    metadata.prepareTransitionTo(nextState)

    def logAppendCallback(errors: Errors): Unit = {
      // we can respond to the client immediately and continue to write the txn markers if
      // the log append was successful
      responseCallback(errors)
      if (errors == Errors.NONE)
        txnManager.coordinatorEpochFor(transactionalId) match {
          case Some(coordinatorEpoch) =>
            def completionCallback(error: Errors): Unit = {
              error match {
                case Errors.NONE =>
                  txnManager.getTransactionState(transactionalId) match {
                    case Some(preparedCommitMetadata) =>
                      val completedState = if (nextState == PrepareCommit) CompleteCommit else CompleteAbort
                      val committedMetadata = new TransactionMetadata(pid,
                        epoch,
                        preparedCommitMetadata.txnTimeoutMs,
                        completedState,
                        preparedCommitMetadata.topicPartitions,
                        preparedCommitMetadata.transactionStartTime,
                        time.milliseconds())
                      preparedCommitMetadata.prepareTransitionTo(completedState)

                      def writeCommittedTransactionCallback(error: Errors): Unit =
                        error match {
                          case Errors.NONE =>
                            txnMarkerChannelManager.removeCompleted(txnManager.partitionFor(transactionalId), pid)
                          case Errors.NOT_COORDINATOR =>
                            // this one should be completed by the new coordinator
                            warn(s"no longer the coordinator for transactionalId: $transactionalId")
                          case _ =>
                            warn(s"error: $error caught for transactionalId: $transactionalId when appending state: $completedState. retrying")
                            // retry until success
                            appendToLogInReadLock(transactionalId, committedMetadata, writeCommittedTransactionCallback)
                        }

                      appendToLogInReadLock(transactionalId, committedMetadata, writeCommittedTransactionCallback)
                    case None =>
                      // this one should be completed by the new coordinator
                      warn(s"no longer the coordinator for transactionalId: $transactionalId")
                  }
                case Errors.NOT_COORDINATOR =>
                  warn(s"no longer the coordinator for transactionalId: $transactionalId")
                case _ =>
                  warn(s"error: $error caught when writing transaction markers for transactionalId: $transactionalId. retrying")
                  txnMarkerChannelManager.addTxnMarkerRequest(txnManager.partitionFor(transactionalId),
                    newMetadata,
                    coordinatorEpoch,
                    completionCallback)
              }
            }

            txnMarkerChannelManager.addTxnMarkerRequest(txnManager.partitionFor(transactionalId), newMetadata, coordinatorEpoch, completionCallback)
          case None =>
            // this one should be completed by the new coordinator
            warn(s"no longer the coordinator for transactionalId: $transactionalId")
        }
    }

    txnManager.appendTransactionToLog(transactionalId, newMetadata, logAppendCallback)
  }

  def transactionTopicConfigs: Properties = txnManager.transactionTopicConfigs

  def partitionFor(transactionalId: String): Int = txnManager.partitionFor(transactionalId)

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enablePidExpiration: Boolean = true) {
    info("Starting up.")
    if (enablePidExpiration)
      txnManager.enablePidExpiration()
    txnMarkerChannelManager.start()
    isActive.set(true)

    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    pidManager.shutdown()
    txnManager.shutdown()
    txnMarkerChannelManager.shutdown()
    info("Shutdown complete.")
  }
}

case class InitPidResult(pid: Long, epoch: Short, error: Errors)
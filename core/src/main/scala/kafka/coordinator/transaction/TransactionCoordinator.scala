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
    val txnStateManager = new TransactionStateManager(config.brokerId, zkUtils, scheduler, replicaManager, txnConfig, time)
    val txnMarkerPurgatory = DelayedOperationPurgatory[DelayedTxnMarker]("txn-marker-purgatory", config.brokerId)
    val txnMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnStateManager, txnMarkerPurgatory, time)

    new TransactionCoordinator(config.brokerId, pidManager, txnStateManager, txnMarkerChannelManager, txnMarkerPurgatory, time)
  }

  private def initTransactionError(error: Errors): InitPidResult = {
    InitPidResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TransactionMetadata): InitPidResult = {
    InitPidResult(txnMetadata.producerId, txnMetadata.producerEpoch, Errors.NONE)
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
            val newMetadata: TransactionMetadata = new TransactionMetadata(producerId = pid,
              producerEpoch = 0,
              txnTimeoutMs = transactionTimeoutMs,
              state = Empty,
              topicPartitions = collection.mutable.Set.empty[TopicPartition],
              txnLastUpdateTimestamp = time.milliseconds())

            val epochAndMetadata = txnManager.addTransaction(transactionalId, newMetadata)

            // there might be a concurrent thread that has just updated the mapping
            // with the transactional id at the same time; in this case we will
            // treat it as the metadata has existed and update it accordingly
            epochAndMetadata synchronized {
              if (epochAndMetadata.transactionMetadata.pendingTransitionInProgress)
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
              else if (!epochAndMetadata.transactionMetadata.equals(newMetadata))
                initPidWithExistingMetadata(transactionalId, transactionTimeoutMs, responseCallback, epochAndMetadata)
              else
                initPidWithNewMetadata(transactionalId, responseCallback, epochAndMetadata)
            }
          case Some(existingEpochAndMetadata) =>
            existingEpochAndMetadata synchronized {
              initPidWithExistingMetadata(transactionalId, transactionTimeoutMs, responseCallback, existingEpochAndMetadata)
            }
        }
      }
  }

  private def initPidWithNewMetadata(transactionalId: String,
                                     initPidCallback: InitPidCallback,
                                     epochAndMetadata: CoordinatorEpochAndTxnMetadata): Unit = {
    val metadata = epochAndMetadata.transactionMetadata

    def callback(errors: Errors): Unit = {
      if (errors == Errors.NONE)
        initPidCallback(initTransactionMetadata(metadata))
      else
        initPidCallback(initTransactionError(errors))
    }

    // set the pending state to Empty again to allow the metadata to be updated
    // after it was successfully written to log
    metadata.prepareTransitionTo(Empty)

    txnManager.appendTransactionToLog(transactionalId, epochAndMetadata.coordinatorEpoch, epochAndMetadata.transactionMetadata, callback)
  }


  private def initPidWithExistingMetadata(transactionalId: String,
                                          transactionTimeoutMs: Int,
                                          responseCallback: InitPidCallback,
                                          epochAndMetadata: CoordinatorEpochAndTxnMetadata) = {

    val metadata = epochAndMetadata.transactionMetadata

    if (metadata.state == Ongoing) {
      // abort the ongoing transaction and then return CONCURRENT_TRANSACTIONS to let client wait and retry
      def callback(errors: Errors): Unit = {
        if (errors != Errors.NONE) {
          responseCallback(initTransactionError(errors))
        } else {
          responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
        }
      }

      handleEndTransaction(transactionalId,
        metadata.producerId,
        metadata.producerEpoch,
        TransactionResult.ABORT,
        callback)
    } else if (metadata.state == PrepareAbort || metadata.state == PrepareCommit) {
      responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
    } else {
      // update the metadata with incremented epoch and timeout value, also updated the timestamp
      // and then write to log as the new pid
      val newMetadata = metadata.copy()
      newMetadata.producerEpoch = (metadata.producerEpoch + 1).toShort
      newMetadata.txnTimeoutMs = transactionTimeoutMs
      newMetadata.topicPartitions.clear()
      newMetadata.txnLastUpdateTimestamp = time.milliseconds()
      newMetadata.state = Empty

      metadata.prepareTransitionTo(Empty)

      initPidWithNewMetadata(transactionalId, responseCallback, epochAndMetadata)
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
      val (error: Errors, coordinatorEpoch: Int, txnMetadata: TransactionMetadata) = txnManager.getTransactionState(transactionalId) match {
        case None =>
          (Errors.INVALID_PID_MAPPING, null, null)

        case Some(epochAndMetadata) =>
          // generate the new transaction metadata with added partitions
          epochAndMetadata synchronized {
            val metadata = epochAndMetadata.transactionMetadata

            if (metadata.producerId != pid) {
              (Errors.INVALID_PID_MAPPING, null)
            } else if (metadata.producerEpoch != epoch) {
              (Errors.INVALID_PRODUCER_EPOCH, null)
            } else if (metadata.pendingState.isDefined) {
              // return a retriable exception to let the client backoff and retry
              (Errors.CONCURRENT_TRANSACTIONS, null)
            } else if (metadata.state == PrepareCommit || metadata.state == PrepareAbort) {
              (Errors.CONCURRENT_TRANSACTIONS, null)
            } else {
              if (metadata.state == CompleteAbort || metadata.state == CompleteCommit)
                metadata.topicPartitions.clear()
              if (partitions.subsetOf(metadata.topicPartitions)) {
                // this is an optimization: if the partitions are already in the metadata reply OK immediately
                (Errors.NONE, null)
              } else {
                val now = time.milliseconds()
                val newMetadata = new TransactionMetadata(pid,
                  epoch,
                  metadata.txnTimeoutMs,
                  Ongoing,
                  metadata.topicPartitions ++ partitions,
                  if (metadata.state == Empty || metadata.state == CompleteCommit || metadata.state == CompleteAbort)
                    now
                  else
                    metadata.txnStartTimestamp,
                  now)
                metadata.prepareTransitionTo(Ongoing)

                (Errors.NONE, epochAndMetadata.coordinatorEpoch, newMetadata)
              }
            }
          }
      }

      if (txnMetadata != null) {
        txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, txnMetadata, responseCallback)
      } else {
        responseCallback(error)
      }
    }
  }

  def handleTxnImmigration(txnTopicPartitionId: Int, coordinatorEpoch: Int) {
      txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch)
  }

  def handleTxnEmigration(txnTopicPartitionId: Int) {
      txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId)
      txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
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
        case Some(epochAndTxnMetadata) =>
          val metadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch
          metadata synchronized {
            if (metadata.producerId != pid)
              responseCallback(Errors.INVALID_PID_MAPPING)
            else if (metadata.producerEpoch != epoch)
              responseCallback(Errors.INVALID_PRODUCER_EPOCH)
            else metadata.state match {
              case Ongoing =>
                commitOrAbort(transactionalId, pid, epoch, coordinatorEpoch, metadata, command, responseCallback)
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

  private def commitOrAbort(transactionalId: String,
                            pid: Long,
                            producerEpoch: Short,
                            coordinatorEpoch: Int,
                            metadata: TransactionMetadata,
                            command: TransactionResult,
                            responseCallback: EndTxnCallback): Unit = {

    val nextState = if (command == TransactionResult.COMMIT) PrepareCommit else PrepareAbort
    val newMetadata = metadata.copy()
    newMetadata.state = nextState
    newMetadata.txnLastUpdateTimestamp = time.milliseconds()
    metadata.prepareTransitionTo(nextState)

    def sendTxnMarkersCallback(errors: Errors): Unit = {
      // we can respond to the client immediately and continue to write the txn markers if
      // the log append was successful
      responseCallback(errors)

      val nextState = if (command == TransactionResult.COMMIT) CompleteCommit else CompleteAbort
      val newMetadata = metadata.copy()
      newMetadata.state = nextState
      newMetadata.txnLastUpdateTimestamp = time.milliseconds()
      metadata.prepareTransitionTo(nextState)

      if (errors == Errors.NONE)
        txnMarkerChannelManager.addTxnMarkersToSend(transactionalId, coordinatorEpoch, command, newMetadata)
    }

    txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback)
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
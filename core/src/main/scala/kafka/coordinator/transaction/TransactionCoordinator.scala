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

import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.{Logging, Scheduler, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.Time

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

  private def initTransactionMetadata(txnMetadata: TransactionMetadataTransition): InitPidResult = {
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
  type AddPartitionsCallback = Errors => Unit
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
        val result: Either[InitPidResult, (Int, TransactionMetadataTransition)] = txnManager.getTransactionState(transactionalId) match {
          case None =>
            val pid = pidManager.nextPid()
            val now = time.milliseconds()
            val createdMetadata  = new TransactionMetadata(producerId = pid,
              producerEpoch = 0,
              txnTimeoutMs = transactionTimeoutMs,
              state = Empty,
              topicPartitions = collection.mutable.Set.empty[TopicPartition],
              txnLastUpdateTimestamp = now)

            val epochAndMetadata = txnManager.addTransaction(transactionalId, createdMetadata)
            val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
            val txnMetadata = epochAndMetadata.transactionMetadata

            // there might be a concurrent thread that has just updated the mapping
            // with the transactional id at the same time (hence reference equality will fail);
            // in this case we will treat it as the metadata has existed already
            txnMetadata synchronized {
              if (!txnMetadata.eq(createdMetadata)) {
                initPidWithExistingMetadata(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata)
              } else {
                Right(coordinatorEpoch, txnMetadata.prepareNewPid(time.milliseconds()))
              }
            }

          case Some(existingEpochAndMetadata) =>
            val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
            val txnMetadata = existingEpochAndMetadata.transactionMetadata

            txnMetadata synchronized {
              initPidWithExistingMetadata(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata)
            }
        }

        result match {
          case Left(pidResult) =>
            responseCallback(pidResult)

          case Right((coordinatorEpoch, newMetadata)) =>
            if (newMetadata.txnState == Ongoing) {
              // abort the ongoing transaction and then return CONCURRENT_TRANSACTIONS to let client wait and retry
              def sendRetriableErrorCallback(error: Errors): Unit = {
                if (error != Errors.NONE) {
                  responseCallback(initTransactionError(error))
                } else {
                  responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
                }
              }

              handleEndTransaction(transactionalId,
                newMetadata.producerId,
                newMetadata.producerEpoch,
                TransactionResult.ABORT,
                sendRetriableErrorCallback)
            } else {
              def sendPidResponseCallback(error: Errors): Unit = {
                if (error == Errors.NONE)
                  responseCallback(initTransactionMetadata(newMetadata))
                else
                  responseCallback(initTransactionError(error))
              }

              txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendPidResponseCallback)
            }
        }
      }
  }

  private def initPidWithExistingMetadata(transactionalId: String,
                                          transactionTimeoutMs: Int,
                                          coordinatorEpoch: Int,
                                          txnMetadata: TransactionMetadata): Either[InitPidResult, (Int, TransactionMetadataTransition)] = {

    if (txnMetadata.pendingTransitionInProgress) {
      // return a retriable exception to let the client backoff and retry
      Left(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
    } else {
      // caller should have synchronized on txnMetadata already
      txnMetadata.state match {
        case PrepareAbort | PrepareCommit =>
          // reply to client and let client backoff and retry
          Left(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))

        case CompleteAbort | CompleteCommit | Empty =>
          // try to append and then update
          Right(coordinatorEpoch, txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, time.milliseconds()))

        case _ =>
          // indicate to abort the current ongoing txn first
          Right(coordinatorEpoch, txnMetadata.prepareNoTransit())
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
                                       responseCallback: AddPartitionsCallback): Unit = {
    val error = validateTransactionalId(transactionalId)
    if (error != Errors.NONE) {
      responseCallback(error)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid pid mapping error.
      val result: Either[Errors, (Int, TransactionMetadataTransition)] = txnManager.getTransactionState(transactionalId) match {
        case None =>
          Left(Errors.INVALID_PID_MAPPING)

        case Some(epochAndMetadata) =>
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // generate the new transaction metadata with added partitions
          txnMetadata synchronized {
            if (txnMetadata.producerId != pid) {
              Left(Errors.INVALID_PID_MAPPING)
            } else if (txnMetadata.producerEpoch != epoch) {
              Left(Errors.INVALID_PRODUCER_EPOCH)
            } else if (txnMetadata.pendingTransitionInProgress) {
              // return a retriable exception to let the client backoff and retry
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (partitions.subsetOf(txnMetadata.topicPartitions)) {
              // this is an optimization: if the partitions are already in the metadata reply OK immediately
              Left(Errors.NONE)
            } else {
              Right(coordinatorEpoch, txnMetadata.prepareAddPartitions(partitions.toSet, time.milliseconds()))
            }
          }
      }

      result match {
        case Left(err) =>
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, responseCallback)
      }
    }
  }

  def handleTxnImmigration(txnTopicPartitionId: Int, coordinatorEpoch: Int) {
      txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch, sendTxnMarkers)
  }

  def handleTxnEmigration(txnTopicPartitionId: Int) {
      txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId)
      txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }

  def handleEndTransaction(transactionalId: String,
                           pid: Long,
                           epoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback): Unit = {
    val error = validateTransactionalId(transactionalId)
    if (error != Errors.NONE)
      responseCallback(error)
    else {
      val result: Either[Errors, (Int, TransactionMetadataTransition)] = txnManager.getTransactionState(transactionalId) match {
        case None =>
          Left(Errors.INVALID_PID_MAPPING)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch

          txnMetadata synchronized {
            if (txnMetadata.producerId != pid)
              Left(Errors.INVALID_PID_MAPPING)
            else if (txnMetadata.producerEpoch != epoch)
              Left(Errors.INVALID_PRODUCER_EPOCH)
            else if (txnMetadata.pendingTransitionInProgress)
              Left(Errors.CONCURRENT_TRANSACTIONS)
            else txnMetadata.state match {
              case Ongoing =>
                val nextState = if (txnMarkerResult == TransactionResult.COMMIT)
                  PrepareCommit
                else
                  PrepareAbort
                Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))
              case CompleteCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  // this is an optimization: return OK immediately
                  Left(Errors.NONE)
                else
                  Left(Errors.INVALID_TXN_STATE)
              case CompleteAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                // this is an optimization: return OK immediately
                  Left(Errors.NONE)
                else
                  Left(Errors.INVALID_TXN_STATE)
              case _ =>
                Left(Errors.INVALID_TXN_STATE)
            }
          }
      }

      result match {
        case Left(err) =>
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          def sendTxnMarkersCallback(error: Errors): Unit = {
            // we can respond to the client immediately and continue to write the txn markers if
            // the log append was successful
            responseCallback(error)

            if (error == Errors.NONE) {
              txnManager.getTransactionState(transactionalId) match {
                case Some(epochAndMetadata) =>
                  val metadata = epochAndMetadata.transactionMetadata

                  sendTxnMarkers(transactionalId, coordinatorEpoch, metadata, txnMarkerResult)

                case None =>
                  // this transactional id no longer exists, maybe the corresponding partition has already been migrated out.
                  info(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed after the transaction message " +
                    s"has been appended to the log. The partition ${partitionFor(transactionalId)} may have migrated as the metadata is no longer in the cache")
              }
            }
          }

          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback)
      }
    }
  }

  private def sendTxnMarkers(transactionalId: String,
                             coordinatorEpoch: Int,
                             txnMetadata: TransactionMetadata,
                             txnMarkerResult: TransactionResult): Unit = {

    val newMetadata = txnMetadata synchronized {
      txnMetadata.prepareComplete(time.milliseconds())
    }

    txnMarkerChannelManager.addTxnMarkersToSend(transactionalId, coordinatorEpoch, txnMarkerResult, txnMetadata, newMetadata)
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
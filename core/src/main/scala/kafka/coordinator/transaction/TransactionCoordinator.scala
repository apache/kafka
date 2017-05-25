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
      config.transactionTopicMinISR,
      config.transactionTransactionsExpiredTransactionCleanupIntervalMs)

    val producerIdManager = new ProducerIdManager(config.brokerId, zkUtils)
    // we do not need to turn on reaper thread since no tasks will be expired and there are no completed tasks to be purged
    val txnMarkerPurgatory = DelayedOperationPurgatory[DelayedTxnMarker]("txn-marker-purgatory", config.brokerId, reaperEnabled = false, timerEnabled = false)
    val txnStateManager = new TransactionStateManager(config.brokerId, zkUtils, scheduler, replicaManager, txnConfig, time)
    val txnMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnStateManager, txnMarkerPurgatory, time)

    new TransactionCoordinator(config.brokerId, scheduler, producerIdManager, txnStateManager, txnMarkerChannelManager, time)
  }

  private def initTransactionError(error: Errors): InitProducerIdResult = {
    InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TxnTransitMetadata): InitProducerIdResult = {
    InitProducerIdResult(txnMetadata.producerId, txnMetadata.producerEpoch, Errors.NONE)
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
                             scheduler: Scheduler,
                             producerIdManager: ProducerIdManager,
                             txnManager: TransactionStateManager,
                             txnMarkerChannelManager: TransactionMarkerChannelManager,
                             time: Time) extends Logging {
  this.logIdent = "[Transaction Coordinator " + brokerId + "]: "

  import TransactionCoordinator._

  type InitProducerIdCallback = InitProducerIdResult => Unit
  type AddPartitionsCallback = Errors => Unit
  type EndTxnCallback = Errors => Unit

  /* Active flag of the coordinator */
  private val isActive = new AtomicBoolean(false)

  def handleInitProducerId(transactionalId: String,
                           transactionTimeoutMs: Int,
                           responseCallback: InitProducerIdCallback): Unit = {

    if (transactionalId == null) {
      // if the transactional id is null, then always blindly accept the request
      // and return a new producerId from the producerId manager
      val producerId = producerIdManager.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
    } else if (transactionalId.isEmpty) {
      // if transactional id is empty then return error as invalid request. This is
      // to make TransactionCoordinator's behavior consistent with producer client
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
      // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    } else {
      val producerId = producerIdManager.generateProducerId()
      val now = time.milliseconds()
      val createdMetadata = new TransactionMetadata(transactionalId = transactionalId,
        producerId = producerId,
        producerEpoch = 0,
        txnTimeoutMs = transactionTimeoutMs,
        state = Empty,
        topicPartitions = collection.mutable.Set.empty[TopicPartition],
        txnLastUpdateTimestamp = now)

      // only try to get a new producerId and update the cache if the transactional id is unknown
      val result: Either[InitProducerIdResult, (Int, TxnTransitMetadata)] = txnManager.getAndMaybeAddTransactionState(transactionalId, Some(createdMetadata)) match {
        case Left(err) =>
          Left(initTransactionError(err))

        case Right(Some(existingEpochAndMetadata)) =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata

          // there might be a concurrent thread that has just updated the mapping
          // with the transactional id at the same time (hence reference equality will fail);
          // in this case we will treat it as the metadata has existed already
          txnMetadata synchronized {
            if (!txnMetadata.eq(createdMetadata)) {
              initProducerIdWithExistingMetadata(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata)
            } else {
              Right(coordinatorEpoch, txnMetadata.prepareNewProducerId(time.milliseconds()))
            }
          }

        case Right(None) =>
          throw new IllegalStateException("Trying to add metadata to the cache still returns NONE; this is not expected")
      }

      result match {
        case Left(producerIdResult) =>
          responseCallback(producerIdResult)

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

  private def initProducerIdWithExistingMetadata(transactionalId: String,
                                                 transactionTimeoutMs: Int,
                                                 coordinatorEpoch: Int,
                                                 txnMetadata: TransactionMetadata): Either[InitProducerIdResult, (Int, TxnTransitMetadata)] = {
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

        case Ongoing =>
          // indicate to abort the current ongoing txn first
          Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())
      }
    }
  }

  def handleAddPartitionsToTransaction(transactionalId: String,
                                       producerId: Long,
                                       producerEpoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: AddPartitionsCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      responseCallback(Errors.INVALID_REQUEST)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid producerId mapping error.
      val result: Either[Errors, (Int, TxnTransitMetadata)] = txnManager.getAndMaybeAddTransactionState(transactionalId) match {
        case Left(err) =>
          Left(err)

        case Right(None) =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Right(Some(epochAndMetadata)) =>
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // generate the new transaction metadata with added partitions
          txnMetadata synchronized {
            if (txnMetadata.producerId != producerId) {
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.producerEpoch != producerEpoch) {
              Left(Errors.INVALID_PRODUCER_EPOCH)
            } else if (txnMetadata.pendingTransitionInProgress) {
              // return a retriable exception to let the client backoff and retry
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == Ongoing && partitions.subsetOf(txnMetadata.topicPartitions)) {
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
      txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch, txnMarkerChannelManager.addTxnMarkersToSend)
  }

  def handleTxnEmigration(txnTopicPartitionId: Int, coordinatorEpoch: Int) {
      txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch)
      txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }

  private def logInvalidStateTransitionAndReturnError(transactionalId: String,
                                                      transactionState: TransactionState,
                                                      transactionResult: TransactionResult) = {
    debug(s"TransactionalId: $transactionalId's state is $transactionState, but received transaction " +
      s"marker result to send: $transactionResult")
    Left(Errors.INVALID_TXN_STATE)
  }

  def handleEndTransaction(transactionalId: String,
                           producerId: Long,
                           producerEpoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty)
      responseCallback(Errors.INVALID_REQUEST)
    else {
      val preAppendResult: Either[Errors, (Int, TxnTransitMetadata)] = txnManager.getAndMaybeAddTransactionState(transactionalId) match {
        case Left(err) =>
          Left(err)

        case Right(None) =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Right(Some(epochAndTxnMetadata)) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch
          val now = time.milliseconds()

          txnMetadata synchronized {
            if (txnMetadata.producerId != producerId)
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            else if (txnMetadata.producerEpoch != producerEpoch)
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
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case CompleteAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Empty =>
                logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
            }
          }
      }

      preAppendResult match {
        case Left(err) =>
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          def sendTxnMarkersCallback(error: Errors): Unit = {
            if (error == Errors.NONE) {
              val preSendResult: Either[Errors, (TransactionMetadata, TxnTransitMetadata)] = txnManager.getAndMaybeAddTransactionState(transactionalId) match {
                case Left(err) =>
                  Left(err)

                case Right(Some(epochAndMetadata)) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                    val txnMetadata = epochAndMetadata.transactionMetadata
                    txnMetadata synchronized {
                      if (txnMetadata.producerId != producerId)
                        Left(Errors.INVALID_PRODUCER_ID_MAPPING)
                      else if (txnMetadata.producerEpoch != producerEpoch)
                        Left(Errors.INVALID_PRODUCER_EPOCH)
                      else if (txnMetadata.pendingTransitionInProgress)
                        Left(Errors.CONCURRENT_TRANSACTIONS)
                      else txnMetadata.state match {
                        case Empty| Ongoing | CompleteCommit | CompleteAbort =>
                          logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                        case PrepareCommit =>
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case PrepareAbort =>
                          if (txnMarkerResult != TransactionResult.ABORT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                      }
                    }
                  } else {
                    info(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed since the transaction coordinator epoch " +
                      s"has been changed to ${epochAndMetadata.coordinatorEpoch} after the transaction metadata has been successfully appended to the log")

                    Left(Errors.NOT_COORDINATOR)
                  }

                case Right(None) =>
                  throw new IllegalStateException(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                    s"no metadata in the cache; this is not expected")
              }

              preSendResult match {
                case Left(err) =>
                  responseCallback(err)

                case Right((txnMetadata, newPreSendMetadata)) =>
                  // we can respond to the client immediately and continue to write the txn markers if
                  // the log append was successful
                  responseCallback(Errors.NONE)

                  txnMarkerChannelManager.addTxnMarkersToSend(transactionalId, coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else {
              info(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed since the transaction message " +
                s"cannot be appended to the log. Returning error code $error to the client")

              responseCallback(error)
            }
          }

          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback)
      }
    }
  }

  def transactionTopicConfigs: Properties = txnManager.transactionTopicConfigs

  def partitionFor(transactionalId: String): Int = txnManager.partitionFor(transactionalId)

  private def expireTransactions(): Unit = {
    txnManager.transactionsToExpire().foreach { txnIdAndPidEpoch =>
      handleEndTransaction(txnIdAndPidEpoch.transactionalId,
        txnIdAndPidEpoch.producerId,
        txnIdAndPidEpoch.producerEpoch,
        TransactionResult.ABORT,
        (error: Errors) => error match {
          case Errors.NONE =>
            debug(s"Completed rollback ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} due to timeout")
          case Errors.INVALID_PRODUCER_ID_MAPPING |
               Errors.INVALID_PRODUCER_EPOCH |
               Errors.CONCURRENT_TRANSACTIONS =>
            debug(s"Rolling back ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} has aborted due to ${error.exceptionName()}")
          case e =>
            warn(s"Rolling back ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} failed due to ${error.exceptionName()}")
        })
    }
  }

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enablePidExpiration: Boolean = true) {
    info("Starting up.")
    scheduler.startup()
    scheduler.schedule("transaction-expiration",
      expireTransactions,
      TransactionStateManager.DefaultRemoveExpiredTransactionsIntervalMs,
      TransactionStateManager.DefaultRemoveExpiredTransactionsIntervalMs
    )
    if (enablePidExpiration)
      txnManager.enableProducerIdExpiration()
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
    scheduler.shutdown()
    producerIdManager.shutdown()
    txnManager.shutdown()
    txnMarkerChannelManager.shutdown()
    info("Shutdown complete.")
  }
}

case class InitProducerIdResult(producerId: Long, producerEpoch: Short, error: Errors)

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

import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.{Logging, Scheduler, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.utils.Time

import scala.collection.concurrent

object TransactionCoordinator {

  def apply(config: KafkaConfig,
            replicaManager: ReplicaManager,
            scheduler: Scheduler,
            zkUtils: ZkUtils,
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

    new TransactionCoordinator(config, pidManager, logManager)
  }

  private def initTransactionError(error: Errors): InitPidResult = {
    InitPidResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TransactionMetadata): InitPidResult = {
    InitPidResult(txnMetadata.pid, txnMetadata.epoch, Errors.NONE)
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
class TransactionCoordinator(config: KafkaConfig,
                             pidManager: ProducerIdManager,
                             txnManager: TransactionStateManager) extends Logging {
  this.logIdent = "[Transaction Coordinator " + config.brokerId + "]: "

  import TransactionCoordinator._

  type InitPidCallback = InitPidResult => Unit
  type TxnMetadataUpdateCallback = Errors => Unit

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
      txnManager.getTransaction(transactionalId) match {
        case None =>
          val pid = pidManager.nextPid()
          val newMetadata: TransactionMetadata = new TransactionMetadata(pid = pid,
            epoch = 0,
            txnTimeoutMs = transactionTimeoutMs,
            state = Empty,
            topicPartitions = collection.mutable.Set.empty[TopicPartition])

          val metadata = txnManager.addTransaction(transactionalId, newMetadata)

          // there might be a concurrent thread that has just updated the mapping
          // with the transactional id at the same time; in this case we will
          // treat it as the metadata has existed and update it accordingly
          metadata synchronized {
            if (!metadata.equals(newMetadata))
              metadata.epoch = (metadata.epoch + 1).toShort
          }

          responseCallback(initTransactionMetadata(metadata))

        case Some(metadata) =>
          metadata synchronized {
            metadata.epoch = (metadata.epoch + 1).toShort
          }
          responseCallback(initTransactionMetadata(metadata))
      }
    }
  }

  def handleAddPartitionsToTransaction(transactionalId: String,
                                       pid: Long,
                                       epoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: TxnMetadataUpdateCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      // first check that transactional id is specified
      responseCallback(Errors.INVALID_REQUEST)
    } else if (!txnManager.isCoordinatorFor(transactionalId)) {
      // check if it is the assigned coordinator for the transactional id
      responseCallback(Errors.NOT_COORDINATOR)
    } else if (txnManager.isCoordinatorLoadingInProgress(transactionalId)) {
      // check if the corresponding partition is still being loaded
      responseCallback(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid pid mapping error.
      val (error, newMetadata) = txnManager.getTransaction(transactionalId) match {
        case None =>
          (Errors.INVALID_PID_MAPPING, null)

        case Some(metadata) =>
          // generate the new transaction metadata with added partitions
          metadata synchronized {
            if (metadata.pid != pid) {
              (Errors.INVALID_PID_MAPPING, null)
            } else if (metadata.epoch != epoch) {
              (Errors.INVALID_PRODUCER_EPOCH, null)
            } else if (metadata.pendingState.isDefined) {
              // return a retriable exception to let the client backoff and retry
              (Errors.COORDINATOR_LOAD_IN_PROGRESS, null)
            } else if (!TransactionMetadata.isValidTransition(metadata.state, Ongoing)) {
              (Errors.INVALID_TXN_STATE, null)
            } else if (partitions.subsetOf(metadata.topicPartitions)) {
              // this is an optimization: if the partitions are already in the metadata reply OK immediately
              (Errors.NONE, null)
            } else {
              val newMetadata = new TransactionMetadata(pid, epoch, metadata.txnTimeoutMs, Ongoing, metadata.topicPartitions ++ partitions)
              metadata.prepareTransitionTo(Ongoing)
              (Errors.NONE, newMetadata)
            }
          }
      }

      if (newMetadata != null) {
        txnManager.appendTransactionToLog(transactionalId, newMetadata, responseCallback)
      } else {
        responseCallback(error)
      }
    }
  }

  def handleTxnImmigration(transactionStateTopicPartitionId: Int, coordinatorEpoch: Int) {
    txnManager.loadTransactionsForPartition(transactionStateTopicPartitionId, coordinatorEpoch)
  }

  def handleTxnEmigration(transactionStateTopicPartitionId: Int) {
    txnManager.removeTransactionsForPartition(transactionStateTopicPartitionId)
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
    info("Shutdown complete.")
  }
}

case class InitPidResult(pid: Long, epoch: Short, error: Errors)
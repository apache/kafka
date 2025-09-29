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

import java.util.concurrent.locks.ReentrantLock
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.server.common.TransactionVersion

import scala.collection.{immutable, mutable}


object TransactionState {
  val AllStates: Set[TransactionState] = Set(
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
    PrepareEpochFence
  )

  def fromName(name: String): Option[TransactionState] = {
    AllStates.find(_.name == name)
  }

  def fromId(id: Byte): TransactionState = {
    id match {
      case 0 => Empty
      case 1 => Ongoing
      case 2 => PrepareCommit
      case 3 => PrepareAbort
      case 4 => CompleteCommit
      case 5 => CompleteAbort
      case 6 => Dead
      case 7 => PrepareEpochFence
      case _ => throw new IllegalStateException(s"Unknown transaction state id $id from the transaction status message")
    }
  }
}

private[transaction] sealed trait TransactionState {
  def id: Byte

  /**
   * Get the name of this state. This is exposed through the `DescribeTransactions` API.
   */
  def name: String

  def validPreviousStates: Set[TransactionState]

  def isExpirationAllowed: Boolean = false
}

/**
 * Transaction has not existed yet
 *
 * transition: received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 *             received EndTxnRequest with abort and TransactionV2 enabled => PrepareAbort
 */
private[transaction] case object Empty extends TransactionState {
  val id: Byte = 0
  val name: String = "Empty"
  val validPreviousStates: Set[TransactionState] = Set(Empty, CompleteCommit, CompleteAbort)
  override def isExpirationAllowed: Boolean = true
}

/**
 * Transaction has started and ongoing
 *
 * transition: received EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Ongoing extends TransactionState {
  val id: Byte = 1
  val name: String = "Ongoing"
  val validPreviousStates: Set[TransactionState] = Set(Ongoing, Empty, CompleteCommit, CompleteAbort)
}

/**
 * Group is preparing to commit
 *
 * transition: received acks from all partitions => CompleteCommit
 */
private[transaction] case object PrepareCommit extends TransactionState {
  val id: Byte = 2
  val name: String = "PrepareCommit"
  val validPreviousStates: Set[TransactionState] = Set(Ongoing)
}

/**
 * Group is preparing to abort
 *
 * transition: received acks from all partitions => CompleteAbort
 *
 * Note, In transaction v2, we allow Empty, CompleteCommit, CompleteAbort to transition to PrepareAbort. because the
 * client may not know the txn state on the server side, it needs to send endTxn request when uncertain.
 */
private[transaction] case object PrepareAbort extends TransactionState {
  val id: Byte = 3
  val name: String = "PrepareAbort"
  val validPreviousStates: Set[TransactionState] = Set(Ongoing, PrepareEpochFence, Empty, CompleteCommit, CompleteAbort)
}

/**
 * Group has completed commit
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[transaction] case object CompleteCommit extends TransactionState {
  val id: Byte = 4
  val name: String = "CompleteCommit"
  val validPreviousStates: Set[TransactionState] = Set(PrepareCommit)
  override def isExpirationAllowed: Boolean = true
}

/**
 * Group has completed abort
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[transaction] case object CompleteAbort extends TransactionState {
  val id: Byte = 5
  val name: String = "CompleteAbort"
  val validPreviousStates: Set[TransactionState] = Set(PrepareAbort)
  override def isExpirationAllowed: Boolean = true
}

/**
  * TransactionalId has expired and is about to be removed from the transaction cache
  */
private[transaction] case object Dead extends TransactionState {
  val id: Byte = 6
  val name: String = "Dead"
  val validPreviousStates: Set[TransactionState] = Set(Empty, CompleteAbort, CompleteCommit)
}

/**
  * We are in the middle of bumping the epoch and fencing out older producers.
  */

private[transaction] case object PrepareEpochFence extends TransactionState {
  val id: Byte = 7
  val name: String = "PrepareEpochFence"
  val validPreviousStates: Set[TransactionState] = Set(Ongoing)
}

private[transaction] object TransactionMetadata {
  def isEpochExhausted(producerEpoch: Short): Boolean = producerEpoch >= Short.MaxValue - 1
}

// this is a immutable object representing the target transition of the transaction metadata
private[transaction] case class TxnTransitMetadata(producerId: Long,
                                                   prevProducerId: Long,
                                                   nextProducerId: Long,
                                                   producerEpoch: Short,
                                                   lastProducerEpoch: Short,
                                                   txnTimeoutMs: Int,
                                                   txnState: TransactionState,
                                                   topicPartitions: immutable.Set[TopicPartition],
                                                   txnStartTimestamp: Long,
                                                   txnLastUpdateTimestamp: Long,
                                                   clientTransactionVersion: TransactionVersion) {
  override def toString: String = {
    "TxnTransitMetadata(" +
      s"producerId=$producerId, " +
      s"previousProducerId=$prevProducerId, " +
      s"nextProducerId=$nextProducerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"lastProducerEpoch=$lastProducerEpoch, " +
      s"txnTimeoutMs=$txnTimeoutMs, " +
      s"txnState=$txnState, " +
      s"topicPartitions=$topicPartitions, " +
      s"txnStartTimestamp=$txnStartTimestamp, " +
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp, " +
      s"clientTransactionVersion=$clientTransactionVersion)"
  }
}

/**
  *
  * @param producerId                  producer id
  * @param previousProducerId          producer id for the last committed transaction with this transactional ID
  * @param nextProducerId              Latest producer ID sent to the producer for the given transactional ID
  * @param producerEpoch               current epoch of the producer
  * @param lastProducerEpoch           last epoch of the producer
  * @param txnTimeoutMs                timeout to be used to abort long running transactions
  * @param state                       current state of the transaction
  * @param topicPartitions             current set of partitions that are part of this transaction
  * @param txnStartTimestamp           time the transaction was started, i.e., when first partition is added
  * @param txnLastUpdateTimestamp      updated when any operation updates the TransactionMetadata. To be used for expiration
  * @param clientTransactionVersion    TransactionVersion used by the client when the state was transitioned
  */
@nonthreadsafe
private[transaction] class TransactionMetadata(val transactionalId: String,
                                               var producerId: Long,
                                               var previousProducerId: Long,
                                               var nextProducerId: Long,
                                               var producerEpoch: Short,
                                               var lastProducerEpoch: Short,
                                               var txnTimeoutMs: Int,
                                               var state: TransactionState,
                                               val topicPartitions: mutable.Set[TopicPartition],
                                               @volatile var txnStartTimestamp: Long = -1,
                                               @volatile var txnLastUpdateTimestamp: Long,
                                               var clientTransactionVersion: TransactionVersion) extends Logging {

  // pending state is used to indicate the state that this transaction is going to
  // transit to, and for blocking future attempts to transit it again if it is not legal;
  // initialized as the same as the current state
  var pendingState: Option[TransactionState] = None

  // Indicates that during a previous attempt to fence a producer, the bumped epoch may not have been
  // successfully written to the log. If this is true, we will not bump the epoch again when fencing
  var hasFailedEpochFence: Boolean = false

  private[transaction] val lock = new ReentrantLock

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def addPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    topicPartitions ++= partitions
  }

  def removePartition(topicPartition: TopicPartition): Unit = {
    if (state != PrepareCommit && state != PrepareAbort)
      throw new IllegalStateException(s"Transaction metadata's current state is $state, and its pending state is $pendingState " +
        s"while trying to remove partitions whose txn marker has been sent, this is not expected")

    topicPartitions -= topicPartition
  }

  // this is visible for test only
  def prepareNoTransit(): TxnTransitMetadata = {
    // do not call transitTo as it will set the pending state, a follow-up call to abort the transaction will set its pending state
    TxnTransitMetadata(producerId, previousProducerId, nextProducerId, producerEpoch, lastProducerEpoch, txnTimeoutMs, state, topicPartitions.toSet,
      txnStartTimestamp, txnLastUpdateTimestamp, clientTransactionVersion)
  }

  def prepareFenceProducerEpoch(): TxnTransitMetadata = {
    if (producerEpoch == Short.MaxValue)
      throw new IllegalStateException(s"Cannot fence producer with epoch equal to Short.MaxValue since this would overflow")

    // If we've already failed to fence an epoch (because the write to the log failed), we don't increase it again.
    // This is safe because we never return the epoch to client if we fail to fence the epoch
    val bumpedEpoch = if (hasFailedEpochFence) producerEpoch else (producerEpoch + 1).toShort

    prepareTransitionTo(PrepareEpochFence, producerId, bumpedEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs,
      topicPartitions.toSet, txnStartTimestamp, txnLastUpdateTimestamp, clientTransactionVersion)
  }

  def prepareIncrementProducerEpoch(newTxnTimeoutMs: Int,
                                    expectedProducerEpoch: Option[Short],
                                    updateTimestamp: Long): Either[Errors, TxnTransitMetadata] = {
    if (isProducerEpochExhausted)
      throw new IllegalStateException(s"Cannot allocate any more producer epochs for producerId $producerId")

    val bumpedEpoch = (producerEpoch + 1).toShort
    val epochBumpResult: Either[Errors, (Short, Short)] = expectedProducerEpoch match {
      case None =>
        // If no expected epoch was provided by the producer, bump the current epoch and set the last epoch to -1
        // In the case of a new producer, producerEpoch will be -1 and bumpedEpoch will be 0
        Right(bumpedEpoch, RecordBatch.NO_PRODUCER_EPOCH)

      case Some(expectedEpoch) =>
        if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH || expectedEpoch == producerEpoch)
          // If the expected epoch matches the current epoch, or if there is no current epoch, the producer is attempting
          // to continue after an error and no other producer has been initialized. Bump the current and last epochs.
          // The no current epoch case means this is a new producer; producerEpoch will be -1 and bumpedEpoch will be 0
          Right(bumpedEpoch, producerEpoch)
        else if (expectedEpoch == lastProducerEpoch)
          // If the expected epoch matches the previous epoch, it is a retry of a successful call, so just return the
          // current epoch without bumping. There is no danger of this producer being fenced, because a new producer
          // calling InitProducerId would have caused the last epoch to be set to -1.
          // Note that if the IBP is prior to 2.4.IV1, the lastProducerId and lastProducerEpoch will not be written to
          // the transaction log, so a retry that spans a coordinator change will fail. We expect this to be a rare case.
          Right(producerEpoch, lastProducerEpoch)
        else {
          // Otherwise, the producer has a fenced epoch and should receive an PRODUCER_FENCED error
          info(s"Expected producer epoch $expectedEpoch does not match current " +
            s"producer epoch $producerEpoch or previous producer epoch $lastProducerEpoch")
          Left(Errors.PRODUCER_FENCED)
        }
    }

    epochBumpResult match {
      case Right((nextEpoch, lastEpoch)) => Right(prepareTransitionTo(Empty, producerId, nextEpoch, lastEpoch, newTxnTimeoutMs,
        immutable.Set.empty[TopicPartition], -1, updateTimestamp, clientTransactionVersion))

      case Left(err) => Left(err)
    }
  }

  def prepareProducerIdRotation(newProducerId: Long,
                                newTxnTimeoutMs: Int,
                                updateTimestamp: Long,
                                recordLastEpoch: Boolean): TxnTransitMetadata = {
    if (hasPendingTransaction)
      throw new IllegalStateException("Cannot rotate producer ids while a transaction is still pending")

    prepareTransitionTo(Empty, newProducerId, 0, if (recordLastEpoch) producerEpoch else RecordBatch.NO_PRODUCER_EPOCH,
      newTxnTimeoutMs, immutable.Set.empty[TopicPartition], -1, updateTimestamp, clientTransactionVersion)
  }

  def prepareAddPartitions(addedTopicPartitions: immutable.Set[TopicPartition], updateTimestamp: Long, clientTransactionVersion: TransactionVersion): TxnTransitMetadata = {
    val newTxnStartTimestamp = state match {
      case Empty | CompleteAbort | CompleteCommit => updateTimestamp
      case _ => txnStartTimestamp
    }

    prepareTransitionTo(Ongoing, producerId, producerEpoch, lastProducerEpoch, txnTimeoutMs,
      (topicPartitions ++ addedTopicPartitions).toSet, newTxnStartTimestamp, updateTimestamp, clientTransactionVersion)
  }

  def prepareAbortOrCommit(newState: TransactionState, clientTransactionVersion: TransactionVersion, nextProducerId: Long, updateTimestamp: Long, noPartitionAdded: Boolean): TxnTransitMetadata = {
    val (updatedProducerEpoch, updatedLastProducerEpoch) = if (clientTransactionVersion.supportsEpochBump()) {
      // We already ensured that we do not overflow here. MAX_SHORT is the highest possible value.
      ((producerEpoch + 1).toShort, producerEpoch)
    } else {
      (producerEpoch, lastProducerEpoch)
    }

    // With transaction V2, it is allowed to abort the transaction without adding any partitions. Then, the transaction
    // start time is uncertain but it is still required. So we can use the update time as the transaction start time.
    val newTxnStartTimestamp = if (noPartitionAdded) updateTimestamp else txnStartTimestamp
    prepareTransitionTo(newState, producerId, nextProducerId, updatedProducerEpoch, updatedLastProducerEpoch, txnTimeoutMs, topicPartitions.toSet,
      newTxnStartTimestamp, updateTimestamp, clientTransactionVersion)
  }

  def prepareComplete(updateTimestamp: Long): TxnTransitMetadata = {
    val newState = if (state == PrepareCommit) CompleteCommit else CompleteAbort

    // Since the state change was successfully written to the log, unset the flag for a failed epoch fence
    hasFailedEpochFence = false
    val (updatedProducerId, updatedProducerEpoch) =
      // In the prepareComplete transition for the overflow case, the lastProducerEpoch is kept at MAX-1,
      // which is the last epoch visible to the client.
      // Internally, however, during the transition between prepareAbort/prepareCommit and prepareComplete, the producer epoch
      // reaches MAX but the client only sees the transition as MAX-1 followed by 0.
      // When an epoch overflow occurs, we set the producerId to nextProducerId and reset the epoch to 0,
      // but lastProducerEpoch remains MAX-1 to maintain consistency with what the client last saw.
      if (clientTransactionVersion.supportsEpochBump() && nextProducerId != RecordBatch.NO_PRODUCER_ID) {
        (nextProducerId, 0.toShort)
      } else {
        (producerId, producerEpoch)
      }
    prepareTransitionTo(newState, updatedProducerId, RecordBatch.NO_PRODUCER_ID, updatedProducerEpoch, lastProducerEpoch, txnTimeoutMs, Set.empty[TopicPartition],
      txnStartTimestamp, updateTimestamp, clientTransactionVersion)
  }

  def prepareDead(): TxnTransitMetadata = {
    prepareTransitionTo(Dead, producerId, producerEpoch, lastProducerEpoch, txnTimeoutMs, Set.empty[TopicPartition],
      txnStartTimestamp, txnLastUpdateTimestamp, clientTransactionVersion)
  }

  /**
   * Check if the epochs have been exhausted for the current producerId. We do not allow the client to use an
   * epoch equal to Short.MaxValue to ensure that the coordinator will always be able to fence an existing producer.
   */
  def isProducerEpochExhausted: Boolean = TransactionMetadata.isEpochExhausted(producerEpoch)

  private def hasPendingTransaction: Boolean = {
    state match {
      case Ongoing | PrepareAbort | PrepareCommit => true
      case _ => false
    }
  }

  private def prepareTransitionTo(updatedState: TransactionState,
                                  updatedProducerId: Long,
                                  updatedEpoch: Short,
                                  updatedLastEpoch: Short,
                                  updatedTxnTimeoutMs: Int,
                                  updatedTopicPartitions: immutable.Set[TopicPartition],
                                  updatedTxnStartTimestamp: Long,
                                  updateTimestamp: Long,
                                  clientTransactionVersion: TransactionVersion): TxnTransitMetadata = {
    prepareTransitionTo(updatedState, updatedProducerId, RecordBatch.NO_PRODUCER_ID, updatedEpoch, updatedLastEpoch, updatedTxnTimeoutMs, updatedTopicPartitions, updatedTxnStartTimestamp, updateTimestamp, clientTransactionVersion)
  }

  private def prepareTransitionTo(updatedState: TransactionState,
                                  updatedProducerId: Long,
                                  nextProducerId: Long,
                                  updatedEpoch: Short,
                                  updatedLastEpoch: Short,
                                  updatedTxnTimeoutMs: Int,
                                  updatedTopicPartitions: immutable.Set[TopicPartition],
                                  updatedTxnStartTimestamp: Long,
                                  updateTimestamp: Long,
                                  clientTransactionVersion: TransactionVersion): TxnTransitMetadata = {
    if (pendingState.isDefined)
      throw new IllegalStateException(s"Preparing transaction state transition to $updatedState " +
        s"while it already a pending state ${pendingState.get}")

    if (updatedProducerId < 0)
      throw new IllegalArgumentException(s"Illegal new producer id $updatedProducerId")

    // The epoch is initialized to NO_PRODUCER_EPOCH when the TransactionMetadata
    // is created for the first time and it could stay like this until transitioning
    // to Dead.
    if (updatedState != Dead && updatedEpoch < 0)
      throw new IllegalArgumentException(s"Illegal new producer epoch $updatedEpoch")

    // check that the new state transition is valid and update the pending state if necessary
    if (updatedState.validPreviousStates.contains(state)) {
      val transitMetadata = TxnTransitMetadata(updatedProducerId, producerId, nextProducerId, updatedEpoch, updatedLastEpoch, updatedTxnTimeoutMs, updatedState,
        updatedTopicPartitions, updatedTxnStartTimestamp, updateTimestamp, clientTransactionVersion)
      debug(s"TransactionalId $transactionalId prepare transition from $state to $transitMetadata")
      pendingState = Some(updatedState)
      transitMetadata
    } else {
      throw new IllegalStateException(s"Preparing transaction state transition to $updatedState failed since the target state" +
        s" $updatedState is not a valid previous state of the current state $state")
    }
  }

  def completeTransitionTo(transitMetadata: TxnTransitMetadata): Unit = {
    // metadata transition is valid only if all the following conditions are met:
    //
    // 1. the new state is already indicated in the pending state.
    // 2. the epoch should be either the same value, the old value + 1, or 0 if we have a new producerId.
    // 3. the last update time is no smaller than the old value.
    // 4. the old partitions set is a subset of the new partitions set.
    //
    // plus, we should only try to update the metadata after the corresponding log entry has been successfully
    // written and replicated (see TransactionStateManager#appendTransactionToLog)
    //
    // if valid, transition is done via overwriting the whole object to ensure synchronization

    val toState = pendingState.getOrElse {
      fatal(s"$this's transition to $transitMetadata failed since pendingState is not defined: this should not happen")

      throw new IllegalStateException(s"TransactionalId $transactionalId " +
        "completing transaction state transition while it does not have a pending state")
    }

    if (toState != transitMetadata.txnState) {
      throwStateTransitionFailure(transitMetadata)
    } else {
      toState match {
        case Empty => // from initPid
          if ((producerEpoch != transitMetadata.producerEpoch && !validProducerEpochBump(transitMetadata)) ||
            transitMetadata.topicPartitions.nonEmpty ||
            transitMetadata.txnStartTimestamp != -1) {

            throwStateTransitionFailure(transitMetadata)
          } else {
            txnTimeoutMs = transitMetadata.txnTimeoutMs
            producerEpoch = transitMetadata.producerEpoch
            lastProducerEpoch = transitMetadata.lastProducerEpoch
            producerId = transitMetadata.producerId
            previousProducerId = transitMetadata.prevProducerId
          }

        case Ongoing => // from addPartitions
          if (!validProducerEpoch(transitMetadata) ||
            !topicPartitions.subsetOf(transitMetadata.topicPartitions) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs) {

            throwStateTransitionFailure(transitMetadata)
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp
            addPartitions(transitMetadata.topicPartitions)
          }

        case PrepareAbort | PrepareCommit => // from endTxn
          // In V2, we allow state transits from Empty, CompleteCommit and CompleteAbort to PrepareAbort. It is possible
          // their updated start time is not equal to the current start time.
          val allowedEmptyAbort = toState == PrepareAbort && transitMetadata.clientTransactionVersion.supportsEpochBump() &&
            (state == Empty || state == CompleteCommit || state == CompleteAbort)
          val validTimestamp = txnStartTimestamp == transitMetadata.txnStartTimestamp || allowedEmptyAbort
          if (!validProducerEpoch(transitMetadata) ||
            !topicPartitions.toSet.equals(transitMetadata.topicPartitions) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs || !validTimestamp) {

            throwStateTransitionFailure(transitMetadata)
          } else if (transitMetadata.clientTransactionVersion.supportsEpochBump()) {
            producerEpoch = transitMetadata.producerEpoch
            lastProducerEpoch = transitMetadata.lastProducerEpoch
            nextProducerId = transitMetadata.nextProducerId
            txnStartTimestamp = transitMetadata.txnStartTimestamp
          }

        case CompleteAbort | CompleteCommit => // from write markers
          if (!validProducerEpoch(transitMetadata) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||
            transitMetadata.txnStartTimestamp == -1) {
            throwStateTransitionFailure(transitMetadata)
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp
            topicPartitions.clear()
            if (transitMetadata.clientTransactionVersion.supportsEpochBump()) {
              producerEpoch = transitMetadata.producerEpoch
              lastProducerEpoch = transitMetadata.lastProducerEpoch
              previousProducerId = transitMetadata.prevProducerId
              producerId = transitMetadata.producerId
              nextProducerId = transitMetadata.nextProducerId
            }
          }

        case PrepareEpochFence =>
          // We should never get here, since once we prepare to fence the epoch, we immediately set the pending state
          // to PrepareAbort, and then consequently to CompleteAbort after the markers are written.. So we should never
          // ever try to complete a transition to PrepareEpochFence, as it is not a valid previous state for any other state, and hence
          // can never be transitioned out of.
          throwStateTransitionFailure(transitMetadata)


        case Dead =>
          // The transactionalId was being expired. The completion of the operation should result in removal of the
          // the metadata from the cache, so we should never realistically transition to the dead state.
          throw new IllegalStateException(s"TransactionalId $transactionalId is trying to complete a transition to " +
            s"$toState. This means that the transactionalId was being expired, and the only acceptable completion of " +
            s"this operation is to remove the transaction metadata from the cache, not to persist the $toState in the log.")
      }

      debug(s"TransactionalId $transactionalId complete transition from $state to $transitMetadata")
      clientTransactionVersion = transitMetadata.clientTransactionVersion
      txnLastUpdateTimestamp = transitMetadata.txnLastUpdateTimestamp
      pendingState = None
      state = toState
    }
  }

  /**
   * Validates the producer epoch and ID based on transaction state and version.
   *
   * Logic:
   * * 1. **Overflow Case in Transactions V2:**
   * *    - During overflow (epoch reset to 0), we compare both `lastProducerEpoch` values since it
   * *      does not change during completion.
   * *    - For PrepareComplete, the producer ID has been updated. We ensure that the `prevProducerID`
   * *      in the transit metadata matches the current producer ID, confirming the change.
   * *
   * * 2. **Epoch Bump Case in Transactions V2:**
   * *    - For PrepareCommit or PrepareAbort, the producer epoch has been bumped. We ensure the `lastProducerEpoch`
   * *      in transit metadata matches the current producer epoch, confirming the bump.
   * *    - We also verify that the producer ID remains the same.
   * *
   * * 3. **Other Cases:**
   * *    - For other states and versions, check if the producer epoch and ID match the current values.
   *
   * @param transitMetadata       The transaction transition metadata containing state, producer epoch, and ID.
   * @return true if the producer epoch and ID are valid; false otherwise.
   */
  private def validProducerEpoch(transitMetadata: TxnTransitMetadata): Boolean = {
    val isAtLeastTransactionsV2 = transitMetadata.clientTransactionVersion.supportsEpochBump()
    val txnState = transitMetadata.txnState
    val transitProducerEpoch = transitMetadata.producerEpoch
    val transitProducerId = transitMetadata.producerId
    val transitLastProducerEpoch = transitMetadata.lastProducerEpoch

    (isAtLeastTransactionsV2, txnState, transitProducerEpoch) match {
      case (true, CompleteCommit | CompleteAbort, epoch) if epoch == 0.toShort =>
        transitLastProducerEpoch == lastProducerEpoch &&
          transitMetadata.prevProducerId == producerId

      case (true, PrepareCommit | PrepareAbort, _) =>
        transitLastProducerEpoch == producerEpoch &&
          transitProducerId == producerId

      case _ =>
        transitProducerEpoch == producerEpoch &&
          transitProducerId == producerId
    }
  }

  private def validProducerEpochBump(transitMetadata: TxnTransitMetadata): Boolean = {
    val transitEpoch = transitMetadata.producerEpoch
    val transitProducerId = transitMetadata.producerId
    transitEpoch == producerEpoch + 1 || (transitEpoch == 0 && transitProducerId != producerId)
  }

  private def throwStateTransitionFailure(txnTransitMetadata: TxnTransitMetadata): Unit = {
    fatal(s"${this.toString}'s transition to $txnTransitMetadata failed: this should not happen")

    throw new IllegalStateException(s"TransactionalId $transactionalId failed transition to state $txnTransitMetadata " +
      "due to unexpected metadata")
  }

  def pendingTransitionInProgress: Boolean = pendingState.isDefined

  override def toString: String = {
    "TransactionMetadata(" +
      s"transactionalId=$transactionalId, " +
      s"producerId=$producerId, " +
      s"previousProducerId=$previousProducerId, " +
      s"nextProducerId=$nextProducerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"lastProducerEpoch=$lastProducerEpoch, " +
      s"txnTimeoutMs=$txnTimeoutMs, " +
      s"state=$state, " +
      s"pendingState=$pendingState, " +
      s"topicPartitions=$topicPartitions, " +
      s"txnStartTimestamp=$txnStartTimestamp, " +
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp, " +
      s"clientTransactionVersion=$clientTransactionVersion)"
  }

  override def equals(that: Any): Boolean = that match {
    case other: TransactionMetadata =>
      transactionalId == other.transactionalId &&
      producerId == other.producerId &&
      producerEpoch == other.producerEpoch &&
      lastProducerEpoch == other.lastProducerEpoch &&
      txnTimeoutMs == other.txnTimeoutMs &&
      state.equals(other.state) &&
      topicPartitions.equals(other.topicPartitions) &&
      txnStartTimestamp == other.txnStartTimestamp &&
      txnLastUpdateTimestamp == other.txnLastUpdateTimestamp &&
      clientTransactionVersion == other.clientTransactionVersion
    case _ => false
  }

  override def hashCode(): Int = {
    val fields = Seq(transactionalId, producerId, producerEpoch, txnTimeoutMs, state, topicPartitions,
      txnStartTimestamp, txnLastUpdateTimestamp, clientTransactionVersion)
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

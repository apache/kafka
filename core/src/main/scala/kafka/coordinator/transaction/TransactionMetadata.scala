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

import kafka.utils.nonthreadsafe
import org.apache.kafka.common.TopicPartition

import scala.collection.{immutable, mutable}

private[transaction] sealed trait TransactionState { def byte: Byte }

/**
 * Transaction has not existed yet
 *
 * transition: received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Empty extends TransactionState { val byte: Byte = 0 }

/**
 * Transaction has started and ongoing
 *
 * transition: received EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Ongoing extends TransactionState { val byte: Byte = 1 }

/**
 * Group is preparing to commit
 *
 * transition: received acks from all partitions => CompleteCommit
 */
private[transaction] case object PrepareCommit extends TransactionState { val byte: Byte = 2}

/**
 * Group is preparing to abort
 *
 * transition: received acks from all partitions => CompleteAbort
 */
private[transaction] case object PrepareAbort extends TransactionState { val byte: Byte = 3 }

/**
 * Group has completed commit
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[transaction] case object CompleteCommit extends TransactionState { val byte: Byte = 4 }

/**
 * Group has completed abort
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[transaction] case object CompleteAbort extends TransactionState { val byte: Byte = 5 }

private[transaction] object TransactionMetadata {
  def apply(producerId: Long, epoch: Short, txnTimeoutMs: Int, timestamp: Long) = new TransactionMetadata(producerId, epoch, txnTimeoutMs, Empty, collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

  def apply(producerId: Long, epoch: Short, txnTimeoutMs: Int, state: TransactionState, timestamp: Long) = new TransactionMetadata(producerId, epoch, txnTimeoutMs, state, collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

  def byteToState(byte: Byte): TransactionState = {
    byte match {
      case 0 => Empty
      case 1 => Ongoing
      case 2 => PrepareCommit
      case 3 => PrepareAbort
      case 4 => CompleteCommit
      case 5 => CompleteAbort
      case unknown => throw new IllegalStateException("Unknown transaction state byte " + unknown + " from the transaction status message")
    }
  }

  def isValidTransition(oldState: TransactionState, newState: TransactionState): Boolean = TransactionMetadata.validPreviousStates(newState).contains(oldState)

  private val validPreviousStates: Map[TransactionState, Set[TransactionState]] =
    Map(Empty -> Set(Empty, CompleteCommit, CompleteAbort),
      Ongoing -> Set(Ongoing, Empty, CompleteCommit, CompleteAbort),
      PrepareCommit -> Set(Ongoing),
      PrepareAbort -> Set(Ongoing),
      CompleteCommit -> Set(PrepareCommit),
      CompleteAbort -> Set(PrepareAbort))
}

// this is a immutable object representing the target transition of the transaction metadata
private[transaction] case class TransactionMetadataTransition(producerId: Long,
                                                              producerEpoch: Short,
                                                              txnTimeoutMs: Int,
                                                              txnState: TransactionState,
                                                              topicPartitions: immutable.Set[TopicPartition],
                                                              txnStartTimestamp: Long,
                                                              txnLastUpdateTimestamp: Long)

/**
  *
  * @param producerId            producer id
  * @param producerEpoch         current epoch of the producer
  * @param txnTimeoutMs          timeout to be used to abort long running transactions
  * @param state                 current state of the transaction
  * @param topicPartitions       current set of partitions that are part of this transaction
  * @param txnStartTimestamp     time the transaction was started, i.e., when first partition is added
  * @param txnLastUpdateTimestamp   updated when any operation updates the TransactionMetadata. To be used for expiration
  */
@nonthreadsafe
private[transaction] class TransactionMetadata(val producerId: Long,
                                               var producerEpoch: Short,
                                               var txnTimeoutMs: Int,
                                               var state: TransactionState,
                                               val topicPartitions: mutable.Set[TopicPartition],
                                               var txnStartTimestamp: Long = -1,
                                               var txnLastUpdateTimestamp: Long) {

  // pending state is used to indicate the state that this transaction is going to
  // transit to, and for blocking future attempts to transit it again if it is not legal;
  // initialized as the same as the current state
  var pendingState: Option[TransactionState] = None

  def addPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    topicPartitions ++= partitions
  }

  def removePartition(topicPartition: TopicPartition): Unit = {
    if (pendingState.isDefined || (state != PrepareCommit && state != PrepareAbort))
      throw new IllegalStateException(s"Transation metadata's current state is $state, and its pending state is $state " +
        s"while trying to remove partitions whose txn marker has been sent, this is not expected")

    topicPartitions -= topicPartition
  }

  def prepareNoTransit(): TransactionMetadataTransition =
    // do not call transitTo as it will set the pending state
    TransactionMetadataTransition(producerId, producerEpoch, txnTimeoutMs, state, topicPartitions.toSet, txnStartTimestamp, txnLastUpdateTimestamp)

  def prepareIncrementProducerEpoch(newTxnTimeoutMs: Int,
                                    updateTimestamp: Long): TransactionMetadataTransition = {

    prepareTransitionTo(Empty, (producerEpoch + 1).toShort, newTxnTimeoutMs, immutable.Set.empty[TopicPartition], -1, updateTimestamp)
  }

  def prepareNewPid(updateTimestamp: Long): TransactionMetadataTransition = {

    prepareTransitionTo(Empty, producerEpoch, txnTimeoutMs, immutable.Set.empty[TopicPartition], -1, updateTimestamp)
  }

  def prepareAddPartitions(addedTopicPartitions: immutable.Set[TopicPartition],
                           updateTimestamp: Long): TransactionMetadataTransition = {

    if (state == Empty || state == CompleteCommit || state == CompleteAbort) {
      prepareTransitionTo(Ongoing, producerEpoch, txnTimeoutMs, (topicPartitions ++ addedTopicPartitions).toSet, updateTimestamp, updateTimestamp)
    } else {
      prepareTransitionTo(Ongoing, producerEpoch, txnTimeoutMs, (topicPartitions ++ addedTopicPartitions).toSet, txnStartTimestamp, updateTimestamp)
    }
  }

  def prepareAbortOrCommit(newState: TransactionState,
                           updateTimestamp: Long): TransactionMetadataTransition = {

    prepareTransitionTo(newState, producerEpoch, txnTimeoutMs, topicPartitions.toSet, txnStartTimestamp, updateTimestamp)
  }

  def prepareComplete(updateTimestamp: Long): TransactionMetadataTransition = {
    val newState = if (state == PrepareCommit) CompleteCommit else CompleteAbort
    prepareTransitionTo(newState, producerEpoch, txnTimeoutMs, topicPartitions.toSet, txnStartTimestamp, updateTimestamp)
  }

  // visible for testing only
  def copy(): TransactionMetadata = {
    val cloned = new TransactionMetadata(producerId, producerEpoch, txnTimeoutMs, state,
      mutable.Set.empty ++ topicPartitions.toSet, txnStartTimestamp, txnLastUpdateTimestamp)
    cloned.pendingState = pendingState

    cloned
  }

  private def prepareTransitionTo(newState: TransactionState,
                                  newEpoch: Short,
                                  newTxnTimeoutMs: Int,
                                  newTopicPartitions: immutable.Set[TopicPartition],
                                  newTxnStartTimestamp: Long,
                                  updateTimestamp: Long): TransactionMetadataTransition = {
    if (pendingState.isDefined)
      throw new IllegalStateException(s"Preparing transaction state transition to $newState " +
        s"while it already a pending state ${pendingState.get}")

    // check that the new state transition is valid and update the pending state if necessary
    if (TransactionMetadata.validPreviousStates(newState).contains(state)) {
      pendingState = Some(newState)

      TransactionMetadataTransition(producerId, newEpoch, newTxnTimeoutMs, newState, newTopicPartitions, newTxnStartTimestamp, updateTimestamp)
    } else {
      throw new IllegalStateException(s"Preparing transaction state transition to $newState failed since the target state" +
        s" $newState is not a valid previous state of the current state $state")
    }
  }

  def completeTransitionTo(newMetadata: TransactionMetadataTransition): Unit = {
    // metadata transition is valid only if all the following conditions are met:
    //
    // 1. the new state is already indicated in the pending state.
    // 2. the producerId is the same (i.e. this field should never be changed)
    // 3. the epoch should be either the same value or old value + 1.
    // 4. the last update time is no smaller than the old value.
    // 4. the old partitions set is a subset of the new partitions set.
    //
    // plus, we should only try to update the metadata after the corresponding log entry has been successfully written and replicated (see TransactionStateManager#appendTransactionToLog)
    //
    // if valid, transition is done via overwriting the whole object to ensure synchronization

    val toState = pendingState.getOrElse(throw new IllegalStateException("Completing transaction state transition while it does not have a pending state"))

    if (toState != newMetadata.txnState ||
      producerId != newMetadata.producerId ||
      txnLastUpdateTimestamp > newMetadata.txnLastUpdateTimestamp) {

      throw new IllegalStateException("Completing transaction state transition failed due to unexpected metadata state")
    } else {
      val updated = toState match {
        case Empty => // from initPid
          if (producerEpoch > newMetadata.producerEpoch ||
            producerEpoch < newMetadata.producerEpoch - 1 ||
            newMetadata.topicPartitions.nonEmpty ||
            newMetadata.txnStartTimestamp != -1) {

            throw new IllegalStateException("Completing transaction state transition failed due to unexpected metadata")
          } else {
            txnTimeoutMs = newMetadata.txnTimeoutMs
            producerEpoch = newMetadata.producerEpoch
          }

        case Ongoing => // from addPartitions
          if (producerEpoch != newMetadata.producerEpoch ||
            !topicPartitions.subsetOf(newMetadata.topicPartitions) ||
            txnTimeoutMs != newMetadata.txnTimeoutMs ||
            txnStartTimestamp > newMetadata.txnStartTimestamp) {

            throw new IllegalStateException("Completing transaction state transition failed due to unexpected metadata")
          } else {
            txnStartTimestamp = newMetadata.txnStartTimestamp
            addPartitions(newMetadata.topicPartitions)
          }

        case PrepareAbort | PrepareCommit => // from endTxn
          if (producerEpoch != newMetadata.producerEpoch ||
            !topicPartitions.toSet.equals(newMetadata.topicPartitions) ||
            txnTimeoutMs != newMetadata.txnTimeoutMs ||
            txnStartTimestamp != newMetadata.txnStartTimestamp) {

            throw new IllegalStateException("Completing transaction state transition failed due to unexpected metadata")
          }

        case CompleteAbort | CompleteCommit => // from write markers
          if (producerEpoch != newMetadata.producerEpoch ||
            txnTimeoutMs != newMetadata.txnTimeoutMs ||
            newMetadata.txnStartTimestamp == -1) {

            throw new IllegalStateException("Completing transaction state transition failed due to unexpected metadata")
          } else {
            txnStartTimestamp = newMetadata.txnStartTimestamp
            topicPartitions.clear()
          }
      }

      txnLastUpdateTimestamp = newMetadata.txnLastUpdateTimestamp
      pendingState = None
      state = toState
    }
  }

  def pendingTransitionInProgress: Boolean = pendingState.isDefined

  override def toString = s"TransactionMetadata($pendingState, $producerId, $producerEpoch, $txnTimeoutMs, $state, $topicPartitions, $txnStartTimestamp, $txnLastUpdateTimestamp)"

  override def equals(that: Any): Boolean = that match {
    case other: TransactionMetadata =>
      producerId == other.producerId &&
      producerEpoch == other.producerEpoch &&
      txnTimeoutMs == other.txnTimeoutMs &&
      state.equals(other.state) &&
      topicPartitions.equals(other.topicPartitions) &&
      txnStartTimestamp == other.txnStartTimestamp &&
      txnLastUpdateTimestamp == other.txnLastUpdateTimestamp
    case _ => false
  }

  override def hashCode(): Int = {
    val fields = Seq(producerId, producerEpoch, txnTimeoutMs, state, topicPartitions, txnStartTimestamp, txnLastUpdateTimestamp)
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

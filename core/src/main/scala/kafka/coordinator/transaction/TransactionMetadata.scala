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

import scala.collection.mutable

private[coordinator] sealed trait TransactionState { def byte: Byte }

/**
 * Transaction has not existed yet
 *
 * transition: received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[coordinator] case object Empty extends TransactionState { val byte: Byte = 0 }

/**
 * Transaction has started and ongoing
 *
 * transition: received EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[coordinator] case object Ongoing extends TransactionState { val byte: Byte = 1 }

/**
 * Group is preparing to commit
 *
 * transition: received acks from all partitions => CompleteCommit
 */
private[coordinator] case object PrepareCommit extends TransactionState { val byte: Byte = 2}

/**
 * Group is preparing to abort
 *
 * transition: received acks from all partitions => CompleteAbort
 */
private[coordinator] case object PrepareAbort extends TransactionState { val byte: Byte = 3 }

/**
 * Group has completed commit
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[coordinator] case object CompleteCommit extends TransactionState { val byte: Byte = 4 }

/**
 * Group has completed abort
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[coordinator] case object CompleteAbort extends TransactionState { val byte: Byte = 5 }

private[coordinator] object TransactionMetadata {
  def apply(pid: Long, epoch: Short, txnTimeoutMs: Int, timestamp: Long) = new TransactionMetadata(pid, epoch, txnTimeoutMs, Empty, collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

  def apply(pid: Long, epoch: Short, txnTimeoutMs: Int, state: TransactionState, timestamp: Long) = new TransactionMetadata(pid, epoch, txnTimeoutMs, state, collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

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
    Map(Empty -> Set(),
      Ongoing -> Set(Ongoing, Empty, CompleteCommit, CompleteAbort),
      PrepareCommit -> Set(Ongoing),
      PrepareAbort -> Set(Ongoing),
      CompleteCommit -> Set(PrepareCommit),
      CompleteAbort -> Set(PrepareAbort))
}

/**
  *
  * @param pid                   producer id
  * @param producerEpoch         current epoch of the producer
  * @param txnTimeoutMs          timeout to be used to abort long running transactions
  * @param state                 the current state of the transaction
  * @param topicPartitions       set of partitions that are part of this transaction
  * @param transactionStartTime  time the transaction was started, i.e., when first partition is added
  * @param lastUpdateTimestamp   updated when any operation updates the TransactionMetadata. To be used for expiration
  */
@nonthreadsafe
private[coordinator] class TransactionMetadata(val pid: Long,
                                               var producerEpoch: Short,
                                               var txnTimeoutMs: Int,
                                               var state: TransactionState,
                                               val topicPartitions: mutable.Set[TopicPartition],
                                               var transactionStartTime: Long = -1,
                                               var lastUpdateTimestamp: Long) {

  // pending state is used to indicate the state that this transaction is going to
  // transit to, and for blocking future attempts to transit it again if it is not legal;
  // initialized as the same as the current state
  var pendingState: Option[TransactionState] = None

  def addPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    topicPartitions ++= partitions
  }

  def prepareTransitionTo(newState: TransactionState): Boolean = {
    if (pendingState.isDefined)
      throw new IllegalStateException(s"Preparing transaction state transition to $newState while it already a pending state ${pendingState.get}")

    // check that the new state transition is valid and update the pending state if necessary
    if (TransactionMetadata.validPreviousStates(newState).contains(state)) {
      pendingState = Some(newState)
      true
    } else {
      false
    }
  }

  def completeTransitionTo(newState: TransactionState): Boolean = {
    val toState = pendingState.getOrElse(throw new IllegalStateException("Completing transaction state transition while it does not have a pending state"))
    if (toState != newState) {
      false
    } else {
      pendingState = None
      state = toState
      true
    }
  }

  def copy(): TransactionMetadata =
    new TransactionMetadata(pid, producerEpoch, txnTimeoutMs, state, collection.mutable.Set.empty[TopicPartition] ++ topicPartitions, transactionStartTime, lastUpdateTimestamp)

  override def toString = s"TransactionMetadata($pendingState, $pid, $producerEpoch, $txnTimeoutMs, $state, $topicPartitions, $transactionStartTime, $lastUpdateTimestamp)"

  override def equals(that: Any): Boolean = that match {
    case other: TransactionMetadata =>
      pid == other.pid &&
      producerEpoch == other.producerEpoch &&
      txnTimeoutMs == other.txnTimeoutMs &&
      state.equals(other.state) &&
      topicPartitions.equals(other.topicPartitions) &&
      transactionStartTime.equals(other.transactionStartTime) &&
      lastUpdateTimestamp.equals(other.lastUpdateTimestamp)
    case _ => false
  }


  override def hashCode(): Int = {
    val state = Seq(pid, txnTimeoutMs, topicPartitions, lastUpdateTimestamp)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

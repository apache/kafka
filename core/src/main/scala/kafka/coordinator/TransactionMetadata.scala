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
package kafka.coordinator

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
private[coordinator] case object NotExist extends TransactionState { val byte: Byte = 0 }

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
  def byteToState(byte: Byte): TransactionState = {
    byte match {
      case 0 => NotExist
      case 1 => Ongoing
      case 2 => PrepareCommit
      case 3 => PrepareAbort
      case 4 => CompleteCommit
      case 5 => CompleteAbort
      case unknown => throw new IllegalStateException("Unknown transaction state byte " + unknown + " from the transaction status message")
    }
  }
}

@nonthreadsafe
private[coordinator] class TransactionMetadata(var state: TransactionState) {

  // participated partitions in this transaction
  val topicPartitions = mutable.Set.empty[TopicPartition]

  def addPartitions(partitions: Set[TopicPartition]): Unit = {
    topicPartitions ++= partitions
  }

  override def toString: String =
    s"(state: $state, topicPartitions: ${topicPartitions.mkString("(",",",")")})"

  override def equals(that: Any): Boolean = that match {
    case other: TransactionMetadata => state.equals(other.state) && topicPartitions.equals(other.topicPartitions)
    case _ => false
  }

}

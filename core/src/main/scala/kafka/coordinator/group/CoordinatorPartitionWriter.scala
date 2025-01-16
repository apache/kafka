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
package kafka.coordinator.group

import kafka.cluster.PartitionListener
import kafka.server.{AddPartitionsToTxnManager, ReplicaManager}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.coordinator.common.runtime.PartitionWriter
import org.apache.kafka.server.ActionQueue
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, VerificationGuard}

import java.util.concurrent.CompletableFuture
import scala.collection.Map

/**
 * ListenerAdapter adapts the PartitionListener interface to the
 * PartitionWriter.Listener interface.
 */
private[group] class ListenerAdapter(
  val listener: PartitionWriter.Listener
) extends PartitionListener {
  override def onHighWatermarkUpdated(
    tp: TopicPartition,
    offset: Long
  ): Unit = {
    listener.onHighWatermarkUpdated(tp, offset)
  }

  override def equals(that: Any): Boolean = that match {
    case other: ListenerAdapter => listener.equals(other.listener)
    case _ => false
  }

  override def hashCode(): Int = {
    listener.hashCode()
  }

  override def toString: String = {
    s"ListenerAdapter(listener=$listener)"
  }
}

class CoordinatorPartitionWriter(
  replicaManager: ReplicaManager
) extends PartitionWriter {
  // We use an action queue which directly executes actions. This is possible
  // here because we don't hold any conflicting locks.
  private val directActionQueue = new ActionQueue {
    override def add(action: Runnable): Unit = {
      action.run()
    }

    override def tryCompleteActions(): Unit = {}
  }

  /**
   * {@inheritDoc}
   */
  override def registerListener(
    tp: TopicPartition,
    listener: PartitionWriter.Listener
  ): Unit = {
    replicaManager.maybeAddListener(tp, new ListenerAdapter(listener))
  }

  /**
   * {@inheritDoc}
   */
  override def deregisterListener(
    tp: TopicPartition,
    listener: PartitionWriter.Listener
  ): Unit = {
    replicaManager.removeListener(tp, new ListenerAdapter(listener))
  }

  /**
   * {@inheritDoc}
   */
  override def config(tp: TopicPartition): LogConfig = {
    replicaManager.getLogConfig(tp).getOrElse {
      throw Errors.NOT_LEADER_OR_FOLLOWER.exception()
    }
  }

  /**
   * {@inheritDoc}
   */
  override def maybeStartTransactionVerification(
    tp: TopicPartition,
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    apiVersion: Short
  ): CompletableFuture[VerificationGuard] = {
    val transactionSupportedOperation = AddPartitionsToTxnManager.txnOffsetCommitRequestVersionToTransactionSupportedOperation(apiVersion)
    val future = new CompletableFuture[VerificationGuard]()
    replicaManager.maybeStartTransactionVerificationForPartition(
      topicPartition = tp,
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      baseSequence = RecordBatch.NO_SEQUENCE,
      callback = errorAndGuard => {
        val (error, verificationGuard) = errorAndGuard
        if (error != Errors.NONE) {
          future.completeExceptionally(error.exception)
        } else {
          future.complete(verificationGuard)
        }
      },
      transactionSupportedOperation
    )
    future
  }

  /**
   * {@inheritDoc }
   */
  override def append(
    tp: TopicPartition,
    verificationGuard: VerificationGuard,
    records: MemoryRecords
  ): Long = {
    var appendResults: Map[TopicPartition, PartitionResponse] = Map.empty
    replicaManager.appendRecords(
      timeout = 0L,
      requiredAcks = 1,
      internalTopicsAllowed = true,
      origin = AppendOrigin.COORDINATOR,
      entriesPerPartition = Map(tp -> records),
      responseCallback = results => appendResults = results,
      requestLocal = RequestLocal.noCaching,
      verificationGuards = Map(tp -> verificationGuard),
      delayedProduceLock = None,
      // We can directly complete the purgatories here because we don't hold
      // any conflicting locks.
      actionQueue = directActionQueue
    )

    val partitionResult = appendResults.getOrElse(tp,
      throw new IllegalStateException(s"Append status $appendResults should have partition $tp."))

    if (partitionResult.error != Errors.NONE) {
      throw partitionResult.error.exception()
    }

    // Required offset.
    partitionResult.lastOffset + 1
  }

  override def deleteRecords(tp: TopicPartition, deleteBeforeOffset: Long): CompletableFuture[Void] = {
    val responseFuture: CompletableFuture[Void] = new CompletableFuture[Void]()

    replicaManager.deleteRecords(
      timeout = 30000L, // 30 seconds.
      offsetPerPartition = Map(tp -> deleteBeforeOffset),
      responseCallback = results => {
        val result = results.get(tp)
        if (result.isEmpty) {
          responseFuture.completeExceptionally(new IllegalStateException(s"Delete status $result should have partition $tp."))
        } else if (result.get.errorCode != Errors.NONE.code) {
          responseFuture.completeExceptionally(Errors.forCode(result.get.errorCode).exception)
        } else {
          responseFuture.complete(null)
        }
      },
      allowInternalTopicDeletion = true
    )
    responseFuture
  }
}

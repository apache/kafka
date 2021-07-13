/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{AlterReplicaStateRequestData, AlterReplicaStateResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterReplicaStateRequest, AlterReplicaStateResponse}
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles the sending of AlterReplicaState requests to the controller when LogDirFailure.
 */
abstract class LogDirEventManager {

  def start(): Unit

  def handleAlterReplicaStateChanges(logDirEventItem: AlterReplicaStateItem): Unit

  def pendingAlterReplicaStateItemCount(): Int

  def shutdown(): Unit
}

case class AlterReplicaStateItem(topicPartitions: util.List[TopicPartition],
                                 newState: Byte,
                                 reason: String,
                                 callback: Either[Errors, TopicPartition] => Unit)

class LogDirEventManagerImpl(val controllerChannelManager: BrokerToControllerChannelManager,
                             val scheduler: Scheduler,
                             val time: Time,
                             val brokerId: Int,
                             val brokerEpochSupplier: () => Long) extends LogDirEventManager with Logging with KafkaMetricsGroup {

  // Used to allow only one in-flight request at a time
  private val inflightRequest: AtomicBoolean = new AtomicBoolean(false)

  private val pendingReplicaStateUpdates: LinkedBlockingQueue[AlterReplicaStateItem] = new LinkedBlockingQueue[AlterReplicaStateItem]()

  private val lastSentMs = new AtomicLong(0)

  def start(): Unit = {
    scheduler.schedule("send-alter-replica-state", propagateReplicaStateChanges, 50, 50, TimeUnit.MILLISECONDS)
  }

  override def pendingAlterReplicaStateItemCount(): Int = pendingReplicaStateUpdates.size()

  private def propagateReplicaStateChanges(): Unit = {
    if (!pendingReplicaStateUpdates.isEmpty && inflightRequest.compareAndSet(false, true)) {
      // Copy current unsent items and remove from the queue, will be inserted if failed
      val inflightAlterIsrItem = pendingReplicaStateUpdates.poll()

      lastSentMs.set(time.milliseconds())
      sendRequest(inflightAlterIsrItem)
    }
  }

  def handleAlterReplicaStateChanges(logDirEventItem: AlterReplicaStateItem): Unit = {
    pendingReplicaStateUpdates.put(logDirEventItem)
  }

  def sendRequest(logDirEventItem: AlterReplicaStateItem): Unit = {

    val message = buildRequest(logDirEventItem)

    debug(s"Sending AlterReplicaState to controller $message")
    controllerChannelManager.sendRequest(new AlterReplicaStateRequest.Builder(message),
      new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          try {
            val body = response.responseBody().asInstanceOf[AlterReplicaStateResponse]
            handleAlterReplicaStateResponse(body, message.brokerEpoch, logDirEventItem)
          } finally {
            inflightRequest.set(false)
          }
        }

        override def onTimeout(): Unit = {
          throw new IllegalStateException("Encountered unexpected timeout when sending AlterIsr to the controller")
        }
      })
  }

  private def buildRequest(logDirEventItem: AlterReplicaStateItem): AlterReplicaStateRequestData = {
    val message = new AlterReplicaStateRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpochSupplier.apply())
      .setNewState(logDirEventItem.newState)
      .setReason(logDirEventItem.reason)
      .setTopics(new java.util.ArrayList())

    logDirEventItem.topicPartitions.asScala.groupBy(_.topic).foreach(entry => {
      val topicPart = new AlterReplicaStateRequestData.TopicData()
        .setName(entry._1)
        .setPartitions(new java.util.ArrayList())
      message.topics().add(topicPart)
      entry._2.foreach(item => {
        topicPart.partitions().add(new AlterReplicaStateRequestData.PartitionData()
          .setPartitionIndex(item.partition)
        )
      })
    })
    message
  }

  private def handleAlterReplicaStateResponse(alterReplicaStateResponse: AlterReplicaStateResponse,
                                              sentBrokerEpoch: Long,
                                              logDirEventItem: AlterReplicaStateItem): Unit = {
    val data: AlterReplicaStateResponseData = alterReplicaStateResponse.data

    Errors.forCode(data.errorCode) match {
      case Errors.STALE_BROKER_EPOCH =>
        warn(s"Broker had a stale broker epoch ($sentBrokerEpoch), broker could have been repaired and restarted, ignore")
        pendingReplicaStateUpdates.put(logDirEventItem)

      case Errors.NOT_CONTROLLER =>
        warn(s"Remote broker is not controller, ignore")
        pendingReplicaStateUpdates.put(logDirEventItem)

      case Errors.CLUSTER_AUTHORIZATION_FAILED =>
        val exception = Errors.CLUSTER_AUTHORIZATION_FAILED.exception("Broker is not authorized to send AlterReplicaState to controller")
        error(s"Broker is not authorized to send AlterReplicaState to controller", exception)
        pendingReplicaStateUpdates.put(logDirEventItem)

      case Errors.UNKNOWN_REPLICA_STATE =>
        val exception = Errors.CLUSTER_AUTHORIZATION_FAILED.exception("ReplicaStateChange failed with an unknown replica state")
        error(s"Broker is not authorized to send AlterReplicaState to controller", exception)
        pendingReplicaStateUpdates.put(logDirEventItem)

      case Errors.NONE =>
        // success is a flag to indicate whether all the partitions had successfully alter state
        val failedPartitions = new util.ArrayList[TopicPartition]()
        // Collect partition-level responses to pass to the callbacks
        val partitionResponses: mutable.Map[TopicPartition, Either[Errors, TopicPartition]] =
          new mutable.HashMap[TopicPartition, Either[Errors, TopicPartition]]()
        data.topics.forEach { topic =>
          topic.partitions().forEach(partition => {
            val tp = new TopicPartition(topic.name, partition.partitionIndex)
            val error = Errors.forCode(partition.errorCode())
            debug(s"Controller successfully handled AlterReplicaState request for $tp: $partition")
            if (error == Errors.NONE) {
              partitionResponses(tp) = Right(tp)
            } else {
              failedPartitions.add(new TopicPartition(topic.name(), partition.partitionIndex()))
              partitionResponses(tp) = Left(error)
            }
          })
        }

        // Iterate across the items we sent rather than what we received to ensure we run the callback even if a
        // partition was somehow erroneously excluded from the response.
        logDirEventItem.topicPartitions.asScala.foreach(topicPartition =>
          if (partitionResponses.contains(topicPartition)) {
            logDirEventItem.callback.apply(partitionResponses(topicPartition))
          } else {
            // Don't remove this partition so it will get re-sent
            warn(s"Partition $topicPartition was sent but not included in the response")
          }
        )
        if (failedPartitions.size() > 0) {
          pendingReplicaStateUpdates.put(logDirEventItem.copy(topicPartitions = failedPartitions))
        }

      case e: Errors =>
        warn(s"Controller returned an unexpected top-level error when handling AlterReplicaState request: $e")
        pendingReplicaStateUpdates.put(logDirEventItem)
    }
  }

  override def shutdown(): Unit = {
    controllerChannelManager.shutdown()
  }
}

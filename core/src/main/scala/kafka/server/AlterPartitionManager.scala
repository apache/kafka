/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.errors.OperationNotAttemptedException
import org.apache.kafka.common.message.AlterPartitionRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterPartitionRequest, AlterPartitionResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.{LeaderAndIsr, LeaderRecoveryState}
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, NodeToControllerChannelManager, TopicIdPartition}
import org.apache.kafka.server.util.Scheduler

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.OptionConverters.RichOptional

/**
 * Handles updating the ISR by sending AlterPartition requests to the controller. Updating the ISR is an asynchronous 
 * operation, so partitions will learn about the result of their request through a callback.
 *
 * Note that ISR state changes can still be initiated by the controller and sent to the partitions via LeaderAndIsr
 * requests.
 */
trait AlterPartitionManager {
  def start(): Unit = {}

  def shutdown(): Unit = {}

  def submit(
    topicIdPartition: TopicIdPartition,
    leaderAndIsr: LeaderAndIsr
  ): CompletableFuture[LeaderAndIsr]
}

case class AlterPartitionItem(
  topicIdPartition: TopicIdPartition,
  leaderAndIsr: LeaderAndIsr,
  future: CompletableFuture[LeaderAndIsr]
)

object AlterPartitionManager {

  /**
   * Factory to AlterPartition based implementation
   */
  def apply(
    config: KafkaConfig,
    scheduler: Scheduler,
    controllerNodeProvider: ControllerNodeProvider,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: String,
    brokerEpochSupplier: () => Long,
  ): AlterPartitionManager = {
    val channelManager = new NodeToControllerChannelManagerImpl(
      controllerNodeProvider,
      time = time,
      metrics = metrics,
      config = config,
      channelName = "alter-partition",
      threadNamePrefix = threadNamePrefix,
      retryTimeoutMs = Long.MaxValue
    )
    new DefaultAlterPartitionManager(
      controllerChannelManager = channelManager,
      scheduler = scheduler,
      time = time,
      brokerId = config.brokerId,
      brokerEpochSupplier = brokerEpochSupplier
    )
  }
}

class DefaultAlterPartitionManager(
  val controllerChannelManager: NodeToControllerChannelManager,
  val scheduler: Scheduler,
  val time: Time,
  val brokerId: Int,
  val brokerEpochSupplier: () => Long,
) extends AlterPartitionManager with Logging {

  // Used to allow only one pending ISR update per partition (visible for testing)
  private[server] val unsentIsrUpdates = new ConcurrentHashMap[TopicIdPartition, AlterPartitionItem]()

  // Used to allow only one in-flight request at a time
  private val inflightRequest: AtomicBoolean = new AtomicBoolean(false)

  override def start(): Unit = {
    controllerChannelManager.start()
  }

  override def shutdown(): Unit = {
    controllerChannelManager.shutdown()
  }

  override def submit(
    topicIdPartition: TopicIdPartition,
    leaderAndIsr: LeaderAndIsr
  ): CompletableFuture[LeaderAndIsr] = {
    val future = new CompletableFuture[LeaderAndIsr]()
    val alterPartitionItem = AlterPartitionItem(topicIdPartition, leaderAndIsr, future)
    val enqueued = unsentIsrUpdates.putIfAbsent(alterPartitionItem.topicIdPartition, alterPartitionItem) == null
    if (enqueued) {
      maybePropagateIsrChanges()
    } else {
      future.completeExceptionally(new OperationNotAttemptedException(
        s"Failed to enqueue ISR change state $leaderAndIsr for partition $topicIdPartition"))
    }
    future
  }

  private[server] def maybePropagateIsrChanges(): Unit = {
    // Send all pending items if there is not already a request in-flight.
    if (!unsentIsrUpdates.isEmpty && inflightRequest.compareAndSet(false, true)) {
      // Copy current unsent ISRs but don't remove from the map, they get cleared in the response handler
      val inflightAlterPartitionItems = new ListBuffer[AlterPartitionItem]()
      unsentIsrUpdates.values.forEach(item => inflightAlterPartitionItems.append(item))
      sendRequest(inflightAlterPartitionItems.toSeq)
    }
  }

  private[server] def clearInFlightRequest(): Unit = {
    if (!inflightRequest.compareAndSet(true, false)) {
      warn("Attempting to clear AlterPartition in-flight flag when no apparent request is in-flight")
    }
  }

  private def sendRequest(inflightAlterPartitionItems: Seq[AlterPartitionItem]): Unit = {
    val brokerEpoch = brokerEpochSupplier()
    val request = buildRequest(inflightAlterPartitionItems, brokerEpoch)
    debug(s"Sending AlterPartition to controller $request")

    // We will not timeout AlterPartition request, instead letting it retry indefinitely
    // until a response is received, or a new LeaderAndIsr overwrites the existing isrState
    // which causes the response for those partitions to be ignored.
    controllerChannelManager.sendRequest(request,
      new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          debug(s"Received AlterPartition response $response")
          val error = try {
            if (response.authenticationException != null) {
              // For now we treat authentication errors as retriable. We use the
              // `NETWORK_EXCEPTION` error code for lack of a good alternative.
              // Note that `NodeToControllerChannelManager` will still log the
              // authentication errors so that users have a chance to fix the problem.
              Errors.NETWORK_EXCEPTION
            } else if (response.versionMismatch != null) {
              Errors.UNSUPPORTED_VERSION
            } else {
              handleAlterPartitionResponse(
                response.responseBody.asInstanceOf[AlterPartitionResponse],
                brokerEpoch,
                inflightAlterPartitionItems
              )
            }
          } finally {
            // clear the flag so future requests can proceed
            clearInFlightRequest()
          }

          // check if we need to send another request right away
          error match {
              case Errors.NONE =>
                // In the normal case, check for pending updates to send immediately
                maybePropagateIsrChanges()
              case _ =>
                // If we received a top-level error from the controller, retry the request in the near future
                scheduler.scheduleOnce("send-alter-partition", () => maybePropagateIsrChanges(), 50)
            }
        }

        override def onTimeout(): Unit = {
          throw new IllegalStateException("Encountered unexpected timeout when sending AlterPartition to the controller")
        }
      })
  }

  /**
   * Builds an AlterPartition request.
   *
   * While building the request, we don't know which version of the AlterPartition API is
   * supported by the controller. The final decision is taken when the AlterPartitionRequest
   * is built in the network client based on the advertised api versions of the controller.
   *
   * @return an AlterPartitionRequest.Builder with the provided parameters.
   */
  private def buildRequest(
    inflightAlterPartitionItems: Seq[AlterPartitionItem],
    brokerEpoch: Long
  ): AlterPartitionRequest.Builder = {
    val message = new AlterPartitionRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpoch)

    inflightAlterPartitionItems.groupBy(_.topicIdPartition.topicId).foreach { case (topicId, items) =>
      val topicData = new AlterPartitionRequestData.TopicData().setTopicId(topicId)
      message.topics.add(topicData)

      items.foreach { item =>
        val partitionData = new AlterPartitionRequestData.PartitionData()
          .setPartitionIndex(item.topicIdPartition.partitionId)
          .setLeaderEpoch(item.leaderAndIsr.leaderEpoch)
          .setNewIsrWithEpochs(item.leaderAndIsr.isrWithBrokerEpoch)
          .setPartitionEpoch(item.leaderAndIsr.partitionEpoch)

        partitionData.setLeaderRecoveryState(item.leaderAndIsr.leaderRecoveryState.value)

        topicData.partitions.add(partitionData)
      }
    }

    new AlterPartitionRequest.Builder(message)
  }

  private def handleAlterPartitionResponse(
    alterPartitionResp: AlterPartitionResponse,
    sentBrokerEpoch: Long,
    inflightAlterPartitionItems: Seq[AlterPartitionItem],
  ): Errors = {
    val data = alterPartitionResp.data

    Errors.forCode(data.errorCode) match {
      case Errors.STALE_BROKER_EPOCH =>
        warn(s"Broker had a stale broker epoch ($sentBrokerEpoch), retrying.")

      case Errors.CLUSTER_AUTHORIZATION_FAILED =>
        error(s"Broker is not authorized to send AlterPartition to controller",
          Errors.CLUSTER_AUTHORIZATION_FAILED.exception("Broker is not authorized to send AlterPartition to controller"))

      case Errors.NONE =>
        // Collect partition-level responses to pass to the callbacks
        val partitionResponses = new mutable.HashMap[TopicIdPartition, Either[Errors, LeaderAndIsr]]()
        data.topics.forEach { topic =>
          topic.partitions.forEach { partition =>
            val tp = new TopicIdPartition(topic.topicId, partition.partitionIndex)
            val apiError = Errors.forCode(partition.errorCode)
            debug(s"Controller successfully handled AlterPartition request for $tp: $partition")
            if (apiError == Errors.NONE) {
              LeaderRecoveryState.optionalOf(partition.leaderRecoveryState).toScala match {
                case Some(leaderRecoveryState) =>
                  partitionResponses(tp) = Right(
                    new LeaderAndIsr(
                      partition.leaderId,
                      partition.leaderEpoch,
                      partition.isr,
                      leaderRecoveryState,
                      partition.partitionEpoch
                    )
                  )

                case None =>
                  error(s"Controller returned an invalid leader recovery state (${partition.leaderRecoveryState}) for $tp: $partition")
                  partitionResponses(tp) = Left(Errors.UNKNOWN_SERVER_ERROR)
              }
            } else {
              partitionResponses(tp) = Left(apiError)
            }
          }
        }

        // Iterate across the items we sent rather than what we received to ensure we run the callback even if a
        // partition was somehow erroneously excluded from the response. Note that these callbacks are run from
        // the leaderIsrUpdateLock write lock in Partition#sendAlterPartitionRequest
        inflightAlterPartitionItems.foreach { inflightAlterPartition =>
          partitionResponses.get(inflightAlterPartition.topicIdPartition) match {
            case Some(leaderAndIsrOrError) =>
              // Regardless of callback outcome, we need to clear from the unsent updates map to unblock further
              // updates. We clear it now to allow the callback to submit a new update if needed.
              unsentIsrUpdates.remove(inflightAlterPartition.topicIdPartition)
              leaderAndIsrOrError match {
                case Left(error) => inflightAlterPartition.future.completeExceptionally(error.exception)
                case Right(leaderAndIsr) => inflightAlterPartition.future.complete(leaderAndIsr)
              }
            case None =>
              // Don't remove this partition from the update map so it will get re-sent
              warn(s"Partition ${inflightAlterPartition.topicIdPartition} was sent but not included in the response")
          }
        }

      case e =>
        warn(s"Controller returned an unexpected top-level error when handling AlterPartition request: $e")
    }

    Errors.forCode(data.errorCode)
  }
}

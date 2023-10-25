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

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import kafka.api.LeaderAndIsr
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.OperationNotAttemptedException
import org.apache.kafka.common.message.AlterPartitionRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.{AlterPartitionRequest, AlterPartitionResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.util.Scheduler

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

/**
 * Handles updating the ISR by sending AlterPartition requests to the controller (as of 2.7) or by updating ZK directly
 * (prior to 2.7). Updating the ISR is an asynchronous operation, so partitions will learn about the result of their
 * request through a callback.
 *
 * Note that ISR state changes can still be initiated by the controller and sent to the partitions via LeaderAndIsr
 * requests.
 */
trait AlterPartitionManager {
  def start(): Unit = {}

  def shutdown(): Unit = {}

  def submit(
    topicIdPartition: TopicIdPartition,
    leaderAndIsr: LeaderAndIsr,
    controllerEpoch: Int
  ): CompletableFuture[LeaderAndIsr]
}

case class AlterPartitionItem(
  topicIdPartition: TopicIdPartition,
  leaderAndIsr: LeaderAndIsr,
  future: CompletableFuture[LeaderAndIsr],
  controllerEpoch: Int // controllerEpoch needed for `ZkAlterPartitionManager`
)

object AlterPartitionManager {

  /**
   * Factory to AlterPartition based implementation, used when IBP >= 2.7-IV2
   */
  def apply(
    config: KafkaConfig,
    metadataCache: MetadataCache,
    scheduler: Scheduler,
    controllerNodeProvider: ControllerNodeProvider,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: String,
    brokerEpochSupplier: () => Long,
  ): AlterPartitionManager = {
    val channelManager = NodeToControllerChannelManager(
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
      brokerEpochSupplier = brokerEpochSupplier,
      metadataVersionSupplier = () => metadataCache.metadataVersion()
    )
  }

  /**
   * Factory for ZK based implementation, used when IBP < 2.7-IV2
   */
  def apply(
    scheduler: Scheduler,
    time: Time,
    zkClient: KafkaZkClient
  ): AlterPartitionManager = {
    new ZkAlterPartitionManager(scheduler, time, zkClient)
  }
}

class DefaultAlterPartitionManager(
  val controllerChannelManager: NodeToControllerChannelManager,
  val scheduler: Scheduler,
  val time: Time,
  val brokerId: Int,
  val brokerEpochSupplier: () => Long,
  val metadataVersionSupplier: () => MetadataVersion
) extends AlterPartitionManager with Logging {

  // Used to allow only one pending ISR update per partition (visible for testing).
  // Note that we key items by TopicPartition despite using TopicIdPartition while
  // submitting changes. We do this to ensure that topics with the same name but
  // with a different topic id or no topic id collide here. There are two cases to
  // consider:
  // 1) When the cluster is upgraded from IBP < 2.8 to IBP >= 2.8, the ZK controller
  //    assigns topic ids to the partitions. So partitions will start sending updates
  //    with a topic id while they might still have updates without topic ids in this
  //    Map. This would break the contract of only allowing one pending ISR update per
  //    partition.
  // 2) When a topic is deleted and re-created, we cannot have two entries in this Map
  //    especially if we cannot use an AlterPartition request version which supports
  //    topic ids in the end because the two updates with the same name would be merged
  //    together.
  private[server] val unsentIsrUpdates: util.Map[TopicPartition, AlterPartitionItem] = new ConcurrentHashMap[TopicPartition, AlterPartitionItem]()

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
    leaderAndIsr: LeaderAndIsr,
    controllerEpoch: Int
  ): CompletableFuture[LeaderAndIsr] = {
    val future = new CompletableFuture[LeaderAndIsr]()
    val alterPartitionItem = AlterPartitionItem(topicIdPartition, leaderAndIsr, future, controllerEpoch)
    val enqueued = unsentIsrUpdates.putIfAbsent(alterPartitionItem.topicIdPartition.topicPartition, alterPartitionItem) == null
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
    val (request, topicNamesByIds) = buildRequest(inflightAlterPartitionItems, brokerEpoch)
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
                response.requestHeader,
                response.responseBody.asInstanceOf[AlterPartitionResponse],
                brokerEpoch,
                inflightAlterPartitionItems,
                topicNamesByIds
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
   * We could use version 2 or above if all the pending changes have an topic id defined;
   * otherwise we must use version 1 or below.
   *
   * @return A tuple containing the AlterPartitionRequest.Builder and a mapping from
   *         topic id to topic name. This mapping is used in the response handling.
   */
  private def buildRequest(
    inflightAlterPartitionItems: Seq[AlterPartitionItem],
    brokerEpoch: Long
  ): (AlterPartitionRequest.Builder, mutable.Map[Uuid, String]) = {
    val metadataVersion = metadataVersionSupplier()
    // We build this mapping in order to map topic id back to their name when we
    // receive the response. We cannot rely on the metadata cache for this because
    // the metadata cache is updated after the partition state so it might not know
    // yet about a topic id already used here.
    val topicNamesByIds = mutable.HashMap[Uuid, String]()
    // We can use topic ids only if all the pending changed have one defined and
    // we use IBP 2.8 or above.
    var canUseTopicIds = metadataVersion.isTopicIdsSupported

    val message = new AlterPartitionRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpoch)

    inflightAlterPartitionItems.groupBy(_.topicIdPartition.topic).foreach { case (topicName, items) =>
      val topicId = items.head.topicIdPartition.topicId
      canUseTopicIds &= topicId != Uuid.ZERO_UUID
      topicNamesByIds(topicId) = topicName

      // Both the topic name and the topic id are set here because at this stage
      // we don't know which version of the request will be used.
      val topicData = new AlterPartitionRequestData.TopicData()
        .setTopicName(topicName)
        .setTopicId(topicId)
      message.topics.add(topicData)

      items.foreach { item =>
        val partitionData = new AlterPartitionRequestData.PartitionData()
          .setPartitionIndex(item.topicIdPartition.partition)
          .setLeaderEpoch(item.leaderAndIsr.leaderEpoch)
          .setNewIsrWithEpochs(item.leaderAndIsr.isrWithBrokerEpoch.asJava)
          .setPartitionEpoch(item.leaderAndIsr.partitionEpoch)

        if (metadataVersion.isLeaderRecoverySupported) {
          partitionData.setLeaderRecoveryState(item.leaderAndIsr.leaderRecoveryState.value)
        }

        topicData.partitions.add(partitionData)
      }
    }

    // If we cannot use topic ids, the builder will ensure that no version higher than 1 is used.
    (new AlterPartitionRequest.Builder(message, canUseTopicIds), topicNamesByIds)
  }

  def handleAlterPartitionResponse(
    requestHeader: RequestHeader,
    alterPartitionResp: AlterPartitionResponse,
    sentBrokerEpoch: Long,
    inflightAlterPartitionItems: Seq[AlterPartitionItem],
    topicNamesByIds: mutable.Map[Uuid, String]
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
        val partitionResponses = new mutable.HashMap[TopicPartition, Either[Errors, LeaderAndIsr]]()
        data.topics.forEach { topic =>
          // Topic IDs are used since version 2 of the AlterPartition API.
          val topicName = if (requestHeader.apiVersion > 1) topicNamesByIds.get(topic.topicId).orNull else topic.topicName
          if (topicName == null || topicName.isEmpty) {
            error(s"Received an unexpected topic $topic in the alter partition response, ignoring it.")
          } else {
            topic.partitions.forEach { partition =>
              val tp = new TopicPartition(topicName, partition.partitionIndex)
              val apiError = Errors.forCode(partition.errorCode)
              debug(s"Controller successfully handled AlterPartition request for $tp: $partition")
              if (apiError == Errors.NONE) {
                LeaderRecoveryState.optionalOf(partition.leaderRecoveryState).asScala match {
                  case Some(leaderRecoveryState) =>
                    partitionResponses(tp) = Right(
                      LeaderAndIsr(
                        partition.leaderId,
                        partition.leaderEpoch,
                        partition.isr.asScala.toList.map(_.toInt),
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
        }

        // Iterate across the items we sent rather than what we received to ensure we run the callback even if a
        // partition was somehow erroneously excluded from the response. Note that these callbacks are run from
        // the leaderIsrUpdateLock write lock in Partition#sendAlterPartitionRequest
        inflightAlterPartitionItems.foreach { inflightAlterPartition =>
          partitionResponses.get(inflightAlterPartition.topicIdPartition.topicPartition) match {
            case Some(leaderAndIsrOrError) =>
              // Regardless of callback outcome, we need to clear from the unsent updates map to unblock further
              // updates. We clear it now to allow the callback to submit a new update if needed.
              unsentIsrUpdates.remove(inflightAlterPartition.topicIdPartition.topicPartition)
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

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

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{KafkaScheduler, Logging, Scheduler}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.message.ElectLeadersResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ElectLeadersRequest, ElectLeadersResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.TopicPartition

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Transferring the leadership is an asynchronous operation, so partitions will learn about the result of their
 * request through a callback.
 *
 */
trait TransferLeaderManager {
  def start(): Unit = {}

  def shutdown(): Unit = {}

  def submit(transferLeaderItem: TransferLeaderItem): Unit
}

case class TransferLeaderItem(topicPartition: TopicPartition,
  newLeader: Int,
  callback: Either[Errors, TopicPartition] => Unit)

object TransferLeaderManager {
  val defaultTimeout = 60000
  def apply(
    config: KafkaConfig,
    metadataCache: MetadataCache,
    scheduler: KafkaScheduler,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: Option[String],
    brokerEpochSupplier: () => Long,
    brokerId: Int
  ): TransferLeaderManager = {
    val nodeProvider = MetadataCacheControllerNodeProvider(config, metadataCache)

    val channelManager = BrokerToControllerChannelManager(
      controllerNodeProvider = nodeProvider,
      time = time,
      metrics = metrics,
      config = config,
      channelName = "transferLeader",
      threadNamePrefix = threadNamePrefix,
      retryTimeoutMs = Long.MaxValue
    )
    new DefaultTransferLeaderManager(
      controllerChannelManager = channelManager,
      scheduler = scheduler,
      time = time,
      brokerId = brokerId,
      brokerEpochSupplier = brokerEpochSupplier
    )
  }
}

class DefaultTransferLeaderManager(
  val controllerChannelManager: BrokerToControllerChannelManager,
  val scheduler: Scheduler,
  val time: Time,
  val brokerId: Int,
  val brokerEpochSupplier: () => Long
) extends TransferLeaderManager with Logging with KafkaMetricsGroup {

  private[server] val unsentTransferQueue: BlockingQueue[TransferLeaderItem] = new LinkedBlockingQueue()
  // The inflightTransfers map is populated by the unsentTransferQueue, and the items in it are removed in the response callback.
  // Putting new items to the inflightTransfers and removing items from it won't be performed at the same time, and the coordination is
  // done via the inflightRequest flag.
  private[server] val inflightTransfers: util.Map[TopicPartition, TransferLeaderItem] = new ConcurrentHashMap[TopicPartition, TransferLeaderItem]()

  // Used to allow only one in-flight request at a time
  private val inflightRequest: AtomicBoolean = new AtomicBoolean(false)

  override def start(): Unit = {
    controllerChannelManager.start()
  }

  override def shutdown(): Unit = {
    controllerChannelManager.shutdown()
  }

  override def submit(transferLeaderItem: TransferLeaderItem): Unit = {
    unsentTransferQueue.put(transferLeaderItem)
    maybeTransferLeader()
  }

  private[server] def maybeTransferLeader(): Unit = {
    // Send all pending items if there is not already a request in-flight.
    if ((!unsentTransferQueue.isEmpty || !inflightTransfers.isEmpty) && inflightRequest.compareAndSet(false, true)) {
      // Copy current unsent ISRs but don't remove from the map, they get cleared in the response handler
      while (!unsentTransferQueue.isEmpty) {
        val item = unsentTransferQueue.poll()
        // if there are multiple TransferLeaderItems for the same partition in the queue, the last one wins
        // and the previous ones will be discarded
        inflightTransfers.put(item.topicPartition, item)
      }
      // Since the maybeTransferLeader can be called from the response callback,
      // there may be cases where right after the while loop, some new items are added to the unsentIsrQueue in the submit
      // thread. In such a case, the newly added item will be sent in the next request

      val inflightTransferLeaderItems = new ListBuffer[TransferLeaderItem]()
      inflightTransfers.values().forEach(item => inflightTransferLeaderItems.append(item))
      sendRequest(inflightTransferLeaderItems)
    }
  }

  private[server] def clearInFlightRequest(): Unit = {
    if (!inflightRequest.compareAndSet(true, false)) {
      warn("Attempting to clear TransferLeader in-flight flag when no apparent request is in-flight")
    }
  }

  private def sendRequest(inflightTransferLeaderItems: Seq[TransferLeaderItem]): Unit = {
    val brokerEpoch = brokerEpochSupplier.apply()
    val request = buildRequest(inflightTransferLeaderItems, brokerEpoch)
    debug(s"Sending TransferLeader to controller $request")

    // We will not timeout the ElectLeaders request, instead letting it retry indefinitely
    // until a response is received.
    controllerChannelManager.sendRequest(request,
      new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          debug(s"Received TransferLeader response $response")
          val error = try {
            if (response.authenticationException != null) {
              // For now we treat authentication errors as retriable. We use the
              // `NETWORK_EXCEPTION` error code for lack of a good alternative.
              // Note that `BrokerToControllerChannelManager` will still log the
              // authentication errors so that users have a chance to fix the problem.
              Errors.NETWORK_EXCEPTION
            } else if (response.versionMismatch != null) {
              Errors.UNSUPPORTED_VERSION
            } else {
              val body = response.responseBody().asInstanceOf[ElectLeadersResponse]
              handleTransferLeaderResponse(body, brokerEpoch, inflightTransferLeaderItems)
            }
          } finally {
            // clear the flag so future requests can proceed
            clearInFlightRequest()
          }

          // check if we need to send another request right away
          error match {
              case Errors.NONE =>
                // In the normal case, check for pending updates to send immediately
                maybeTransferLeader()
              case _ =>
                // If we received a top-level error from the controller, retry the request in the near future
                scheduler.schedule("send-alter-isr", () => maybeTransferLeader(), 50, -1, TimeUnit.MILLISECONDS)
            }
        }

        override def onTimeout(): Unit = {
          throw new IllegalStateException("Encountered unexpected timeout when sending TransferLeader to the controller")
        }
      })
  }

  private def buildRequest(inflightTransferLeaderItems: Seq[TransferLeaderItem], brokerEpoch: Long): ElectLeadersRequest.Builder = {
    val recommendedLeaders = new util.HashMap[TopicPartition, Integer]()

    inflightTransferLeaderItems.groupBy(_.topicPartition.topic).foreach(entry => {
      entry._2.foreach(item => {
        recommendedLeaders.put(item.topicPartition, item.newLeader)
      })
    })
    new ElectLeadersRequest.Builder(brokerEpoch, recommendedLeaders, TransferLeaderManager.defaultTimeout)
  }

  def handleTransferLeaderResponse(transferLeaderResponse: ElectLeadersResponse,
                             sentBrokerEpoch: Long,
                             inflightTransferLeaderItems: Seq[TransferLeaderItem]): Errors = {
    val data: ElectLeadersResponseData = transferLeaderResponse.data

    Errors.forCode(data.errorCode) match {
      case Errors.STALE_BROKER_EPOCH =>
        warn(s"Broker had a stale broker epoch ($sentBrokerEpoch), retrying.")
      case Errors.CLUSTER_AUTHORIZATION_FAILED =>
        error(s"Broker is not authorized to send TransferLeader to controller",
          Errors.CLUSTER_AUTHORIZATION_FAILED.exception("Broker is not authorized to send TransferLeader to controller"))
      case Errors.NONE =>
        // Collect partition-level responses to pass to the callbacks
        val partitionResponses: mutable.Map[TopicPartition, Either[Errors, TopicPartition]] =
          new mutable.HashMap[TopicPartition, Either[Errors, TopicPartition]]()
        data.replicaElectionResults().forEach { topicResult =>
          topicResult.partitionResult().forEach(partitionResult => {
            val tp = new TopicPartition(topicResult.topic, partitionResult.partitionId())
            val error = Errors.forCode(partitionResult.errorCode())
            debug(s"Controller successfully handled TransferLeader request for $tp: $partitionResult")
            if (error == Errors.NONE) {
              partitionResponses(tp) = Right(tp)
            } else {
              partitionResponses(tp) = Left(error)
            }
          })
        }

        // Iterate across the items we sent rather than what we received to ensure we run the callback even if a
        // partition was somehow erroneously excluded from the response.
        inflightTransferLeaderItems.foreach(inflightTransferLeader =>
          if (partitionResponses.contains(inflightTransferLeader.topicPartition)) {
            try {
              inflightTransferLeader.callback.apply(partitionResponses(inflightTransferLeader.topicPartition))
            } finally {
              // Regardless of callback outcome, we need to clear from the unsent updates map to unblock further updates
              inflightTransfers.remove(inflightTransferLeader.topicPartition)
            }
          } else {
            // Don't remove this partition from the update map so it will get re-sent
            warn(s"Partition ${inflightTransferLeader.topicPartition} was sent but not included in the response")
          }
        )
      case e: Errors =>
        warn(s"Controller returned an unexpected top-level error when handling TransferLeader request: $e")
    }

    Errors.forCode(data.errorCode)
  }
}

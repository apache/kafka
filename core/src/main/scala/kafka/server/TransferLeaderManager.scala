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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ElectLeadersResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, ElectLeadersRequest, ElectLeadersResponse}
import org.apache.kafka.common.utils.Time

import java.util
import scala.collection.mutable

/**
 * Transferring the leadership is an asynchronous operation, so partitions will learn about the result of their
 * request through a callback.
 *
 */
trait TransferLeaderManager extends BrokerToControllerRequestManager[TransferLeaderItem] {
}

case class TransferLeaderItem(topicPartition: TopicPartition,
  newLeader: Int,
  callback: Either[Errors, TopicPartition] => Unit) extends BrokerToControllerRequestItem

object TransferLeaderManager {
  def apply( config: KafkaConfig,
    metadataCache: MetadataCache,
    scheduler: KafkaScheduler,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: Option[String],
    brokerEpochSupplier: () => Long,
    brokerId: Int): TransferLeaderManager = {
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
  controllerChannelManager: BrokerToControllerChannelManager,
  scheduler: Scheduler,
  time: Time,
  brokerId: Int,
  brokerEpochSupplier: () => Long
) extends AbstractBrokerToControllerRequestManager[TransferLeaderItem](controllerChannelManager, scheduler, time, brokerId, brokerEpochSupplier) with TransferLeaderManager with Logging with KafkaMetricsGroup {
  override def buildRequest(inflightItems: Seq[TransferLeaderItem], brokerEpoch: Long): AbstractRequest.Builder[_ <: AbstractRequest] = {
    val recommendedLeaders = new util.HashMap[TopicPartition, Integer]()

    inflightItems.groupBy(_.topicPartition.topic).foreach(entry => {
      entry._2.foreach(item => {
        recommendedLeaders.put(item.topicPartition, item.newLeader)
      })
    })
    new ElectLeadersRequest.Builder(brokerEpoch, recommendedLeaders, BrokerToControllerRequestManager.defaultTimeout)
  }

  override def handleResponse(clientResponse: ClientResponse, sentBrokerEpoch: Long, itemsToSend: Seq[TransferLeaderItem]): Errors = {
    val transferLeaderResponse = clientResponse.responseBody().asInstanceOf[ElectLeadersResponse]
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
        itemsToSend.foreach(inflightTransferLeader =>
          if (partitionResponses.contains(inflightTransferLeader.topicPartition)) {
            try {
              inflightTransferLeader.callback.apply(partitionResponses(inflightTransferLeader.topicPartition))
            } finally {
              // Regardless of callback outcome, we need to clear from the unsent updates map to unblock further updates
              removeInflightItem(inflightTransferLeader.topicPartition)
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

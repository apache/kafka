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

import kafka.api.LeaderAndIsr
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{KafkaScheduler, Logging, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{AlterIsrRequestData, AlterIsrResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AlterIsrRequest, AlterIsrResponse}
import org.apache.kafka.common.utils.Time

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles updating the ISR by sending AlterIsr requests to the controller (as of 2.7) or by updating ZK directly
 * (prior to 2.7). Updating the ISR is an asynchronous operation, so partitions will learn about the result of their
 * request through a callback.
 *
 * Note that ISR state changes can still be initiated by the controller and sent to the partitions via LeaderAndIsr
 * requests.
 */
trait AlterIsrManager {
  def start(): Unit

  def shutdown(): Unit

  def submit(alterIsrItem: AlterIsrItem): Boolean
}

case class AlterIsrItem(topicPartition: TopicPartition,
                        leaderAndIsr: LeaderAndIsr,
                        callback: Either[Errors, LeaderAndIsr] => Unit,
                        controllerEpoch: Int) extends BrokerToControllerRequestItem // controllerEpoch needed for Zk impl

object AlterIsrManager {

  /**
   * Factory for ZK based implementation, used when IBP < 2.7-IV2
   */
  def apply(
    scheduler: Scheduler,
    time: Time,
    zkClient: KafkaZkClient
  ): AlterIsrManager = {
    new ZkIsrManager(scheduler, time, zkClient)
  }

  def apply(
    config: KafkaConfig,
    metadataCache: MetadataCache,
    scheduler: KafkaScheduler,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: Option[String],
    brokerEpochSupplier: () => Long,
    brokerId: Int
  ): AlterIsrManager = {
    val nodeProvider = MetadataCacheControllerNodeProvider(config, metadataCache)

    val channelManager = BrokerToControllerChannelManager(
      controllerNodeProvider = nodeProvider,
      time = time,
      metrics = metrics,
      config = config,
      channelName = "alterIsr",
      threadNamePrefix = threadNamePrefix,
      retryTimeoutMs = Long.MaxValue
    )

    new DefaultAlterIsrManager(
      controllerChannelManager = channelManager,
      scheduler = scheduler,
      time = time,
      brokerId = brokerId,
      brokerEpochSupplier = brokerEpochSupplier
    )
  }
}

class DefaultAlterIsrManager(
  controllerChannelManager: BrokerToControllerChannelManager,
  scheduler: Scheduler,
  time: Time,
  brokerId: Int,
  brokerEpochSupplier: () => Long
) extends AbstractBrokerToControllerRequestManager[AlterIsrItem](controllerChannelManager, scheduler, time, brokerId, brokerEpochSupplier) with AlterIsrManager with Logging with KafkaMetricsGroup {
  override def buildRequest(inflightItems: Seq[AlterIsrItem], brokerEpoch: Long): AbstractRequest.Builder[_ <: AbstractRequest] = {
    val message = new AlterIsrRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpochSupplier.apply())
      .setTopics(new util.ArrayList())

    inflightItems.groupBy(_.topicPartition.topic).foreach(entry => {
      val topicPart = new AlterIsrRequestData.TopicData()
        .setName(entry._1)
        .setPartitions(new util.ArrayList())
      message.topics().add(topicPart)
      entry._2.foreach(item => {
        topicPart.partitions().add(new AlterIsrRequestData.PartitionData()
          .setPartitionIndex(item.topicPartition.partition)
          .setLeaderEpoch(item.leaderAndIsr.leaderEpoch)
          .setNewIsr(item.leaderAndIsr.isr.map(Integer.valueOf).asJava)
          .setCurrentIsrVersion(item.leaderAndIsr.zkVersion)
        )
      })
    })
    new AlterIsrRequest.Builder(message)
  }

  override def handleResponse(clientResponse: ClientResponse, sentBrokerEpoch: Long, inflightItems: Seq[AlterIsrItem]): Errors = {
    val alterIsrResponse = clientResponse.responseBody().asInstanceOf[AlterIsrResponse]

    val data: AlterIsrResponseData = alterIsrResponse.data

    Errors.forCode(data.errorCode) match {
      case Errors.STALE_BROKER_EPOCH =>
        warn(s"Broker had a stale broker epoch ($sentBrokerEpoch), retrying.")
      case Errors.CLUSTER_AUTHORIZATION_FAILED =>
        error(s"Broker is not authorized to send AlterIsr to controller",
          Errors.CLUSTER_AUTHORIZATION_FAILED.exception("Broker is not authorized to send AlterIsr to controller"))
      case Errors.NONE =>
        // Collect partition-level responses to pass to the callbacks
        val partitionResponses: mutable.Map[TopicPartition, Either[Errors, LeaderAndIsr]] =
          new mutable.HashMap[TopicPartition, Either[Errors, LeaderAndIsr]]()
        data.topics.forEach { topic =>
          topic.partitions().forEach(partition => {
            val tp = new TopicPartition(topic.name, partition.partitionIndex)
            val error = Errors.forCode(partition.errorCode())
            debug(s"Controller successfully handled AlterIsr request for $tp: $partition")
            if (error == Errors.NONE) {
              val newLeaderAndIsr = new LeaderAndIsr(partition.leaderId, partition.leaderEpoch,
                partition.isr.asScala.toList.map(_.toInt), partition.currentIsrVersion)
              partitionResponses(tp) = Right(newLeaderAndIsr)
            } else {
              partitionResponses(tp) = Left(error)
            }
          })
        }

        // Iterate across the items we sent rather than what we received to ensure we run the callback even if a
        // partition was somehow erroneously excluded from the response. Note that these callbacks are run from
        // the leaderIsrUpdateLock write lock in Partition#sendAlterIsrRequest
        inflightItems.foreach(inflightAlterIsr =>
          if (partitionResponses.contains(inflightAlterIsr.topicPartition)) {
            try {
              inflightAlterIsr.callback.apply(partitionResponses(inflightAlterIsr.topicPartition))
            } finally {
              // Regardless of callback outcome, we need to clear from the unsent updates map to unblock further updates
              removeInflightItem(inflightAlterIsr.topicPartition)
            }
          } else {
            // Don't remove this partition from the update map so it will get re-sent
            warn(s"Partition ${inflightAlterIsr.topicPartition} was sent but not included in the response")
          }
        )
      case e: Errors =>
        warn(s"Controller returned an unexpected top-level error when handling AlterIsr request: $e")
    }

    Errors.forCode(data.errorCode)
  }

}

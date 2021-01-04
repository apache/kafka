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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import kafka.api.LeaderAndIsr
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{KafkaScheduler, Logging, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{AlterIsrRequestData, AlterIsrResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterIsrRequest, AlterIsrResponse}
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
  def start(): Unit = {}

  def shutdown(): Unit = {}

  def submit(alterIsrItem: AlterIsrItem): Boolean

  def clearPending(topicPartition: TopicPartition): Unit
}

case class AlterIsrItem(topicPartition: TopicPartition,
                        leaderAndIsr: LeaderAndIsr,
                        callback: Either[Errors, LeaderAndIsr] => Unit,
                        controllerEpoch: Int) // controllerEpoch needed for Zk impl

object AlterIsrManager {

  /**
   * Factory to AlterIsr based implementation, used when IBP >= 2.7-IV2
   */
  def apply(
    config: KafkaConfig,
    metadataCache: MetadataCache,
    scheduler: KafkaScheduler,
    time: Time,
    metrics: Metrics,
    threadNamePrefix: Option[String],
    brokerEpochSupplier: () => Long
  ): AlterIsrManager = {
    val channelManager = new BrokerToControllerChannelManager(
      metadataCache = metadataCache,
      time = time,
      metrics = metrics,
      config = config,
      channelName = "alterIsrChannel",
      threadNamePrefix = threadNamePrefix,
      retryTimeoutMs = Long.MaxValue
    )
    new DefaultAlterIsrManager(
      controllerChannelManager = channelManager,
      scheduler = scheduler,
      time = time,
      brokerId = config.brokerId,
      brokerEpochSupplier = brokerEpochSupplier
    )
  }

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

}

class DefaultAlterIsrManager(
  val controllerChannelManager: BrokerToControllerChannelManager,
  val scheduler: Scheduler,
  val time: Time,
  val brokerId: Int,
  val brokerEpochSupplier: () => Long
) extends AlterIsrManager with Logging with KafkaMetricsGroup {

  // Used to allow only one pending ISR update per partition
  private val unsentIsrUpdates: util.Map[TopicPartition, AlterIsrItem] = new ConcurrentHashMap[TopicPartition, AlterIsrItem]()

  // Used to allow only one in-flight request at a time
  private val inflightRequest: AtomicBoolean = new AtomicBoolean(false)

  private val lastIsrPropagationMs = new AtomicLong(0)

  override def start(): Unit = {
    controllerChannelManager.start()
    scheduler.schedule("send-alter-isr", propagateIsrChanges, 50, 50, TimeUnit.MILLISECONDS)
  }

  override def shutdown(): Unit = {
    controllerChannelManager.shutdown()
  }

  override def submit(alterIsrItem: AlterIsrItem): Boolean = {
    unsentIsrUpdates.putIfAbsent(alterIsrItem.topicPartition, alterIsrItem) == null
  }

  override def clearPending(topicPartition: TopicPartition): Unit = {
    unsentIsrUpdates.remove(topicPartition)
  }

  private def propagateIsrChanges(): Unit = {
    if (!unsentIsrUpdates.isEmpty && inflightRequest.compareAndSet(false, true)) {
      // Copy current unsent ISRs but don't remove from the map
      val inflightAlterIsrItems = new ListBuffer[AlterIsrItem]()
      unsentIsrUpdates.values().forEach(item => inflightAlterIsrItems.append(item))

      val now = time.milliseconds()
      lastIsrPropagationMs.set(now)
      sendRequest(inflightAlterIsrItems.toSeq)
    }
  }

  private def sendRequest(inflightAlterIsrItems: Seq[AlterIsrItem]): Unit = {
    val message = buildRequest(inflightAlterIsrItems)

    def clearInflightRequests(): Unit = {
      // Be sure to clear the in-flight flag to allow future AlterIsr requests
      if (!inflightRequest.compareAndSet(true, false)) {
        throw new IllegalStateException("AlterIsr response callback called when no requests were in flight")
      }
    }

    debug(s"Sending AlterIsr to controller $message")

    // We will not timeout AlterISR request, instead letting it retry indefinitely
    // until a response is received, or a new LeaderAndIsr overwrites the existing isrState
    // which causes the inflight requests to be ignored.
    controllerChannelManager.sendRequest(new AlterIsrRequest.Builder(message),
      new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          try {
            debug(s"Received AlterIsr response $response")
            val body = response.responseBody().asInstanceOf[AlterIsrResponse]
            handleAlterIsrResponse(body, message.brokerEpoch, inflightAlterIsrItems)
          } finally {
            clearInflightRequests()
          }
        }

        override def onTimeout(): Unit = {
          throw new IllegalStateException("Encountered unexpected timeout when sending AlterIsr to the controller")
        }
      })
  }

  private def buildRequest(inflightAlterIsrItems: Seq[AlterIsrItem]): AlterIsrRequestData = {
    val message = new AlterIsrRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpochSupplier.apply())
      .setTopics(new util.ArrayList())

    inflightAlterIsrItems.groupBy(_.topicPartition.topic).foreach(entry => {
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
    message
  }

  def handleAlterIsrResponse(alterIsrResponse: AlterIsrResponse,
                             sentBrokerEpoch: Long,
                             inflightAlterIsrItems: Seq[AlterIsrItem]): Unit = {
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
        inflightAlterIsrItems.foreach(inflightAlterIsr =>
          if (partitionResponses.contains(inflightAlterIsr.topicPartition)) {
            try {
              inflightAlterIsr.callback.apply(partitionResponses(inflightAlterIsr.topicPartition))
            } finally {
              // Regardless of callback outcome, we need to clear from the unsent updates map to unblock further updates
              unsentIsrUpdates.remove(inflightAlterIsr.topicPartition)
            }
          } else {
            // Don't remove this partition from the update map so it will get re-sent
            warn(s"Partition ${inflightAlterIsr.topicPartition} was sent but not included in the response")
          }
        )
      case e: Errors =>
        warn(s"Controller returned an unexpected top-level error when handling AlterIsr request: $e")
    }
  }
}

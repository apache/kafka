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
import kafka.utils.{Logging, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{AlterIsrRequestData, AlterIsrResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AlterIsrRequest, AlterIsrResponse}
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles the sending of AlterIsr requests to the controller. Updating the ISR is an asynchronous operation,
 * so partitions will learn about updates through LeaderAndIsr messages sent from the controller
 */
trait AlterIsrManager {
  def start(): Unit

  def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Boolean

  def clearPending(topicPartition: TopicPartition): Unit
}

case class AlterIsrItem(topicPartition: TopicPartition, leaderAndIsr: LeaderAndIsr, callback: Either[Errors, LeaderAndIsr] => Unit)

class AlterIsrManagerImpl(val controllerChannelManager: BrokerToControllerChannelManager,
                          val zkClient: KafkaZkClient,
                          val scheduler: Scheduler,
                          val time: Time,
                          val brokerId: Int,
                          val brokerEpochSupplier: () => Long) extends AlterIsrManager with Logging with KafkaMetricsGroup {

  // Used to allow only one pending ISR update per partition
  private val unsentIsrUpdates: util.Map[TopicPartition, AlterIsrItem] = new ConcurrentHashMap[TopicPartition, AlterIsrItem]()

  // Used to allow only one in-flight request at a time
  private val inflightRequest: AtomicBoolean = new AtomicBoolean(false)

  private val lastIsrPropagationMs = new AtomicLong(0)

  override def start(): Unit = {
    scheduler.schedule("send-alter-isr", propagateIsrChanges, 50, 50, TimeUnit.MILLISECONDS)
  }

  override def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Boolean = {
    unsentIsrUpdates.putIfAbsent(alterIsrItem.topicPartition, alterIsrItem) == null
  }

  override def clearPending(topicPartition: TopicPartition): Unit = {
    unsentIsrUpdates.remove(topicPartition)
  }

  private def propagateIsrChanges(): Unit = {
    if (!unsentIsrUpdates.isEmpty && inflightRequest.compareAndSet(false, true)) {
      // Copy current unsent ISRs but don't remove from the map
      val inflightAlterIsrItems: mutable.Queue[AlterIsrItem] = new mutable.Queue[AlterIsrItem]()
      unsentIsrUpdates.values().forEach(item => inflightAlterIsrItems.enqueue(item))

      val now = time.milliseconds()
      lastIsrPropagationMs.set(now)

      buildAndSendRequest(inflightAlterIsrItems.toSeq)
    }
  }

  def buildAndSendRequest(inflightAlterIsrItems: Seq[AlterIsrItem]): Unit = {
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
          .setLeaderId(item.leaderAndIsr.leader)
          .setLeaderEpoch(item.leaderAndIsr.leaderEpoch)
          .setNewIsr(item.leaderAndIsr.isr.map(Integer.valueOf).asJava)
          .setCurrentIsrVersion(item.leaderAndIsr.zkVersion)
        )
      })
    })

    def responseHandler(response: ClientResponse): Unit = {
      try {
        val body: AlterIsrResponse = response.responseBody().asInstanceOf[AlterIsrResponse]
        handleAlterIsrResponse(body, inflightAlterIsrItems)
      } finally {
        // Be sure to clear the in-flight flag to allow future requests
        if (!inflightRequest.compareAndSet(true, false)) {
          throw new IllegalStateException("AlterIsr response callback called when no requests were in flight")
        }
      }
    }

    debug(s"Sending AlterIsr to controller $message")
    controllerChannelManager.sendRequest(new AlterIsrRequest.Builder(message), responseHandler)
  }
  
  def handleAlterIsrResponse(alterIsrResponse: AlterIsrResponse, inflightAlterIsrItems: Seq[AlterIsrItem]): Unit = {
    val data: AlterIsrResponseData = alterIsrResponse.data

    Errors.forCode(data.errorCode) match {
      case Errors.STALE_BROKER_EPOCH =>
        warn(s"Broker had a stale broker epoch, retrying.")
      case Errors.CLUSTER_AUTHORIZATION_FAILED =>
        warn(s"Broker is not authorized to send AlterIsr to controller")
        throw Errors.CLUSTER_AUTHORIZATION_FAILED.exception("Broker is not authorized to send AlterIsr to controller")
      case Errors.NONE =>
        // Collect partition-level responses to pass to the callbacks
        val partitionResponses: mutable.Map[TopicPartition, Either[Errors, LeaderAndIsr]] =
          new mutable.HashMap[TopicPartition, Either[Errors, LeaderAndIsr]]()
        debug(s"Controller successfully handled AlterIsr request")
        data.topics.forEach(topic => {
          topic.partitions().forEach(partition => {
            val tp = new TopicPartition(topic.name, partition.partitionIndex)
            if (partition.errorCode() == Errors.NONE.code()) {
              val newLeaderAndIsr = new LeaderAndIsr(partition.leader(), partition.leaderEpoch(),
                partition.isr().asScala.toList.map(_.toInt), partition.currentIsrVersion)
              partitionResponses(tp) = Right(newLeaderAndIsr)
            } else {
              partitionResponses(tp) = Left(Errors.forCode(partition.errorCode()))
            }
          })
        })

        // Iterate across the items we sent rather than what we received to ensure we run the callback even if a
        // partition was somehow erroneously excluded from the response. Note that these callbacks are run from
        // the leaderIsrUpdateLock write lock in Partition#sendAlterIsrRequest
        inflightAlterIsrItems.foreach(inflightAlterIsr => try {
          if (partitionResponses.contains(inflightAlterIsr.topicPartition)) {
            inflightAlterIsr.callback.apply(partitionResponses(inflightAlterIsr.topicPartition))
          } else {
            inflightAlterIsr.callback.apply(Left(Errors.UNKNOWN_SERVER_ERROR)) // TODO better error here?
          }
        } finally {
          // Clear this partition from the map to allow for future ISR updates
          unsentIsrUpdates.remove(inflightAlterIsr.topicPartition)
        })
      case e: Errors =>
        warn(s"Controller returned an unexpected top-level error when handling AlterIsr request: $e")
    }
  }
}

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
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

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
  def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Unit

  def clearPending(topicPartition: TopicPartition): Unit
}

case class AlterIsrItem(topicPartition: TopicPartition, leaderAndIsr: LeaderAndIsr, callback: Either[Errors, LeaderAndIsr] => Unit)

class AlterIsrManagerImpl(val controllerChannelManager: BrokerToControllerChannelManager,
                          val zkClient: KafkaZkClient,
                          val scheduler: Scheduler,
                          val time: Time,
                          val brokerId: Int,
                          val brokerEpochSupplier: () => Long) extends AlterIsrManager with Logging with KafkaMetricsGroup {

  private val unsentIsrUpdates: mutable.Queue[AlterIsrItem] = new mutable.Queue[AlterIsrItem]()
  private val lastIsrChangeMs = new AtomicLong(0)
  private val lastIsrPropagationMs = new AtomicLong(0)

  @volatile private var scheduledRequest: Option[ScheduledFuture[_]] = None

  override def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Unit = {
    unsentIsrUpdates synchronized {
      unsentIsrUpdates.enqueue(alterIsrItem)
      lastIsrChangeMs.set(time.milliseconds)
      // Rather than sending right away, we'll delay at most 50ms to allow for batching of ISR changes happening
      // in fast succession
      if (scheduledRequest.isEmpty) {
        scheduledRequest = Some(scheduler.schedule("propagate-alter-isr", propagateIsrChanges, 50, -1, TimeUnit.MILLISECONDS))
      }
    }
  }

  override def clearPending(topicPartition: TopicPartition): Unit = {
    unsentIsrUpdates synchronized {
      unsentIsrUpdates.dequeueAll(_.topicPartition == topicPartition)
    }
  }

  private def propagateIsrChanges(): Unit = {
    val now = time.milliseconds()

    // Minimize time in this lock since it's also held during fetch hot path
    val copy: Seq[AlterIsrItem] = unsentIsrUpdates synchronized {
      lastIsrPropagationMs.set(now)
      unsentIsrUpdates.dequeueAll(_ => true)
    }

    // Send the request and clear the optional. We rely on BrokerToControllerChannelManager for one limiting to one
    // in-flight request at a time
    buildAndSendRequest(copy)
    scheduledRequest = None
  }

  def buildAndSendRequest(isrUpdates: Seq[AlterIsrItem]): Unit = {
    if (isrUpdates.nonEmpty) {
      val message = new AlterIsrRequestData()
        .setBrokerId(brokerId)
        .setBrokerEpoch(brokerEpochSupplier.apply())
        .setTopics(new util.ArrayList())

      // N.B., these callbacks are run inside the leaderIsrUpdateLock write lock
      val callbacks = new mutable.HashMap[TopicPartition, Either[Errors, LeaderAndIsr] => Unit]()
      isrUpdates.groupBy(_.topicPartition.topic).foreach(entry => {
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
          callbacks(item.topicPartition) = item.callback
        })
      })

      def responseHandler(response: ClientResponse): Unit = {
        val body: AlterIsrResponse = response.responseBody().asInstanceOf[AlterIsrResponse]
        val data: AlterIsrResponseData = body.data
        Errors.forCode(data.errorCode) match {
          case Errors.NONE =>
            debug(s"Controller successfully handled AlterIsr request")
            data.topics.forEach(topic => {
              topic.partitions().forEach(partition => {
                if (partition.errorCode() == Errors.NONE.code()) {
                  val newLeaderAndIsr = new LeaderAndIsr(partition.leader(), partition.leaderEpoch(),
                    partition.isr().asScala.toList.map(_.toInt), partition.currentIsrVersion)
                  callbacks(new TopicPartition(topic.name, partition.partitionIndex))(Right(newLeaderAndIsr))
                } else {
                  callbacks(new TopicPartition(topic.name, partition.partitionIndex))(Left(Errors.forCode(partition.errorCode())))
                }
              })
            })
          case Errors.STALE_BROKER_EPOCH =>
            warn(s"Broker had a stale broker epoch, retrying.")
            data.topics.forEach(topic => {
              // Bubble this up to the partition so we can re-create the request
              topic.partitions().forEach(partition => {
                callbacks(new TopicPartition(topic.name, partition.partitionIndex))(Left(Errors.STALE_BROKER_EPOCH))
              })
            })
          case Errors.CLUSTER_AUTHORIZATION_FAILED =>
            warn(s"Broker is not authorized to send AlterIsr to controller")
          case e: Errors =>
            warn(s"Controller returned an unexpected top-level error when handling AlterIsr request: $e")
        }
      }

      debug(s"Sending AlterIsr to controller $message")
      controllerChannelManager.sendRequest(new AlterIsrRequest.Builder(message), responseHandler)
    }
  }
}

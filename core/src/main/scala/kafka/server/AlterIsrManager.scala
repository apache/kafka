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
import org.apache.kafka.common.message.AlterIsrRequestData.{AlterIsrRequestPartitions, AlterIsrRequestTopics}
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

case class AlterIsrItem(topicPartition: TopicPartition, leaderAndIsr: LeaderAndIsr, callback: Errors => Unit)

class AlterIsrManagerImpl(val controllerChannelManager: BrokerToControllerChannelManager,
                          val zkClient: KafkaZkClient,
                          val scheduler: Scheduler,
                          val time: Time,
                          val brokerId: Int) extends AlterIsrManager with Logging with KafkaMetricsGroup {

  private val unsentIsrUpdates: mutable.Map[TopicPartition, AlterIsrItem] = new mutable.HashMap[TopicPartition, AlterIsrItem]()
  private val lastIsrChangeMs = new AtomicLong(0)
  private val lastIsrPropagationMs = new AtomicLong(0)

  @volatile private var scheduledRequest: Option[ScheduledFuture[_]] = None

  override def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Unit = {
    unsentIsrUpdates synchronized {
      unsentIsrUpdates(alterIsrItem.topicPartition) = alterIsrItem
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
      // when we get a new LeaderAndIsr, we clear out any pending requests
      unsentIsrUpdates.remove(topicPartition)
    }
  }

  private def propagateIsrChanges(): Unit = {
    val now = time.milliseconds()
    unsentIsrUpdates synchronized {
      if (unsentIsrUpdates.nonEmpty) {
        val brokerEpoch: Long = zkClient.getBrokerEpoch(brokerId) match {
          case Some(brokerEpoch) => brokerEpoch
          case None => throw new RuntimeException("Cannot send AlterIsr because we cannot determine broker epoch")
        }

        val message = new AlterIsrRequestData()
          .setBrokerId(brokerId)
          .setBrokerEpoch(brokerEpoch)
          .setTopics(new util.ArrayList())

        val callbacks = new mutable.HashMap[TopicPartition, Errors => Unit]()
        unsentIsrUpdates.values.groupBy(_.topicPartition.topic).foreachEntry((topic, items) => {
          val topicPart = new AlterIsrRequestTopics()
            .setName(topic)
            .setPartitions(new util.ArrayList())
          message.topics().add(topicPart)
          items.foreach(item => {
            topicPart.partitions().add(new AlterIsrRequestPartitions()
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
              info(s"Controller handled AlterIsr request")
              data.topics.forEach(topic => {
                topic.partitions().forEach(partition => {
                  callbacks(new TopicPartition(topic.name, partition.partitionIndex))(
                    Errors.forCode(partition.errorCode))
                })
              })
            case e: Errors =>
              // Need to propagate top-level errors back to all partitions so they can react accordingly
              warn(s"Controller returned a top-level error when handling AlterIsr request: $e")
              data.topics.forEach(topic => {
                topic.partitions().forEach(partition => {
                  callbacks(new TopicPartition(topic.name, partition.partitionIndex))(e)
                })
              })
          }
        }

        debug(s"Sending AlterIsr to controller $message")
        controllerChannelManager.sendRequest(new AlterIsrRequest.Builder(message), responseHandler)

        unsentIsrUpdates.clear()
        lastIsrPropagationMs.set(now)
      }
      scheduledRequest = None
    }
  }
}

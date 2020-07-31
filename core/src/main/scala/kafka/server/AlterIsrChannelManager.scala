package kafka.server

import java.util
import java.util.concurrent.TimeUnit
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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles the sending of AlterIsr requests to the controller. Updating the ISR is an asynchronous operation,
 * so partitions will learn about updates through LeaderAndIsr messages sent from the controller
 */
trait AlterIsrChannelManager {
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L

  def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Unit

  def startup(): Unit

  def shutdown(): Unit
}

case class AlterIsrItem(topicPartition: TopicPartition, leaderAndIsr: LeaderAndIsr, callback: Errors => Unit)

class AlterIsrChannelManagerImpl(val controllerChannelManager: BrokerToControllerChannelManager,
                                 val zkClient: KafkaZkClient,
                                 val scheduler: Scheduler,
                                 val brokerId: Int,
                                 val brokerEpoch: Long) extends AlterIsrChannelManager with Logging with KafkaMetricsGroup {

  private val pendingIsrUpdates: mutable.Queue[AlterIsrItem] = new mutable.Queue[AlterIsrItem]()
  private val lastIsrChangeMs = new AtomicLong(0)
  private val lastIsrPropagationMs = new AtomicLong(0)

  override def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Unit = {
    pendingIsrUpdates synchronized {
      pendingIsrUpdates += alterIsrItem
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }

  override def startup(): Unit = {
    scheduler.schedule("alter-isr-send", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    controllerChannelManager.start()
  }

  override def shutdown(): Unit = {
    controllerChannelManager.shutdown()
  }

  private def maybePropagateIsrChanges(): Unit = {
    val now = System.currentTimeMillis()
    pendingIsrUpdates synchronized {
      if (pendingIsrUpdates.nonEmpty &&
        (lastIsrChangeMs.get() + IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + IsrChangePropagationInterval < now)) {

        // Max ISRs to send?
        val message = new AlterIsrRequestData()
          .setBrokerId(brokerId)
          .setBrokerEpoch(brokerEpoch)
          .setTopics(new util.ArrayList())

        val callbacks = mutable.Map[TopicPartition, Errors => Unit]()
        pendingIsrUpdates.groupBy(_.topicPartition.topic()).foreachEntry((topic, items) => {
          val topicPart = new AlterIsrRequestTopics()
            .setName(topic)
            .setPartitions(new util.ArrayList())
          message.topics().add(topicPart)
          items.foreach(item => {
            topicPart.partitions().add(new AlterIsrRequestPartitions()
              .setPartitionIndex(item.topicPartition.partition())
              .setLeaderId(item.leaderAndIsr.leader)
              .setLeaderEpoch(item.leaderAndIsr.leaderEpoch)
              .setNewIsr(item.leaderAndIsr.isr.map(Integer.valueOf).asJava)
              .setCurrentIsrVersion(item.leaderAndIsr.zkVersion)
            )
            callbacks(item.topicPartition) = item.callback
          })
        })

        def responseHandler(response: ClientResponse): Unit = {
          println(response.responseBody().toString(response.requestHeader().apiVersion()))
          val body: AlterIsrResponse = response.responseBody().asInstanceOf[AlterIsrResponse]
          val data: AlterIsrResponseData = body.data()
          Errors.forCode(data.errorCode()) match {
            case Errors.NONE => info("Success from controller when updating ISR")
            case e: Errors => warn(s"Got $e from controller when updating ISR")
          }
          data.topics().forEach(topic => {
            topic.partitions().forEach(partition => {
              callbacks.remove(new TopicPartition(topic.name(), partition.partitionIndex()))
                .foreach(_.apply(Errors.forCode(partition.errorCode())))
            })
          })
          // Remaining callbacks were not included in response
          callbacks.values.foreach(_.apply(Errors.UNKNOWN_SERVER_ERROR))
        }
        info("Sending AlterIsr to controller")
        controllerChannelManager.sendRequest(new AlterIsrRequest.Builder(message), responseHandler)

        pendingIsrUpdates.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

}
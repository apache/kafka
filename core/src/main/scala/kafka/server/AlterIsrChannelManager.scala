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

  def clearPending(topicPartition: TopicPartition): Unit

  def startup(): Unit

  def shutdown(): Unit
}

case class AlterIsrItem(topicPartition: TopicPartition, leaderAndIsr: LeaderAndIsr)

class AlterIsrChannelManagerImpl(val controllerChannelManager: BrokerToControllerChannelManager,
                                 val zkClient: KafkaZkClient,
                                 val scheduler: Scheduler,
                                 val brokerId: Int,
                                 val brokerEpoch: Long) extends AlterIsrChannelManager with Logging with KafkaMetricsGroup {

  private val pendingIsrUpdates: mutable.Map[TopicPartition, AlterIsrItem] = new mutable.HashMap[TopicPartition, AlterIsrItem]()
  private val lastIsrChangeMs = new AtomicLong(0)
  private val lastIsrPropagationMs = new AtomicLong(0)

  @volatile private var scheduledRequest: Option[ScheduledFuture[_]] = None

  override def enqueueIsrUpdate(alterIsrItem: AlterIsrItem): Unit = {
    pendingIsrUpdates synchronized {
      pendingIsrUpdates(alterIsrItem.topicPartition) = alterIsrItem
      lastIsrChangeMs.set(System.currentTimeMillis())
      // Rather than sending right away, we'll delay at most 50ms to allow for batching of ISR changes happening
      // in fast succession
      if (scheduledRequest.isEmpty) {
        scheduledRequest = Some(scheduler.schedule("propagate-alter-isr", propagateIsrChanges, 50, -1, TimeUnit.MILLISECONDS))
      }
    }
  }

  override def clearPending(topicPartition: TopicPartition): Unit = {
    pendingIsrUpdates synchronized {
      // when we get a new LeaderAndIsr, we clear out any pending requests
      pendingIsrUpdates.remove(topicPartition)
    }
  }

  override def startup(): Unit = {
    controllerChannelManager.start()
  }

  override def shutdown(): Unit = {
    controllerChannelManager.shutdown()
  }

  private def propagateIsrChanges(): Unit = {
    val now = System.currentTimeMillis()
    pendingIsrUpdates synchronized {
      if (pendingIsrUpdates.nonEmpty) {
        // Max ISRs to send?
        val message = new AlterIsrRequestData()
          .setBrokerId(brokerId)
          .setBrokerEpoch(brokerEpoch)
          .setTopics(new util.ArrayList())

        pendingIsrUpdates.values.groupBy(_.topicPartition.topic()).foreachEntry((topic, items) => {
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
          })
        })

        def responseHandler(response: ClientResponse): Unit = {
          println(response.responseBody().toString(response.requestHeader().apiVersion()))
          val body: AlterIsrResponse = response.responseBody().asInstanceOf[AlterIsrResponse]
          val data: AlterIsrResponseData = body.data()
          Errors.forCode(data.errorCode()) match {
            case Errors.NONE => info(s"Controller handled AlterIsr request")
            case e: Errors => warn(s"Controller returned an error when handling AlterIsr request: $e")
          }
          data.topics().forEach(topic => {
            topic.partitions().forEach(topicPartition => {
              Errors.forCode(topicPartition.errorCode()) match {
                case Errors.NONE => info(s"Controller handled AlterIsr for $topicPartition")
                case e: Errors => warn(s"Controlled had an error handling AlterIsr for $topicPartition: $e")
              }
            })
          })
        }

        info("Sending AlterIsr to controller")
        controllerChannelManager.sendRequest(new AlterIsrRequest.Builder(message), responseHandler)

        pendingIsrUpdates.clear()
        lastIsrPropagationMs.set(now)
      }
      scheduledRequest = None
    }
  }
}
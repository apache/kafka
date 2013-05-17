package kafka.client

import scala.collection._
import kafka.cluster._
import kafka.api._
import kafka.producer._
import kafka.common.KafkaException
import kafka.utils.{Utils, Logging}
import java.util.Properties
import util.Random

/**
 * Helper functions common to clients (producer, consumer, or admin)
 */
object ClientUtils extends Logging{

  /**
   * Used by the producer to send a metadata request since it has access to the ProducerConfig
   * @param topics The topics for which the metadata needs to be fetched
   * @param brokers The brokers in the cluster as configured on the producer through metadata.broker.list
   * @param producerConfig The producer's config
   * @return topic metadata response
   */
  def fetchTopicMetadata(topics: Set[String], brokers: Seq[Broker], producerConfig: ProducerConfig, correlationId: Int): TopicMetadataResponse = {
    var fetchMetaDataSucceeded: Boolean = false
    var i: Int = 0
    val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationId, producerConfig.clientId, topics.toSeq)
    var topicMetadataResponse: TopicMetadataResponse = null
    var t: Throwable = null
    // shuffle the list of brokers before sending metadata requests so that most requests don't get routed to the
    // same broker
    val shuffledBrokers = Random.shuffle(brokers)
    while(i < shuffledBrokers.size && !fetchMetaDataSucceeded) {
      val producer: SyncProducer = ProducerPool.createSyncProducer(producerConfig, shuffledBrokers(i))
      info("Fetching metadata from broker %s with correlation id %d for %d topic(s) %s".format(shuffledBrokers(i), correlationId, topics.size, topics))
      try {
        topicMetadataResponse = producer.send(topicMetadataRequest)
        fetchMetaDataSucceeded = true
      }
      catch {
        case e =>
          warn("Fetching topic metadata with correlation id %d for topics [%s] from broker [%s] failed"
            .format(correlationId, topics, shuffledBrokers(i).toString), e)
          t = e
      } finally {
        i = i + 1
        producer.close()
      }
    }
    if(!fetchMetaDataSucceeded) {
      throw new KafkaException("fetching topic metadata for topics [%s] from broker [%s] failed".format(topics, shuffledBrokers), t)
    } else {
      debug("Successfully fetched metadata for %d topic(s) %s".format(topics.size, topics))
    }
    return topicMetadataResponse
  }

  /**
   * Used by a non-producer client to send a metadata request
   * @param topics The topics for which the metadata needs to be fetched
   * @param brokers The brokers in the cluster as configured on the client
   * @param clientId The client's identifier
   * @return topic metadata response
   */
  def fetchTopicMetadata(topics: Set[String], brokers: Seq[Broker], clientId: String, timeoutMs: Int,
                         correlationId: Int = 0): TopicMetadataResponse = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers.map(_.getConnectionString()).mkString(","))
    props.put("client.id", clientId)
    props.put("request.timeout.ms", timeoutMs.toString)
    val producerConfig = new ProducerConfig(props)
    fetchTopicMetadata(topics, brokers, producerConfig, correlationId)
  }

  /**
   * Parse a list of broker urls in the form host1:port1, host2:port2, ... 
   */
  def parseBrokerList(brokerListStr: String): Seq[Broker] = {
    val brokersStr = Utils.parseCsvList(brokerListStr)

    brokersStr.zipWithIndex.map(b =>{
      val brokerStr = b._1
      val brokerId = b._2
      val brokerInfos = brokerStr.split(":")
      val hostName = brokerInfos(0)
      val port = brokerInfos(1).toInt
      new Broker(brokerId, hostName, port)
    })
  }
  
}
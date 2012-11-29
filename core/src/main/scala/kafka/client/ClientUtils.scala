package kafka.client

import scala.collection._
import kafka.cluster._
import kafka.api._
import kafka.producer._
import kafka.common.KafkaException
import kafka.utils.{Utils, Logging}

/**
 * Helper functions common to clients (producer, consumer, or admin)
 */
object ClientUtils extends Logging{

  def fetchTopicMetadata(topics: Set[String], clientId: String, brokers: Seq[Broker]): TopicMetadataResponse = {
    var fetchMetaDataSucceeded: Boolean = false
    var i: Int = 0
    val topicMetadataRequest = new TopicMetadataRequest(topics.toSeq)
    var topicMetadataResponse: TopicMetadataResponse = null
    var t: Throwable = null
    while(i < brokers.size && !fetchMetaDataSucceeded) {
      val producer: SyncProducer = ProducerPool.createSyncProducer(clientId + "-FetchTopicMetadata", brokers(i))
      info("Fetching metadata for topic %s".format(topics))
      try {
        topicMetadataResponse = producer.send(topicMetadataRequest)
        fetchMetaDataSucceeded = true
      }
      catch {
        case e =>
          warn("fetching topic metadata for topics [%s] from broker [%s] failed".format(topics, brokers(i).toString), e)
          t = e
      } finally {
        i = i + 1
        producer.close()
      }
    }
    if(!fetchMetaDataSucceeded){
      throw new KafkaException("fetching topic metadata for topics [%s] from broker [%s] failed".format(topics, brokers), t)
    }
    return topicMetadataResponse
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
      val creatorId = hostName + "-" + System.currentTimeMillis()
      new Broker(brokerId, creatorId, hostName, port)
    })
  }
  
}
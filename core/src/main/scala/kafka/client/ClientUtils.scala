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
package kafka.client

import org.apache.kafka.common.protocol.Errors

import scala.collection._
import kafka.cluster._
import kafka.api._
import kafka.common.{BrokerEndPointNotAvailableException, KafkaException}
import kafka.utils.{CoreUtils, Logging}

import util.Random
import kafka.network.BlockingChannel
import kafka.utils.ZkUtils
import java.io.IOException

import kafka.consumer.SimpleConsumer
import org.apache.kafka.common.security.auth.SecurityProtocol

 /**
 * Helper functions common to clients (producer, consumer, or admin)
 */
@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
object ClientUtils extends Logging {

   /**
   * Send a metadata request
   * @param topics The topics for which the metadata needs to be fetched
   * @param brokers The brokers in the cluster as configured on the client
   * @param clientId The client's identifier
   * @return topic metadata response
   */
  def fetchTopicMetadata(topics: Set[String], brokers: Seq[BrokerEndPoint], clientId: String, timeoutMs: Int,
                                 correlationId: Int = 0): TopicMetadataResponse = {
    var fetchMetaDataSucceeded: Boolean = false
    var i: Int = 0
    val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationId, clientId,
      topics.toSeq)
    var topicMetadataResponse: TopicMetadataResponse = null
    var t: Throwable = null
    // shuffle the list of brokers before sending metadata requests so that most requests don't get routed to the
    // same broker
    val shuffledBrokers = Random.shuffle(brokers)
    while(i < shuffledBrokers.size && !fetchMetaDataSucceeded) {
      val broker = shuffledBrokers(i)
      val consumer = new SimpleConsumer(broker.host, broker.port, timeoutMs, BlockingChannel.UseDefaultBufferSize,
        clientId)
      info("Fetching metadata from broker %s with correlation id %d for %d topic(s) %s".format(shuffledBrokers(i),
        correlationId, topics.size, topics))
      try {
        topicMetadataResponse = consumer.send(topicMetadataRequest)
        fetchMetaDataSucceeded = true
      }
      catch {
        case e: Throwable =>
          warn("Fetching topic metadata with correlation id %d for topics [%s] from broker [%s] failed"
            .format(correlationId, topics, shuffledBrokers(i).toString), e)
          t = e
      } finally {
        i = i + 1
        consumer.close()
      }
    }
    if (!fetchMetaDataSucceeded) {
      throw new KafkaException("fetching topic metadata for topics [%s] from broker [%s] failed".format(topics, shuffledBrokers), t)
    } else {
      debug("Successfully fetched metadata for %d topic(s) %s".format(topics.size, topics))
    }
    topicMetadataResponse
  }

  /**
   * Parse a list of broker urls in the form host1:port1, host2:port2, ...
   */
  def parseBrokerList(brokerListStr: String): Seq[BrokerEndPoint] = {
    val brokersStr = CoreUtils.parseCsvList(brokerListStr)

    brokersStr.zipWithIndex.map { case (address, brokerId) =>
      BrokerEndPoint.createBrokerEndPoint(brokerId, address)
    }
  }

  /**
   * Creates a blocking channel to a random broker
   */
  def channelToAnyBroker(zkUtils: ZkUtils, socketTimeoutMs: Int = 3000) : BlockingChannel = {
    var channel: BlockingChannel = null
    var connected = false
    while (!connected) {
      val allBrokers = getPlaintextBrokerEndPoints(zkUtils)
      Random.shuffle(allBrokers).find { broker =>
        trace("Connecting to broker %s:%d.".format(broker.host, broker.port))
        try {
          channel = new BlockingChannel(broker.host, broker.port, BlockingChannel.UseDefaultBufferSize, BlockingChannel.UseDefaultBufferSize, socketTimeoutMs)
          channel.connect()
          debug("Created channel to broker %s:%d.".format(channel.host, channel.port))
          true
        } catch {
          case _: Exception =>
            if (channel != null) channel.disconnect()
            channel = null
            info("Error while creating channel to %s:%d.".format(broker.host, broker.port))
            false
        }
      }
      connected = channel != null
    }

    channel
  }

   /**
    * Returns the first end point from each broker with the PLAINTEXT security protocol.
    */
  def getPlaintextBrokerEndPoints(zkUtils: ZkUtils): Seq[BrokerEndPoint] = {
    zkUtils.getAllBrokersInCluster().map { broker =>
      broker.endPoints.collectFirst {
        case endPoint if endPoint.securityProtocol == SecurityProtocol.PLAINTEXT =>
          new BrokerEndPoint(broker.id, endPoint.host, endPoint.port)
      }.getOrElse(throw new BrokerEndPointNotAvailableException(s"End point with security protocol PLAINTEXT not found for broker ${broker.id}"))
    }
  }

   /**
    * Creates a blocking channel to the offset manager of the given group
    */
   def channelToOffsetManager(group: String, zkUtils: ZkUtils, socketTimeoutMs: Int = 3000, retryBackOffMs: Int = 1000) = {
     var queryChannel = channelToAnyBroker(zkUtils)

     var offsetManagerChannelOpt: Option[BlockingChannel] = None

     while (offsetManagerChannelOpt.isEmpty) {

       var coordinatorOpt: Option[BrokerEndPoint] = None

       while (coordinatorOpt.isEmpty) {
         try {
           if (!queryChannel.isConnected)
             queryChannel = channelToAnyBroker(zkUtils)
           debug("Querying %s:%d to locate offset manager for %s.".format(queryChannel.host, queryChannel.port, group))
           queryChannel.send(GroupCoordinatorRequest(group))
           val response = queryChannel.receive()
           val consumerMetadataResponse =  GroupCoordinatorResponse.readFrom(response.payload())
           debug("Consumer metadata response: " + consumerMetadataResponse.toString)
           if (consumerMetadataResponse.error == Errors.NONE)
             coordinatorOpt = consumerMetadataResponse.coordinatorOpt
           else {
             debug("Query to %s:%d to locate offset manager for %s failed - will retry in %d milliseconds."
                  .format(queryChannel.host, queryChannel.port, group, retryBackOffMs))
             Thread.sleep(retryBackOffMs)
           }
         }
         catch {
           case _: IOException =>
             info("Failed to fetch consumer metadata from %s:%d.".format(queryChannel.host, queryChannel.port))
             queryChannel.disconnect()
         }
       }

       val coordinator = coordinatorOpt.get
       if (coordinator.host == queryChannel.host && coordinator.port == queryChannel.port) {
         offsetManagerChannelOpt = Some(queryChannel)
       } else {
         val connectString = "%s:%d".format(coordinator.host, coordinator.port)
         var offsetManagerChannel: BlockingChannel = null
         try {
           debug("Connecting to offset manager %s.".format(connectString))
           offsetManagerChannel = new BlockingChannel(coordinator.host, coordinator.port,
                                                      BlockingChannel.UseDefaultBufferSize,
                                                      BlockingChannel.UseDefaultBufferSize,
                                                      socketTimeoutMs)
           offsetManagerChannel.connect()
           offsetManagerChannelOpt = Some(offsetManagerChannel)
           queryChannel.disconnect()
         }
         catch {
           case _: IOException => // offsets manager may have moved
             info("Error while connecting to %s.".format(connectString))
             if (offsetManagerChannel != null) offsetManagerChannel.disconnect()
             Thread.sleep(retryBackOffMs)
             offsetManagerChannelOpt = None // just in case someone decides to change shutdownChannel to not swallow exceptions
         }
       }
     }

     offsetManagerChannelOpt.get
   }
 }

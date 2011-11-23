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

package kafka.producer

import async._
import java.util.Properties
import kafka.serializer.Encoder
import java.util.concurrent.{ConcurrentMap, ConcurrentHashMap}
import kafka.cluster.{Partition, Broker}
import kafka.api.ProducerRequest
import kafka.common.{UnavailableProducerException, InvalidConfigException}
import kafka.utils.{Utils, Logging}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet}

class ProducerPool[V](private val config: ProducerConfig,
                      private val serializer: Encoder[V],
                      private val syncProducers: ConcurrentMap[Int, SyncProducer],
                      private val asyncProducers: ConcurrentMap[Int, AsyncProducer[V]],
                      private val inputEventHandler: EventHandler[V] = null,
                      private val cbkHandler: CallbackHandler[V] = null) extends Logging {

  private var eventHandler = inputEventHandler
  if(eventHandler == null)
    eventHandler = new DefaultEventHandler(config, cbkHandler)

  if(serializer == null)
    throw new InvalidConfigException("serializer passed in is null!")

  private var sync: Boolean = true
  config.producerType match {
    case "sync" =>
    case "async" => sync = false
    case _ => throw new InvalidConfigException("Valid values for producer.type are sync/async")
  }

  def this(config: ProducerConfig, serializer: Encoder[V],
           eventHandler: EventHandler[V], cbkHandler: CallbackHandler[V]) =
    this(config, serializer,
         new ConcurrentHashMap[Int, SyncProducer](),
         new ConcurrentHashMap[Int, AsyncProducer[V]](),
         eventHandler, cbkHandler)

  def this(config: ProducerConfig, serializer: Encoder[V]) = this(config, serializer,
                                                                  new ConcurrentHashMap[Int, SyncProducer](),
                                                                  new ConcurrentHashMap[Int, AsyncProducer[V]](),
                                                                  Utils.getObject(config.eventHandler),
                                                                  Utils.getObject(config.cbkHandler))
  /**
   * add a new producer, either synchronous or asynchronous, connecting
   * to the specified broker 
   * @param bid the id of the broker
   * @param host the hostname of the broker
   * @param port the port of the broker
   */
  def addProducer(broker: Broker) {
    val props = new Properties()
    props.put("host", broker.host)
    props.put("port", broker.port.toString)
    props.putAll(config.props)
    if(sync) {
        val producer = new SyncProducer(new SyncProducerConfig(props))
        info("Creating sync producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port)
        syncProducers.put(broker.id, producer)
    } else {
        val producer = new AsyncProducer[V](new AsyncProducerConfig(props),
                                            new SyncProducer(new SyncProducerConfig(props)),
                                            serializer,
                                            eventHandler, config.eventHandlerProps,
                                            cbkHandler, config.cbkHandlerProps)
        producer.start
        info("Creating async producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port)
        asyncProducers.put(broker.id, producer)
    }
  }

  /**
   * selects either a synchronous or an asynchronous producer, for
   * the specified broker id and calls the send API on the selected
   * producer to publish the data to the specified broker partition
   * @param poolData the producer pool request object
   */
  def send(poolData: ProducerPoolData[V]*) {
    val distinctBrokers = poolData.map(pd => pd.getBidPid.brokerId).distinct
    var remainingRequests = poolData.toSeq
    distinctBrokers.foreach { bid =>
      val requestsForThisBid = remainingRequests partition (_.getBidPid.brokerId == bid)
      remainingRequests = requestsForThisBid._2

      if(sync) {
        val producerRequests = requestsForThisBid._1.map(req => new ProducerRequest(req.getTopic, req.getBidPid.partId,
          new ByteBufferMessageSet(compressionCodec = config.compressionCodec,
                                   messages = req.getData.map(d => serializer.toMessage(d)): _*)))
        debug("Fetching sync producer for broker id: " + bid)
        val producer = syncProducers.get(bid)
        if(producer != null) {
          if(producerRequests.size > 1)
            producer.multiSend(producerRequests.toArray)
          else
            producer.send(topic = producerRequests(0).topic,
                          partition = producerRequests(0).partition,
                          messages = producerRequests(0).messages)
          config.compressionCodec match {
            case NoCompressionCodec => debug("Sending message to broker " + bid)
            case _ => debug("Sending compressed messages to broker " + bid)
          }
        }else
          throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
            "Sync Producer for broker " + bid + " does not exist in the pool")
      }else {
        debug("Fetching async producer for broker id: " + bid)
        val producer = asyncProducers.get(bid)
        if(producer != null) {
          requestsForThisBid._1.foreach { req =>
            req.getData.foreach(d => producer.send(req.getTopic, d, req.getBidPid.partId))
          }
          if(logger.isDebugEnabled)
            config.compressionCodec match {
              case NoCompressionCodec => debug("Sending message")
              case _ => debug("Sending compressed messages")
            }
        }
        else
          throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
            "Async Producer for broker " + bid + " does not exist in the pool")
      }
    }
  }

  /**
   * Closes all the producers in the pool
   */
  def close() = {
    config.producerType match {
      case "sync" =>
        info("Closing all sync producers")
        val iter = syncProducers.values.iterator
        while(iter.hasNext)
          iter.next.close
      case "async" =>
        info("Closing all async producers")
        val iter = asyncProducers.values.iterator
        while(iter.hasNext)
          iter.next.close
    }
  }

  /**
   * This constructs and returns the request object for the producer pool
   * @param topic the topic to which the data should be published
   * @param bidPid the broker id and partition id
   * @param data the data to be published
   */
  def getProducerPoolData(topic: String, bidPid: Partition, data: Seq[V]): ProducerPoolData[V] = {
    new ProducerPoolData[V](topic, bidPid, data)
  }

  class ProducerPoolData[V](topic: String,
                            bidPid: Partition,
                            data: Seq[V]) {
    def getTopic: String = topic
    def getBidPid: Partition = bidPid
    def getData: Seq[V] = data
  }
}

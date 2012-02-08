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
import kafka.utils._
import kafka.common.InvalidConfigException
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import kafka.serializer.Encoder
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}

class Producer[K,V](config: ProducerConfig,
                    private val eventHandler: EventHandler[K,V]) // for testing only
extends Logging {
  private val hasShutdown = new AtomicBoolean(false)
  if(!Utils.propertyExists(config.zkConnect) && !Utils.propertyExists(config.brokerList))
    throw new InvalidConfigException("At least one of zk.connect or broker.list must be specified")
  if (Utils.propertyExists(config.zkConnect) && Utils.propertyExists(config.brokerList))
    throw new InvalidConfigException("Only one of zk.connect and broker.list should be provided")
  if (config.batchSize > config.queueSize)
    throw new InvalidConfigException("Batch size can't be larger than queue size.")

  private val queue = new LinkedBlockingQueue[ProducerData[K,V]](config.queueSize)
  private var sync: Boolean = true
  private var producerSendThread: ProducerSendThread[K,V] = null
  config.producerType match {
    case "sync" =>
    case "async" =>
      sync = false
      val asyncProducerID = Utils.getNextRandomInt
      producerSendThread = new ProducerSendThread[K,V]("ProducerSendThread-" + asyncProducerID, queue,
        eventHandler, config.queueTime, config.batchSize)
      producerSendThread.start
    case _ => throw new InvalidConfigException("Valid values for producer.type are sync/async")
  }

  /**
   * This constructor can be used when all config parameters will be specified through the
   * ProducerConfig object
   * @param config Producer Configuration object
   */
  def this(config: ProducerConfig) =
    this(config,
         new DefaultEventHandler[K,V](config,
                                      Utils.getObject[Partitioner[K]](config.partitionerClass),
                                      Utils.getObject[Encoder[V]](config.serializerClass),
                                      new ProducerPool(config),
                                      populateProducerPool= true,
                                      brokerPartitionInfo= null))

  /**
   * Sends the data, partitioned by key to the topic using either the
   * synchronous or the asynchronous producer
   * @param producerData the producer data object that encapsulates the topic, key and message data
   */
  def send(producerData: ProducerData[K,V]*) {
    if (hasShutdown.get)
      throw new ProducerClosedException
    recordStats(producerData: _*)
    sync match {
      case true => eventHandler.handle(producerData)
      case false => asyncSend(producerData: _*)
    }
  }

  private def recordStats(producerData: ProducerData[K,V]*) {
    for (data <- producerData)
      ProducerTopicStat.getProducerTopicStat(data.getTopic).recordMessagesPerTopic(data.getData.size)
  }

  private def asyncSend(producerData: ProducerData[K,V]*) {
    for (data <- producerData) {
      val added = config.enqueueTimeoutMs match {
        case 0  =>
          queue.offer(data)
        case _  =>
          try {
            config.enqueueTimeoutMs < 0 match {
            case true =>
              queue.put(data)
              true
            case _ =>
              queue.offer(data, config.enqueueTimeoutMs, TimeUnit.MILLISECONDS)
            }
          }
          catch {
            case e: InterruptedException =>
              false
          }
      }
      if(!added) {
        AsyncProducerStats.recordDroppedEvents
        error("Event queue is full of unsent messages, could not send event: " + data.toString)
        throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + data.toString)
      }else {
        trace("Added to send queue an event: " + data.toString)
        trace("Remaining queue size: " + queue.remainingCapacity)
      }
    }
  }

  /**
   * Close API to close the producer pool connections to all Kafka brokers. Also closes
   * the zookeeper client connection if one exists
   */
  def close() = {
    val canShutdown = hasShutdown.compareAndSet(false, true)
    if(canShutdown) {
      if (producerSendThread != null)
        producerSendThread.shutdown
      eventHandler.close
    }
  }
}

trait ProducerTopicStatMBean {
  def getMessagesPerTopic: Long
}

@threadsafe
class ProducerTopicStat extends ProducerTopicStatMBean {
  private val numCumulatedMessagesPerTopic = new AtomicLong(0)

  def getMessagesPerTopic: Long = numCumulatedMessagesPerTopic.get

  def recordMessagesPerTopic(nMessages: Int) = numCumulatedMessagesPerTopic.getAndAdd(nMessages)
}

object ProducerTopicStat extends Logging {
  private val stats = new Pool[String, ProducerTopicStat]

  def getProducerTopicStat(topic: String): ProducerTopicStat = {
    var stat = stats.get(topic)
    if (stat == null) {
      stat = new ProducerTopicStat
      if (stats.putIfNotExists(topic, stat) == null)
        Utils.registerMBean(stat, "kafka.producer.Producer:type=kafka.ProducerTopicStat." + topic)
      else
        stat = stats.get(topic)
    }
    return stat
  }
}

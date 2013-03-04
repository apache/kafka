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

import async.{DefaultEventHandler, ProducerSendThread, EventHandler}
import kafka.utils._
import java.util.Random
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import kafka.serializer.Encoder
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.QueueFullException
import kafka.metrics._


class Producer[K,V](val config: ProducerConfig,
                    private val eventHandler: EventHandler[K,V])  // only for unit testing
  extends Logging {

  private val hasShutdown = new AtomicBoolean(false)
  private val queue = new LinkedBlockingQueue[KeyedMessage[K,V]](config.queueBufferingMaxMessages)

  private val random = new Random
  private var sync: Boolean = true
  private var producerSendThread: ProducerSendThread[K,V] = null
  config.producerType match {
    case "sync" =>
    case "async" =>
      sync = false
      producerSendThread = new ProducerSendThread[K,V]("ProducerSendThread-" + config.clientId,
                                                       queue,
                                                       eventHandler, 
                                                       config.queueBufferingMaxMs,
                                                       config.batchNumMessages,
                                                       config.clientId)
      producerSendThread.start()
  }

  private val producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId)

  KafkaMetricsReporter.startReporters(config.props)

  def this(config: ProducerConfig) =
    this(config,
         new DefaultEventHandler[K,V](config,
                                      Utils.createObject[Partitioner[K]](config.partitionerClass, config.props),
                                      Utils.createObject[Encoder[V]](config.serializerClass, config.props),
                                      Utils.createObject[Encoder[K]](config.keySerializerClass, config.props),
                                      new ProducerPool(config)))

  /**
   * Sends the data, partitioned by key to the topic using either the
   * synchronous or the asynchronous producer
   * @param messages the producer data object that encapsulates the topic, key and message data
   */
  def send(messages: KeyedMessage[K,V]*) {
    if (hasShutdown.get)
      throw new ProducerClosedException
    recordStats(messages)
    sync match {
      case true => eventHandler.handle(messages)
      case false => asyncSend(messages)
    }
  }

  private def recordStats(messages: Seq[KeyedMessage[K,V]]) {
    for (message <- messages) {
      producerTopicStats.getProducerTopicStats(message.topic).messageRate.mark()
      producerTopicStats.getProducerAllTopicsStats.messageRate.mark()
    }
  }

  private def asyncSend(messages: Seq[KeyedMessage[K,V]]) {
    for (message <- messages) {
      val added = config.queueEnqueueTimeoutMs match {
        case 0  =>
          queue.offer(message)
        case _  =>
          try {
            config.queueEnqueueTimeoutMs < 0 match {
            case true =>
              queue.put(message)
              true
            case _ =>
              queue.offer(message, config.queueEnqueueTimeoutMs, TimeUnit.MILLISECONDS)
            }
          }
          catch {
            case e: InterruptedException =>
              false
          }
      }
      if(!added) {
        producerTopicStats.getProducerTopicStats(message.topic).droppedMessageRate.mark()
        producerTopicStats.getProducerAllTopicsStats.droppedMessageRate.mark()
        error("Event queue is full of unsent messages, could not send event: " + message.toString)
        throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + message.toString)
      }else {
        trace("Added to send queue an event: " + message.toString)
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
      info("Shutting down producer")
      if (producerSendThread != null)
        producerSendThread.shutdown
      eventHandler.close
    }
  }
}



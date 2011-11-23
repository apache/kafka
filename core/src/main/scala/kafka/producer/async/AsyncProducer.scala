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

package kafka.producer.async

import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import kafka.utils.{Utils, Logging}
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.ProducerRequest
import kafka.serializer.Encoder
import java.util.{Random, Properties}
import kafka.producer.{ProducerConfig, SyncProducer}

object AsyncProducer {
  val Shutdown = new Object
  val Random = new Random
  val ProducerMBeanName = "kafka.producer.Producer:type=AsyncProducerStats"
  val ProducerQueueSizeMBeanName = "kafka.producer.Producer:type=AsyncProducerQueueSizeStats"
}

private[kafka] class AsyncProducer[T](config: AsyncProducerConfig,
                                      producer: SyncProducer,
                                      serializer: Encoder[T],
                                      eventHandler: EventHandler[T] = null,
                                      eventHandlerProps: Properties = null,
                                      cbkHandler: CallbackHandler[T] = null,
                                      cbkHandlerProps: Properties = null) extends Logging {
  private val closed = new AtomicBoolean(false)
  private val queue = new LinkedBlockingQueue[QueueItem[T]](config.queueSize)
  // initialize the callback handlers
  if(eventHandler != null)
    eventHandler.init(eventHandlerProps)
  if(cbkHandler != null)
    cbkHandler.init(cbkHandlerProps)
  private val asyncProducerID = AsyncProducer.Random.nextInt
  private val sendThread = new ProducerSendThread("ProducerSendThread-" + asyncProducerID, queue,
    serializer, producer,
    if(eventHandler != null) eventHandler else new DefaultEventHandler[T](new ProducerConfig(config.props), cbkHandler),
    cbkHandler, config.queueTime, config.batchSize, AsyncProducer.Shutdown)
  sendThread.setDaemon(false)
  Utils.swallow(logger.warn, Utils.registerMBean(
    new AsyncProducerQueueSizeStats[T](queue), AsyncProducer.ProducerQueueSizeMBeanName + "-" + asyncProducerID))

  def this(config: AsyncProducerConfig) {
    this(config,
      new SyncProducer(config),
      Utils.getObject(config.serializerClass),
      Utils.getObject(config.eventHandler),
      config.eventHandlerProps,
      Utils.getObject(config.cbkHandler),
      config.cbkHandlerProps)
  }

  def start = sendThread.start

  def send(topic: String, event: T) { send(topic, event, ProducerRequest.RandomPartition) }

  def send(topic: String, event: T, partition:Int) {
    AsyncProducerStats.recordEvent

    if(closed.get)
      throw new QueueClosedException("Attempt to add event to a closed queue.")

    var data = new QueueItem(event, topic, partition)
    if(cbkHandler != null)
      data = cbkHandler.beforeEnqueue(data)

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
            val msg = "%s interrupted during enqueue of event %s.".format(
              getClass.getSimpleName, event.toString)
            error(msg)
            throw new AsyncProducerInterruptedException(msg)
        }
    }

    if(cbkHandler != null)
      cbkHandler.afterEnqueue(data, added)

    if(!added) {
      AsyncProducerStats.recordDroppedEvents
      logger.error("Event queue is full of unsent messages, could not send event: " + event.toString)
      throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + event.toString)
    }else {
      if(logger.isTraceEnabled) {
        logger.trace("Added event to send queue for topic: " + topic + ", partition: " + partition + ":" + event.toString)
        logger.trace("Remaining queue size: " + queue.remainingCapacity)
      }
    }
  }

  def close = {
    if(cbkHandler != null) {
      cbkHandler.close
      logger.info("Closed the callback handler")
    }
    closed.set(true)
    queue.put(new QueueItem(AsyncProducer.Shutdown.asInstanceOf[T], null, -1))
    if(logger.isDebugEnabled)
      logger.debug("Added shutdown command to the queue")
    sendThread.shutdown
    sendThread.awaitShutdown
    producer.close
    logger.info("Closed AsyncProducer")
  }

  // for testing only
  import org.apache.log4j.Level
  def setLoggerLevel(level: Level) = logger.setLevel(level)
}

class QueueItem[T](data: T, topic: String, partition: Int) {
  def getData: T = data
  def getPartition: Int = partition
  def getTopic:String = topic
  override def toString = "topic: " + topic + ", partition: " + partition + ", data: " + data.toString
}

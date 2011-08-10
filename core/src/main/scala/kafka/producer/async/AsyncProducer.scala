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
import kafka.utils.Utils
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.log4j.{Level, Logger}
import kafka.api.ProducerRequest
import kafka.serializer.Encoder
import java.lang.management.ManagementFactory
import javax.management.ObjectName
import java.util.{Random, Properties}
import kafka.producer.{ProducerConfig, SyncProducer}

object AsyncProducer {
  val Shutdown = new Object
  val Random = new Random
  val ProducerMBeanName = "kafka.producer.Producer:type=AsyncProducerStats"
}

private[kafka] class AsyncProducer[T](config: AsyncProducerConfig,
                                      producer: SyncProducer,
                                      serializer: Encoder[T],
                                      eventHandler: EventHandler[T] = null,
                                      eventHandlerProps: Properties = null,
                                      cbkHandler: CallbackHandler[T] = null,
                                      cbkHandlerProps: Properties = null) {
  private val logger = Logger.getLogger(classOf[AsyncProducer[T]])
  private val closed = new AtomicBoolean(false)
  private val queue = new LinkedBlockingQueue[QueueItem[T]](config.queueSize)
  // initialize the callback handlers
  if(eventHandler != null)
    eventHandler.init(eventHandlerProps)
  if(cbkHandler != null)
    cbkHandler.init(cbkHandlerProps)
  private val sendThread = new ProducerSendThread("ProducerSendThread-" + AsyncProducer.Random.nextInt, queue,
    serializer, producer,
    if(eventHandler != null) eventHandler else new DefaultEventHandler[T](new ProducerConfig(config.props), cbkHandler),
    cbkHandler, config.queueTime, config.batchSize, AsyncProducer.Shutdown)
  sendThread.setDaemon(false)

  val asyncProducerStats = new AsyncProducerStats[T](queue)
  val mbs = ManagementFactory.getPlatformMBeanServer
  try {
    val objName = new ObjectName(AsyncProducer.ProducerMBeanName)
    if(mbs.isRegistered(objName))
      mbs.unregisterMBean(objName)
    mbs.registerMBean(asyncProducerStats, objName)
  }catch {
    case e: Exception => logger.warn("can't register AsyncProducerStats")
  }

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
    asyncProducerStats.recordEvent

    if(closed.get)
      throw new QueueClosedException("Attempt to add event to a closed queue.")

    var data = new QueueItem(event, topic, partition)
    if(cbkHandler != null)
      data = cbkHandler.beforeEnqueue(data)

    val added = if (config.enqueueTimeoutMs != 0) {
      try {
        if (config.enqueueTimeoutMs < 0) {
          queue.put(data)
          true
        }
        else {
          queue.offer(data, config.enqueueTimeoutMs, TimeUnit.MILLISECONDS)
        }
      }
      catch {
        case e: InterruptedException =>
          val msg = "%s interrupted during enqueue of event %s.".format(
            getClass.getSimpleName, event.toString)
          logger.error(msg)
          throw new AsyncProducerInterruptedException(msg)
      }
    }
    else {
      queue.offer(data)
    }

    if(cbkHandler != null)
      cbkHandler.afterEnqueue(data, added)

    if(!added) {
      asyncProducerStats.recordDroppedEvents
      logger.error("Event queue is full of unsent messages, could not send event: " + event.toString)
      throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + event.toString)
    }else {
      if(logger.isTraceEnabled) {
        logger.trace("Added event to send queue for topic: " + topic + ":" + event.toString)
        logger.trace("Remaining queue size: " + queue.remainingCapacity)
      }
    }
  }

  def close = {
    if(cbkHandler != null) {
      cbkHandler.close
      logger.info("Closed the callback handler")
    }
    queue.put(new QueueItem(AsyncProducer.Shutdown.asInstanceOf[T], null, -1))
    sendThread.shutdown
    sendThread.awaitShutdown
    producer.close
    closed.set(true)
    logger.info("Closed AsyncProducer")
  }

  // for testing only
  def setLoggerLevel(level: Level) = logger.setLevel(level)
}

class QueueItem[T](data: T, topic: String, partition: Int) {
  def getData: T = data
  def getPartition: Int = partition
  def getTopic:String = topic
  override def toString = "topic: " + topic + ", partition: " + partition + ", data: " + data.toString
}

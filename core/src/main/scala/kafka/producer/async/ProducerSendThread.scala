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

import kafka.utils.Logging
import java.util.concurrent.{BlockingQueue, CountDownLatch, TimeUnit}

import collection.mutable.ArrayBuffer
import kafka.producer.KeyedMessage
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.Time

@deprecated("This class has been deprecated and will be removed in a future release.", "0.10.0.0")
class ProducerSendThread[K,V](val threadName: String,
                              val queue: BlockingQueue[KeyedMessage[K,V]],
                              val handler: EventHandler[K,V],
                              val queueTime: Long,
                              val batchSize: Int,
                              val clientId: String) extends Thread(threadName) with Logging with KafkaMetricsGroup {

  private val shutdownLatch = new CountDownLatch(1)
  private val shutdownCommand = new KeyedMessage[K,V]("shutdown", null.asInstanceOf[K], null.asInstanceOf[V])

  newGauge("ProducerQueueSize",
          new Gauge[Int] {
            def value = queue.size
          },
          Map("clientId" -> clientId))

  override def run {
    try {
      processEvents
    }catch {
      case e: Throwable => error("Error in sending events: ", e)
    }finally {
      shutdownLatch.countDown
    }
  }

  def shutdown(): Unit = {
    info("Begin shutting down ProducerSendThread")
    queue.put(shutdownCommand)
    shutdownLatch.await
    info("Shutdown ProducerSendThread complete")
  }

  private def processEvents() {
    var lastSend = Time.SYSTEM.milliseconds
    var events = new ArrayBuffer[KeyedMessage[K,V]]
    var full: Boolean = false

    // drain the queue until you get a shutdown command
    Iterator.continually(queue.poll(scala.math.max(0, (lastSend + queueTime) - Time.SYSTEM.milliseconds), TimeUnit.MILLISECONDS))
                      .takeWhile(item => if(item != null) item ne shutdownCommand else true).foreach {
      currentQueueItem =>
        val elapsed = Time.SYSTEM.milliseconds - lastSend
        // check if the queue time is reached. This happens when the poll method above returns after a timeout and
        // returns a null object
        val expired = currentQueueItem == null
        if(currentQueueItem != null) {
          trace("Dequeued item for topic %s, partition key: %s, data: %s"
              .format(currentQueueItem.topic, currentQueueItem.key, currentQueueItem.message))
          events += currentQueueItem
        }

        // check if the batch size is reached
        full = events.size >= batchSize

        if(full || expired) {
          if(expired)
            debug(elapsed + " ms elapsed. Queue time reached. Sending..")
          if(full)
            debug("Batch full. Sending..")
          // if either queue time has reached or batch size has reached, dispatch to event handler
          tryToHandle(events)
          lastSend = Time.SYSTEM.milliseconds
          events = new ArrayBuffer[KeyedMessage[K,V]]
        }
    }
    // send the last batch of events
    tryToHandle(events)
    if(queue.size > 0)
      throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, %d remaining items in the queue"
        .format(queue.size))
  }

  def tryToHandle(events: Seq[KeyedMessage[K,V]]) {
    val size = events.size
    try {
      debug("Handling " + size + " events")
      if(size > 0)
        handler.handle(events)
    }catch {
      case e: Throwable => error("Error in handling batch of " + size + " events", e)
    }
  }

}

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

import kafka.utils.{SystemTime, Logging}
import java.util.concurrent.{TimeUnit, CountDownLatch, BlockingQueue}
import collection.mutable.ListBuffer
import kafka.producer.ProducerData
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge

class ProducerSendThread[K,V](val threadName: String,
                              val queue: BlockingQueue[ProducerData[K,V]],
                              val handler: EventHandler[K,V],
                              val queueTime: Long,
                              val batchSize: Int) extends Thread(threadName) with Logging with KafkaMetricsGroup {

  private val shutdownLatch = new CountDownLatch(1)
  private val shutdownCommand = new ProducerData[K,V](null, null.asInstanceOf[K], null.asInstanceOf[Seq[V]])

  newGauge(
    "ProducerQueueSize-" + getId,
    new Gauge[Int] {
      def value() = queue.size
    }
  )

  override def run {

    try {
      processEvents
    }catch {
      case e => error("Error in sending events: ", e)
    }finally {
      shutdownLatch.countDown
    }
  }

  def shutdown = {
    info("Beging shutting down ProducerSendThread")
    queue.put(shutdownCommand)
    shutdownLatch.await
    info("Shutdown ProducerSendThread complete")
  }

  private def processEvents() {
    var lastSend = SystemTime.milliseconds
    var events = new ListBuffer[ProducerData[K,V]]
    var full: Boolean = false

    // drain the queue until you get a shutdown command
    Stream.continually(queue.poll(scala.math.max(0, (lastSend + queueTime) - SystemTime.milliseconds), TimeUnit.MILLISECONDS))
                      .takeWhile(item => if(item != null) item ne shutdownCommand else true).foreach {
      currentQueueItem =>
        val elapsed = (SystemTime.milliseconds - lastSend)
        // check if the queue time is reached. This happens when the poll method above returns after a timeout and
        // returns a null object
        val expired = currentQueueItem == null
        if(currentQueueItem != null) {
          if(currentQueueItem.getKey == null)
            trace("Dequeued item for topic %s, no partition key, data: %s"
              .format(currentQueueItem.getTopic, currentQueueItem.getData.toString))
          else
            trace("Dequeued item for topic %s, partition key: %s, data: %s"
              .format(currentQueueItem.getTopic, currentQueueItem.getKey.toString, currentQueueItem.getData.toString))
          events += currentQueueItem
        }

        // check if the batch size is reached
        full = events.size >= batchSize

        if(full || expired) {
          if(expired) debug(elapsed + " ms elapsed. Queue time reached. Sending..")
          if(full) debug("Batch full. Sending..")
          // if either queue time has reached or batch size has reached, dispatch to event handler
          tryToHandle(events)
          lastSend = SystemTime.milliseconds
          events = new ListBuffer[ProducerData[K,V]]
        }
    }
    if(queue.size > 0)
      throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, %d remaining items in the queue"
        .format(queue.size))
  }

  def tryToHandle(events: Seq[ProducerData[K,V]]) {
    val size = events.size
    try {
      debug("Handling " + size + " events")
      if(size > 0)
        handler.handle(events)
    }catch {
      case e => error("Error in handling batch of " + size + " events", e)
    }
  }

}

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
import kafka.serializer.Encoder
import kafka.producer.SyncProducer

private[async] class ProducerSendThread[T](val threadName: String,
                                           val queue: BlockingQueue[QueueItem[T]],
                                           val serializer: Encoder[T],
                                           val underlyingProducer: SyncProducer,
                                           val handler: EventHandler[T],
                                           val cbkHandler: CallbackHandler[T],
                                           val queueTime: Long,
                                           val batchSize: Int,
                                           val shutdownCommand: Any) extends Thread(threadName) with Logging {

  private val shutdownLatch = new CountDownLatch(1)

  override def run {

    try {
      val remainingEvents = processEvents
      debug("Remaining events = " + remainingEvents.size)

      // handle remaining events
      if(remainingEvents.size > 0) {
        debug("Dispatching last batch of %d events to the event handler".format(remainingEvents.size))
        tryToHandle(remainingEvents)
      }
    }catch {
      case e => error("Error in sending events: ", e)
    }finally {
      shutdownLatch.countDown
    }
  }

  def awaitShutdown = shutdownLatch.await

  def shutdown = {
    handler.close
    info("Shutdown thread complete")
  }

  private def processEvents(): Seq[QueueItem[T]] = {
    var lastSend = SystemTime.milliseconds
    var events = new ListBuffer[QueueItem[T]]
    var full: Boolean = false

    // drain the queue until you get a shutdown command
    Stream.continually(queue.poll(scala.math.max(0, (lastSend + queueTime) - SystemTime.milliseconds), TimeUnit.MILLISECONDS))
                      .takeWhile(item => if(item != null) item.getData != shutdownCommand else true).foreach {
      currentQueueItem =>
        val elapsed = (SystemTime.milliseconds - lastSend)
        // check if the queue time is reached. This happens when the poll method above returns after a timeout and
        // returns a null object
        val expired = currentQueueItem == null
        if(currentQueueItem != null)
          trace("Dequeued item for topic %s and partition %d"
              .format(currentQueueItem.getTopic, currentQueueItem.getPartition))

        // handle the dequeued current item
        if(cbkHandler != null)
          events = events ++ cbkHandler.afterDequeuingExistingData(currentQueueItem)
        else {
          if (currentQueueItem != null)
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
          events = new ListBuffer[QueueItem[T]]
        }
    }
    if(queue.size > 0)
      throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, %d remaining items in the queue"
        .format(queue.size))
    if(cbkHandler != null) {
      info("Invoking the callback handler before handling the last batch of %d events".format(events.size))
      val addedEvents = cbkHandler.lastBatchBeforeClose
      logEvents("last batch before close", addedEvents)
      events = events ++ addedEvents
    }
    events
  }

  def tryToHandle(events: Seq[QueueItem[T]]) {
    try {
      debug("Handling " + events.size + " events")
      if(events.size > 0)
        handler.handle(events, underlyingProducer, serializer)
    }catch {
      case e => error("Error in handling batch of " + events.size + " events", e)
    }
  }

  private def logEvents(tag: String, events: Iterable[QueueItem[T]]) {
    if(logger.isTraceEnabled) {
      trace("events for " + tag + ":")
      for (event <- events)
        trace(event.getData.toString)
    }
  }
}

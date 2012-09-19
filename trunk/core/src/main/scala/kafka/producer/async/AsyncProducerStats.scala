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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.BlockingQueue
import org.apache.log4j.Logger
import kafka.utils.Utils

class AsyncProducerStats extends AsyncProducerStatsMBean {
  val droppedEvents = new AtomicInteger(0)
  val numEvents = new AtomicInteger(0)

  def getAsyncProducerEvents: Int = numEvents.get

  def getAsyncProducerDroppedEvents: Int = droppedEvents.get

  def recordDroppedEvents = droppedEvents.getAndAdd(1)

  def recordEvent = numEvents.getAndAdd(1)
}

class AsyncProducerQueueSizeStats[T](private val queue: BlockingQueue[QueueItem[T]]) extends AsyncProducerQueueSizeStatsMBean {
  def getAsyncProducerQueueSize: Int = queue.size
}

object AsyncProducerStats {
  private val logger = Logger.getLogger(getClass())
  private val stats = new AsyncProducerStats
  Utils.registerMBean(stats, AsyncProducer.ProducerMBeanName)

  def recordDroppedEvents = stats.recordDroppedEvents

  def recordEvent = stats.recordEvent
}

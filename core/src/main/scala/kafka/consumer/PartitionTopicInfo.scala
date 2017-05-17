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

package kafka.consumer

import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.message._
import kafka.utils.Logging

class PartitionTopicInfo(val topic: String,
                         val partitionId: Int,
                         private val chunkQueue: BlockingQueue[FetchedDataChunk],
                         private val consumedOffset: AtomicLong,
                         private val fetchedOffset: AtomicLong,
                         private val fetchSize: AtomicInteger,
                         private val clientId: String) extends Logging {

  debug("initial consumer offset of " + this + " is " + consumedOffset.get)
  debug("initial fetch offset of " + this + " is " + fetchedOffset.get)

  private val consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId)

  def getConsumeOffset() = consumedOffset.get

  def getFetchOffset() = fetchedOffset.get

  def resetConsumeOffset(newConsumeOffset: Long) = {
    consumedOffset.set(newConsumeOffset)
    debug("reset consume offset of " + this + " to " + newConsumeOffset)
  }

  def resetFetchOffset(newFetchOffset: Long) = {
    fetchedOffset.set(newFetchOffset)
    debug("reset fetch offset of ( %s ) to %d".format(this, newFetchOffset))
  }

  /**
   * Enqueue a message set for processing.
   */
  def enqueue(messages: ByteBufferMessageSet) {
    val size = messages.validBytes
    if(size > 0) {
      val next = messages.shallowIterator.toSeq.last.nextOffset
      trace("Updating fetch offset = " + fetchedOffset.get + " to " + next)
      chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get))
      fetchedOffset.set(next)
      debug("updated fetch offset of (%s) to %d".format(this, next))
      consumerTopicStats.getConsumerTopicStats(topic).byteRate.mark(size)
      consumerTopicStats.getConsumerAllTopicStats().byteRate.mark(size)
    } else if(messages.sizeInBytes > 0) {
      chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get))
    }
  }
  
  override def toString: String = topic + ":" + partitionId.toString + ": fetched offset = " + fetchedOffset.get +
    ": consumed offset = " + consumedOffset.get
}

object PartitionTopicInfo {
  val InvalidOffset = -1L

  def isOffsetInvalid(offset: Long) = offset < 0L
}
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

package kafka.log

import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._

import kafka.utils._
import kafka.message._
import kafka.common.KafkaException
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}

/**
 * An on-disk message set. The set can be opened either mutably or immutably. Mutation attempts
 * will fail on an immutable message set. An optional limit and start position can be applied to the message set
 * which will control the position in the file at which the set begins
 */
@nonthreadsafe
class FileMessageSet private[kafka](val file: File,
                                    private[log] val channel: FileChannel,
                                    private[log] val start: Long, // the starting position in the file
                                    private[log] val limit: Long, // the length (may be less than the file length)
                                    val mutable: Boolean) extends MessageSet with Logging {
  
  private val setSize = new AtomicLong()

  if(mutable) {
    if(limit < Long.MaxValue || start > 0)
      throw new KafkaException("Attempt to open a mutable message set with a view or offset, which is not allowed.")

    setSize.set(channel.size())
    channel.position(channel.size)
  } else {
    setSize.set(scala.math.min(channel.size(), limit) - start)
  }
  
  /**
   * Create a file message set with no limit or offset
   */
  def this(file: File, channel: FileChannel, mutable: Boolean) = 
    this(file, channel, 0, Long.MaxValue, mutable)
  
  /**
   * Create a file message set with no limit or offset
   */
  def this(file: File, mutable: Boolean) = 
    this(file, Utils.openChannel(file, mutable), mutable)
  
  /**
   * Return a message set which is a view into this set starting from the given position and with the given size limit.
   */
  def read(position: Long, size: Long): FileMessageSet = {
    new FileMessageSet(file, channel, this.start + position, scala.math.min(this.start + position + size, sizeInBytes()),
      false)
  }
  
  /**
   * Search forward for the file position of the last offset that is great than or equal to the target offset 
   * and return its physical position. If no such offsets are found, return null.
   */
  private[log] def searchFor(targetOffset: Long, startingPosition: Int): OffsetPosition = {
    var position = startingPosition
    val buffer = ByteBuffer.allocate(12)
    val size = setSize.get()
    while(position + 12 < size) {
      buffer.rewind()
      channel.read(buffer, position)
      if(buffer.hasRemaining)
        throw new IllegalStateException("Failed to read complete buffer.")
      buffer.rewind()
      val offset = buffer.getLong()
      if(offset >= targetOffset)
        return OffsetPosition(offset, position)
      val messageSize = buffer.getInt()
      position += MessageSet.LogOverhead + messageSize
    }
    null
  }
  
  /**
   * Write some of this set to the given channel, return the amount written
   */
  def writeTo(destChannel: GatheringByteChannel, writePosition: Long, size: Long): Long = 
    channel.transferTo(start + writePosition, scala.math.min(size, sizeInBytes), destChannel)
  
  /**
   * Get an iterator over the messages in the set
   */
  override def iterator: Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var location = start
      
      override def makeNext(): MessageAndOffset = {
        // read the size of the item
        val sizeOffsetBuffer = ByteBuffer.allocate(12)
        channel.read(sizeOffsetBuffer, location)
        if(sizeOffsetBuffer.hasRemaining)
          return allDone()
        
        sizeOffsetBuffer.rewind()
        val offset = sizeOffsetBuffer.getLong()
        val size = sizeOffsetBuffer.getInt()
        if (size < Message.MinHeaderSize)
          return allDone()
        
        // read the item itself
        val buffer = ByteBuffer.allocate(size)
        channel.read(buffer, location + 12)
        if(buffer.hasRemaining)
          return allDone()
        buffer.rewind()
        
        // increment the location and return the item
        location += size + 12
        new MessageAndOffset(new Message(buffer), offset)
      }
    }
  }
  
  /**
   * The number of bytes taken up by this file set
   */
  def sizeInBytes(): Long = setSize.get()

  def checkMutable(): Unit = {
    if(!mutable)
      throw new KafkaException("Attempt to invoke mutation on immutable message set.")
  }
  
  /**
   * Append this message to the message set
   */
  def append(messages: MessageSet): Unit = {
    checkMutable()
    var written = 0L
    while(written < messages.sizeInBytes)
      written += messages.writeTo(channel, 0, messages.sizeInBytes)
    setSize.getAndAdd(written)
  }
 
  /**
   * Commit all written data to the physical disk
   */
  def flush() = {
    checkMutable()
    LogFlushStats.logFlushTimer.time {
      channel.force(true)
    }
  }
  
  /**
   * Close this message set
   */
  def close() {
    if(mutable)
      flush()
    channel.close()
  }
  
  /**
   * Delete this message set from the filesystem
   */
  def delete(): Boolean = {
    Utils.swallow(channel.close())
    file.delete()
  }

  /**
   * Truncate this file message set to the given size. Note that this API does no checking that the 
   * given size falls on a valid byte offset.
   */
  def truncateTo(targetSize: Long) = {
    checkMutable()
    if(targetSize > sizeInBytes())
      throw new KafkaException("Attempt to truncate log segment to %d bytes failed since the current ".format(targetSize) +
        " size of this log segment is only %d bytes".format(sizeInBytes()))
    channel.truncate(targetSize)
    channel.position(targetSize)
    setSize.set(targetSize)
  }
  
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}

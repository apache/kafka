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

package kafka.message

import java.nio._
import java.nio.channels._

/**
 * Message set helper functions
 */
object MessageSet {

  val MessageSizeLength = 4
  val OffsetLength = 8
  val LogOverhead = MessageSizeLength + OffsetLength
  val Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0))
  
  /**
   * The size of a message set containing the given messages
   */
  def messageSetSize(messages: Iterable[Message]): Int =
    messages.foldLeft(0)(_ + entrySize(_))

  /**
   * The size of a size-delimited entry in a message set
   */
  def entrySize(message: Message): Int = LogOverhead + message.size

  /**
   * Validate that all "magic" values in `messages` are the same and return their magic value and max timestamp
   */
  def magicAndLargestTimestamp(messages: Seq[Message]): MagicAndTimestamp = {
    val firstMagicValue = messages.head.magic
    var largestTimestamp = Message.NoTimestamp
    for (message <- messages) {
      if (message.magic != firstMagicValue)
        throw new IllegalStateException("Messages in the same message set must have same magic value")
      if (firstMagicValue > Message.MagicValue_V0)
        largestTimestamp = math.max(largestTimestamp, message.timestamp)
    }
    MagicAndTimestamp(firstMagicValue, largestTimestamp)
  }

}

case class MagicAndTimestamp(magic: Byte, timestamp: Long)

/**
 * A set of messages with offsets. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. The format of each message is
 * as follows:
 * 8 byte message offset number
 * 4 byte size containing an integer N
 * N message bytes as described in the Message class
 */
abstract class MessageSet extends Iterable[MessageAndOffset] {

  /** Write the messages in this set to the given channel starting at the given offset byte. 
    * Less than the complete amount may be written, but no more than maxSize can be. The number
    * of bytes written is returned */
  def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Int): Int

  /**
   * Check if all the wrapper messages in the message set have the expected magic value
   */
  def isMagicValueInAllWrapperMessages(expectedMagicValue: Byte): Boolean

  /**
   * Provides an iterator over the message/offset pairs in this set
   */
  def iterator: Iterator[MessageAndOffset]
  
  /**
   * Gives the total size of this message set in bytes
   */
  def sizeInBytes: Int

  /**
   * Print this message set's contents. If the message set has more than 100 messages, just
   * print the first 100.
   */
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(getClass.getSimpleName + "(")
    val iter = this.iterator
    var i = 0
    while(iter.hasNext && i < 100) {
      val message = iter.next
      builder.append(message)
      if(iter.hasNext)
        builder.append(", ")
      i += 1
    }
    if(iter.hasNext)
      builder.append("...")
    builder.append(")")
    builder.toString
  }

}

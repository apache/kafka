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
  
  val LogOverhead = 4
  val Empty: MessageSet = new ByteBufferMessageSet(ByteBuffer.allocate(0))
  
  /**
   * The size of a message set containing the given messages
   */
  def messageSetSize(messages: Iterable[Message]): Int =
    messages.foldLeft(0)(_ + entrySize(_))

  /**
   * The size of a list of messages
   */
  def messageSetSize(messages: java.util.List[Message]): Int = {
    var size = 0
    val iter = messages.iterator
    while(iter.hasNext) {
      val message = iter.next.asInstanceOf[Message]
      size += entrySize(message)
    }
    size
  }
  
  /**
   * The size of a size-delimited entry in a message set
   */
  def entrySize(message: Message): Int = LogOverhead + message.size

  def createByteBuffer(compressionCodec: CompressionCodec, messages: Message*): ByteBuffer =
    compressionCodec match {
      case NoCompressionCodec =>
        val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
        for (message <- messages) {
          message.serializeTo(buffer)
        }
        buffer.rewind
        buffer
      case _ =>
        messages.size match {
          case 0 =>
            val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
            buffer.rewind
            buffer
          case _ =>
            val message = CompressionUtils.compress(messages, compressionCodec)
            val buffer = ByteBuffer.allocate(message.serializedSize)
            message.serializeTo(buffer)
            buffer.rewind
            buffer
        }
    }
}

/**
 * A set of messages. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. A The format of each message is
 * as follows:
 * 4 byte size containing an integer N
 * N message bytes as described in the message class
 */
abstract class MessageSet extends Iterable[MessageAndOffset] {

  /** Write the messages in this set to the given channel starting at the given offset byte. 
    * Less than the complete amount may be written, but no more than maxSize can be. The number
    * of bytes written is returned */
  def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Long): Long
  
  /**
   * Provides an iterator over the messages in this set
   */
  def iterator: Iterator[MessageAndOffset]
  
  /**
   * Gives the total size of this message set in bytes
   */
  def sizeInBytes: Long
  
  /**
   * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
   * match the payload for any message.
   */
  def validate(): Unit = {
    for(messageAndOffset <- this)
      if(!messageAndOffset.message.isValid)
        throw new InvalidMessageException
  }
  
}

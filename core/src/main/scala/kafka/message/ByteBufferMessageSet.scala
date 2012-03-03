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

import kafka.utils.Logging
import kafka.common.{InvalidMessageSizeException, ErrorMapping}
import java.nio.ByteBuffer
import java.nio.channels._
import kafka.utils.IteratorTemplate

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 * 
 */
class ByteBufferMessageSet(private val buffer: ByteBuffer,
                           private val initialOffset: Long = 0L,
                           private val errorCode: Int = ErrorMapping.NoError) extends MessageSet with Logging {
  private var validByteCount = -1L
  private var shallowValidByteCount = -1L

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(MessageSet.createByteBuffer(compressionCodec, messages:_*), 0L, ErrorMapping.NoError)
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, messages: _*)
  }

  def getInitialOffset = initialOffset

  def getBuffer = buffer

  def getErrorCode = errorCode

  def getSerialized(): ByteBuffer = buffer

  def validBytes: Long = shallowValidBytes

  private def shallowValidBytes: Long = {
    if(shallowValidByteCount < 0) {
      val iter = deepIterator
      while(iter.hasNext) {
        val messageAndOffset = iter.next
        shallowValidByteCount = messageAndOffset.offset
      }
    }
    if(shallowValidByteCount < initialOffset) 0
    else (shallowValidByteCount - initialOffset)
  }
  
  /** Write the messages in this set to the given channel */
  def writeTo(channel: GatheringByteChannel, offset: Long, size: Long): Long =
    channel.write(buffer.duplicate)
  
  override def iterator: Iterator[MessageAndOffset] = deepIterator

  private def deepIterator(): Iterator[MessageAndOffset] = {
    ErrorMapping.maybeThrowException(errorCode)
    new IteratorTemplate[MessageAndOffset] {
      var topIter = buffer.slice()
      var currValidBytes = initialOffset
      var innerIter:Iterator[MessageAndOffset] = null
      var lastMessageSize = 0L

      def innerDone():Boolean = (innerIter==null || !innerIter.hasNext)

      def makeNextOuter: MessageAndOffset = {
        if (topIter.remaining < 4) {
          return allDone()
        }
        val size = topIter.getInt()
        lastMessageSize = size

        trace("Remaining bytes in iterator = " + topIter.remaining)
        trace("size of data = " + size)

        if(size < 0 || topIter.remaining < size) {
          if (currValidBytes == initialOffset || size < 0)
            throw new InvalidMessageSizeException("invalid message size: " + size + " only received bytes: " +
              topIter.remaining + " at " + currValidBytes + "( possible causes (1) a single message larger than " +
              "the fetch size; (2) log corruption )")
          return allDone()
        }
        val message = topIter.slice()
        message.limit(size)
        topIter.position(topIter.position + size)
        val newMessage = new Message(message)
        newMessage.compressionCodec match {
          case NoCompressionCodec =>
            debug("Message is uncompressed. Valid byte count = %d".format(currValidBytes))
            innerIter = null
            currValidBytes += 4 + size
            trace("currValidBytes = " + currValidBytes)
            new MessageAndOffset(newMessage, currValidBytes)
          case _ =>
            debug("Message is compressed. Valid byte count = %d".format(currValidBytes))
            innerIter = CompressionUtils.decompress(newMessage).deepIterator
            if (!innerIter.hasNext) {
              currValidBytes += 4 + lastMessageSize
              innerIter = null
            }
            makeNext()
        }
      }

      override def makeNext(): MessageAndOffset = {
        val isInnerDone = innerDone()
        isInnerDone match {
          case true => makeNextOuter
          case false => {
            val messageAndOffset = innerIter.next
            if (!innerIter.hasNext)
              currValidBytes += 4 + lastMessageSize
            new MessageAndOffset(messageAndOffset.message, currValidBytes)
          }
        }
      }
    }
  }

  def sizeInBytes: Long = buffer.limit
  
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append("ByteBufferMessageSet(")
    for(message <- this) {
      builder.append(message)
      builder.append(", ")
    }
    builder.append(")")
    builder.toString
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        (that canEqual this) && errorCode == that.errorCode && buffer.equals(that.buffer) && initialOffset == that.initialOffset
      case _ => false
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ByteBufferMessageSet]

  override def hashCode: Int = 31 + (17 * errorCode) + buffer.hashCode + initialOffset.hashCode
}

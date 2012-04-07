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
import java.nio.ByteBuffer
import java.nio.channels._
import kafka.utils.IteratorTemplate
import kafka.common.{MessageSizeTooLargeException, InvalidMessageSizeException, ErrorMapping}

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
  private var shallowValidByteCount = -1L
  if(sizeInBytes > Int.MaxValue)
    throw new InvalidMessageSizeException("Message set cannot be larger than " + Int.MaxValue)

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(MessageSet.createByteBuffer(compressionCodec, messages:_*), 0L, ErrorMapping.NoError)
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, messages: _*)
  }

  def getInitialOffset = initialOffset

  def getBuffer = buffer

  def getErrorCode = errorCode

  def serialized(): ByteBuffer = buffer

  def validBytes: Long = shallowValidBytes

  private def shallowValidBytes: Long = {
    if(shallowValidByteCount < 0) {
      val iter = this.internalIterator(true)
      while(iter.hasNext) {
        val messageAndOffset = iter.next
        shallowValidByteCount = messageAndOffset.offset
      }
    }
    if(shallowValidByteCount < initialOffset) 0
    else (shallowValidByteCount - initialOffset)
  }
  
  /** Write the messages in this set to the given channel */
  def writeTo(channel: GatheringByteChannel, offset: Long, size: Long): Long = {
    buffer.mark()
    val written = channel.write(buffer)
    buffer.reset()
    written
  }

  /** default iterator that iterates over decompressed messages */
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(true)

  def verifyMessageSize(maxMessageSize: Int){
    var shallowIter = internalIterator(true)
    while(shallowIter.hasNext){
      var messageAndOffset = shallowIter.next
      val payloadSize = messageAndOffset.message.payloadSize
      if ( payloadSize > maxMessageSize)
        throw new MessageSizeTooLargeException("payload size of " + payloadSize + " larger than " + maxMessageSize)
    }
  }

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. This is used in verifyMessageSize() function **/
  private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
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
        if(!newMessage.isValid)
          throw new InvalidMessageException("message is invalid, compression codec: " + newMessage.compressionCodec
            + " size: " + size + " curr offset: " + currValidBytes + " init offset: " + initialOffset)

        if(isShallow){
          currValidBytes += 4 + size
          trace("shallow iterator currValidBytes = " + currValidBytes)
          new MessageAndOffset(newMessage, currValidBytes)
        }
        else{
          newMessage.compressionCodec match {
            case NoCompressionCodec =>
              debug("Message is uncompressed. Valid byte count = %d".format(currValidBytes))
              innerIter = null
              currValidBytes += 4 + size
              trace("currValidBytes = " + currValidBytes)
              new MessageAndOffset(newMessage, currValidBytes)
            case _ =>
              debug("Message is compressed. Valid byte count = %d".format(currValidBytes))
              innerIter = CompressionUtils.decompress(newMessage).internalIterator()
              if (!innerIter.hasNext) {
                currValidBytes += 4 + lastMessageSize
                innerIter = null
              }
              makeNext()
          }
        }
      }

      override def makeNext(): MessageAndOffset = {
        if(isShallow){
          makeNextOuter
        }
        else{
          val isInnerDone = innerDone()
          debug("makeNext() in internalIterator: innerDone = " + isInnerDone)
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

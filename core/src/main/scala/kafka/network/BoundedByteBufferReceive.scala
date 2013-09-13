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

package kafka.network

import java.nio._
import java.nio.channels._
import kafka.utils._

/**
 * Represents a communication between the client and server
 * 
 */
@nonthreadsafe
private[kafka] class BoundedByteBufferReceive(val maxSize: Int) extends Receive with Logging {
  
  private val sizeBuffer = ByteBuffer.allocate(4)
  private var contentBuffer: ByteBuffer = null
  
  def this() = this(Int.MaxValue)
  
  var complete: Boolean = false
  
  /**
   * Get the content buffer for this transmission
   */
  def buffer: ByteBuffer = {
    expectComplete()
    contentBuffer
  }
  
  /**
   * Read the bytes in this response from the given channel
   */
  def readFrom(channel: ReadableByteChannel): Int = {
    expectIncomplete()
    var read = 0
    // have we read the request size yet?
    if(sizeBuffer.remaining > 0)
      read += Utils.read(channel, sizeBuffer)
    // have we allocated the request buffer yet?
    if(contentBuffer == null && !sizeBuffer.hasRemaining) {
      sizeBuffer.rewind()
      val size = sizeBuffer.getInt()
      if(size <= 0)
        throw new InvalidRequestException("%d is not a valid request size.".format(size))
      if(size > maxSize)
        throw new InvalidRequestException("Request of length %d is not valid, it is larger than the maximum size of %d bytes.".format(size, maxSize))
      contentBuffer = byteBufferAllocate(size)
    }
    // if we have a buffer read some stuff into it
    if(contentBuffer != null) {
      read = Utils.read(channel, contentBuffer)
      // did we get everything?
      if(!contentBuffer.hasRemaining) {
        contentBuffer.rewind()
        complete = true
      }
    }
    read
  }

  private def byteBufferAllocate(size: Int): ByteBuffer = {
    var buffer: ByteBuffer = null
    try {
      buffer = ByteBuffer.allocate(size)
    } catch {
      case e: OutOfMemoryError =>
        error("OOME with size " + size, e)
        throw e
      case e2: Throwable =>
        throw e2
    }
    buffer
  }
}

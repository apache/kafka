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
import kafka.api.RequestOrResponse

@nonthreadsafe
private[kafka] class BoundedByteBufferSend(val buffer: ByteBuffer) extends Send {
  
  private var sizeBuffer = ByteBuffer.allocate(4)

  // Avoid possibility of overflow for 2GB-4 byte buffer
  if(buffer.remaining > Int.MaxValue - sizeBuffer.limit)
    throw new IllegalStateException("Attempt to create a bounded buffer of " + buffer.remaining + " bytes, but the maximum " +
                                       "allowable size for a bounded buffer is " + (Int.MaxValue - sizeBuffer.limit) + ".")    
  sizeBuffer.putInt(buffer.limit)
  sizeBuffer.rewind()

  var complete: Boolean = false

  def this(size: Int) = this(ByteBuffer.allocate(size))
  
  def this(request: RequestOrResponse) = {
    this(request.sizeInBytes + (if(request.requestId != None) 2 else 0))
    request.requestId match {
      case Some(requestId) =>
        buffer.putShort(requestId)
      case None =>
    }

    request.writeTo(buffer)
    buffer.rewind()
  }

  
  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = channel.write(Array(sizeBuffer, buffer))
    // if we are done, mark it off
    if(!buffer.hasRemaining)
      complete = true    
    written.asInstanceOf[Int]
  }
    
}

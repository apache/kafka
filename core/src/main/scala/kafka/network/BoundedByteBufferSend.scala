/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

@nonthreadsafe
private[kafka] class BoundedByteBufferSend(val buffer: ByteBuffer) extends Send {
  
  private var sizeBuffer = ByteBuffer.allocate(4)
  
  sizeBuffer.putInt(buffer.limit)
  sizeBuffer.rewind()
  
  var complete: Boolean = false

  def this(size: Int) = this(ByteBuffer.allocate(size))
  
  def this(request: Request) = {
    this(request.sizeInBytes + 2)
    buffer.putShort(request.id)
    request.writeTo(buffer)
    buffer.rewind()
  }
  
  def writeTo(channel: WritableByteChannel): Int = {
    expectIncomplete()
    var written = 0
    // try to write the size if we haven't already
    if(sizeBuffer.hasRemaining)
      written += channel.write(sizeBuffer)
    // try to write the actual buffer itself
    if(!sizeBuffer.hasRemaining && buffer.hasRemaining)
      written += channel.write(buffer)
    // if we are done, mark it off
    if(!buffer.hasRemaining)
      complete = true
    
    written
  }
    
}

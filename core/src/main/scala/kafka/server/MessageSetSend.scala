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

package kafka.server

import java.nio._
import java.nio.channels._
import kafka.network._
import kafka.message._
import kafka.utils._
import kafka.common.ErrorMapping

/**
 * A zero-copy message response that writes the bytes needed directly from the file
 * wholly in kernel space
 */
@nonthreadsafe
private[server] class MessageSetSend(val messages: MessageSet, val errorCode: Short) extends Send {
  
  private var sent: Int = 0
  private val size: Int = messages.sizeInBytes
  private val header = ByteBuffer.allocate(6)
  header.putInt(size + 2)
  header.putShort(errorCode)
  header.rewind()
  
  var complete: Boolean = false

  def this(messages: MessageSet) = this(messages, ErrorMapping.NoError)

  def this() = this(MessageSet.Empty)

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    if(header.hasRemaining)
      written += channel.write(header)
    if(!header.hasRemaining) {
      val fileBytesSent = messages.writeTo(channel, sent, size - sent)
      written += fileBytesSent
      sent += fileBytesSent
    }

    if(logger.isTraceEnabled)
      if (channel.isInstanceOf[SocketChannel]) {
        val socketChannel = channel.asInstanceOf[SocketChannel]
        logger.trace(sent + " bytes written to " + socketChannel.socket.getRemoteSocketAddress() + " expecting to send " + size + " bytes")
      }

    if(sent >= size)
      complete = true
    written
  }
  
  def sendSize: Int = size + header.capacity
  
}

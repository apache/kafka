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
import kafka.utils.Logging

/**
 * Represents a stateful transfer of data to or from the network
 */
private[network] trait Transmission extends Logging {
  
  def complete: Boolean
  
  protected def expectIncomplete(): Unit = {
    if(complete)
      throw new IllegalStateException("This operation cannot be completed on a complete request.")
  }
  
  protected def expectComplete(): Unit = {
    if(!complete)
      throw new IllegalStateException("This operation cannot be completed on an incomplete request.")
  }
  
}

/**
 * A transmission that is being received from a channel
 */
private[kafka] trait Receive extends Transmission {
  
  def buffer: ByteBuffer
  
  def readFrom(channel: ReadableByteChannel): Int
  
  def readCompletely(channel: ReadableByteChannel): Int = {
    var read = 0
    while(!complete) {
      read = readFrom(channel)
      trace(read + " bytes read.")
    }
    read
  }
  
}

/**
 * A transmission that is being sent out to the channel
 */
private[kafka] trait Send extends Transmission {
    
  def writeTo(channel: GatheringByteChannel): Int
  
  def writeCompletely(channel: GatheringByteChannel): Int = {
    var written = 0
    while(!complete) {
      written = writeTo(channel)
      trace(written + " bytes written.")
    }
    written
  }
    
}

/**
 * A set of composite sends, sent one after another
 */
abstract class MultiSend[S <: Send](val sends: List[S]) extends Send {
  val expectedBytesToWrite: Int
  private var current = sends
  var totalWritten = 0

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete
    val written = current.head.writeTo(channel)
    totalWritten += written
    if(current.head.complete)
      current = current.tail
    written
  }
  
  def complete: Boolean = {
    if (current == Nil) {
      if (totalWritten != expectedBytesToWrite)
        error("mismatch in sending bytes over socket; expected: " + expectedBytesToWrite + " actual: " + totalWritten)
      return true
    }
    else
      return false
  }
}

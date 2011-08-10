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

package kafka.api

import java.nio._
import collection.mutable
import kafka.utils.IteratorTemplate
import kafka.message._

class MultiFetchResponse(val buffer: ByteBuffer, val numSets: Int, val offsets: Array[Long]) extends Iterable[ByteBufferMessageSet] {
  private val messageSets = new mutable.ListBuffer[ByteBufferMessageSet]
  
  for(i <- 0 until numSets) {
    val size = buffer.getInt()
    val errorCode: Int = buffer.getShort()
    val copy = buffer.slice()
    val payloadSize = size - 2
    copy.limit(payloadSize)
    buffer.position(buffer.position + payloadSize)
    messageSets += new ByteBufferMessageSet(copy, offsets(i), errorCode)
  }
 
  def iterator : Iterator[ByteBufferMessageSet] = {
    new IteratorTemplate[ByteBufferMessageSet] {
      val iter = messageSets.iterator

      override def makeNext(): ByteBufferMessageSet = {
        if(iter.hasNext)
          iter.next
        else
          return allDone
      }
    }
  }

  override def toString() = this.messageSets.toString
}

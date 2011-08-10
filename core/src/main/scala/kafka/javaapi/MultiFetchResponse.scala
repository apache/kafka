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

package kafka.javaapi

import kafka.utils.IteratorTemplate
import java.nio.ByteBuffer
import message.ByteBufferMessageSet

class MultiFetchResponse(buffer: ByteBuffer, numSets: Int, offsets: Array[Long]) extends java.lang.Iterable[ByteBufferMessageSet] {
  val underlyingBuffer = ByteBuffer.wrap(buffer.array)
    // this has the side effect of setting the initial position of buffer correctly
  val errorCode = underlyingBuffer.getShort

  import Implicits._
  val underlying = new kafka.api.MultiFetchResponse(underlyingBuffer, numSets, offsets)

  override def toString() = underlying.toString

  def iterator : java.util.Iterator[ByteBufferMessageSet] = {
    new IteratorTemplate[ByteBufferMessageSet] {
      val iter = underlying.iterator
      override def makeNext(): ByteBufferMessageSet = {
        if(iter.hasNext)
          iter.next
        else
          return allDone
      }
    }
  }
}

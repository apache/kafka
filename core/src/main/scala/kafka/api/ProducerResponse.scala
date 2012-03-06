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
import java.nio.channels._
import kafka.network._
import kafka.message._
import kafka.utils._
import kafka.common.ErrorMapping

@nonthreadsafe
class ProducerResponse(val versionId: Short, val correlationId: Int, val errors: Array[Int], val offsets: Array[Long]) extends Send {

  val sizeInBytes = 2 + 4 + 4 + (4 * errors.size) + 4 + (8 * offsets.size)

  private val buffer = ByteBuffer.allocate(sizeInBytes)
  buffer.putShort(versionId)
  buffer.putInt(correlationId)
  buffer.putInt(errors.size)
  errors.foreach(e => buffer.putInt(e))
  buffer.putInt(offsets.size)
  offsets.foreach(o => buffer.putLong(o))

  var complete: Boolean = false

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    written += channel.write(buffer)
    if(!buffer.hasRemaining)
      complete = true
    written
  }
}
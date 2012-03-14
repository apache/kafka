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

import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel
import kafka.common.ErrorMapping
import kafka.network.Send

object ProducerResponse {
  val CurrentVersion = 1.shortValue()

  def readFrom(buffer: ByteBuffer): ProducerResponse = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val errorsSize = buffer.getInt
    val errors = new Array[Short](errorsSize)
    for( i <- 0 until errorsSize) {
      errors(i) = buffer.getShort
    }
    val offsetsSize = buffer.getInt
    val offsets = new Array[Long](offsetsSize)
    for( i <- 0 until offsetsSize) {
      offsets(i) = buffer.getLong
    }
    new ProducerResponse(versionId, correlationId, errors, offsets)
  }

  def serializeResponse(producerResponse: ProducerResponse): ByteBuffer = {
    val buffer = ByteBuffer.allocate(producerResponse.sizeInBytes)
    producerResponse.writeTo(buffer)
    buffer.rewind()
    buffer
  }

  def deserializeResponse(buffer: ByteBuffer): ProducerResponse = readFrom(buffer)

}

case class ProducerResponse(versionId: Short, correlationId: Int, errors: Array[Short], offsets: Array[Long]) {
  val sizeInBytes = 2 + 4 + (4 + 2 * errors.length) + (4 + 8 * offsets.length)

  def writeTo(buffer: ByteBuffer) {
    /* version */
    buffer.putShort(versionId)
    /* correlation id */
    buffer.putInt(correlationId)
    /* errors */
    buffer.putInt(errors.length)
    errors.foreach(buffer.putShort(_))
    /* offsets */
    buffer.putInt(offsets.length)
    offsets.foreach(buffer.putLong(_))
  }
}

class ProducerResponseSend(val producerResponse: ProducerResponse,
                           val error: Int = ErrorMapping.NoError) extends Send {
  private val header = ByteBuffer.allocate(6)
  header.putInt(producerResponse.sizeInBytes + 2)
  header.putShort(error.toShort)
  header.rewind()

  val responseContent = ProducerResponse.serializeResponse(producerResponse)

  var complete = false

  def writeTo(channel: GatheringByteChannel):Int = {
    expectIncomplete()
    var written = 0
    if(header.hasRemaining)
      written += channel.write(header)

    trace("Wrote %d bytes for header".format(written))

    if(!header.hasRemaining && responseContent.hasRemaining)
        written += channel.write(responseContent)

    trace("Wrote %d bytes for header, errors and offsets".format(written))

    if(!header.hasRemaining && !responseContent.hasRemaining)
      complete = true

    written
  }
}

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
import kafka.common.ErrorMapping


object ProducerResponse {
  def readFrom(buffer: ByteBuffer): ProducerResponse = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val errorCode = buffer.getShort
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
    new ProducerResponse(versionId, correlationId, errors, offsets, errorCode)
  }
}

case class ProducerResponse(versionId: Short, correlationId: Int, errors: Array[Short],
                            offsets: Array[Long], errorCode: Short = ErrorMapping.NoError) extends RequestOrResponse{
  val sizeInBytes = 2 + 2 + 4 + (4 + 2 * errors.length) + (4 + 8 * offsets.length)

  def writeTo(buffer: ByteBuffer) {
    /* version id */
    buffer.putShort(versionId)
    /* correlation id */
    buffer.putInt(correlationId)
    /* error code */
    buffer.putShort(errorCode)
    /* errors */
    buffer.putInt(errors.length)
    errors.foreach(buffer.putShort(_))
    /* offsets */
    buffer.putInt(offsets.length)
    offsets.foreach(buffer.putLong(_))
  }

  // need to override case-class equals due to broken java-array equals()
  override def equals(other: Any): Boolean = {
   other match {
      case that: ProducerResponse =>
        ( correlationId == that.correlationId &&
          versionId == that.versionId &&
          errorCode == that.errorCode &&
          errors.toSeq == that.errors.toSeq &&
          offsets.toSeq == that.offsets.toSeq)
      case _ => false
    }
  }
}
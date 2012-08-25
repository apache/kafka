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


object OffsetResponse {
  def readFrom(buffer: ByteBuffer): OffsetResponse = {
    val versionId = buffer.getShort
    val errorCode = buffer.getShort
    val offsetsSize = buffer.getInt
    val offsets = new Array[Long](offsetsSize)
    for( i <- 0 until offsetsSize) {
      offsets(i) = buffer.getLong
    }
    new OffsetResponse(versionId, offsets, errorCode)
  }
}

case class OffsetResponse(versionId: Short,
                          offsets: Array[Long],
                          errorCode: Short = ErrorMapping.NoError) extends RequestOrResponse{
  val sizeInBytes = 2 + 2 + offsets.foldLeft(4)((sum, _) => sum + 8)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    /* error code */
    buffer.putShort(errorCode)
    buffer.putInt(offsets.length)
    offsets.foreach(buffer.putLong(_))
  }

    // need to override case-class equals due to broken java-array equals()
  override def equals(other: Any): Boolean = {
   other match {
      case that: OffsetResponse =>
        ( versionId == that.versionId &&
          errorCode == that.errorCode &&
          offsets.toSeq == that.offsets.toSeq)
      case _ => false
    }
  }
}

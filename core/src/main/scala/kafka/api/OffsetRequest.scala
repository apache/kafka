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
import kafka.utils.Utils

object OffsetRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""

  val SmallestTimeString = "smallest"
  val LargestTimeString = "largest"
  val LatestTime = -1L
  val EarliestTime = -2L

  def readFrom(buffer: ByteBuffer): OffsetRequest = {
    val versionId = buffer.getShort
    val clientId = Utils.readShortString(buffer)
    val topic = Utils.readShortString(buffer, "UTF-8")
    val partition = buffer.getInt()
    val offset = buffer.getLong
    val maxNumOffsets = buffer.getInt
    new OffsetRequest(versionId, clientId, topic, partition, offset, maxNumOffsets)
  }
}

case class OffsetRequest(versionId: Short = OffsetRequest.CurrentVersion,
                    clientId: String = OffsetRequest.DefaultClientId,
                    topic: String,
                    partition: Int,
                    time: Long,
                    maxNumOffsets: Int) extends RequestOrResponse(Some(RequestKeys.OffsetsKey)) {
  def this(topic: String, partition: Int, time: Long, maxNumOffsets: Int) =
    this(OffsetRequest.CurrentVersion, OffsetRequest.DefaultClientId, topic, partition, time, maxNumOffsets)



  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    Utils.writeShortString(buffer, clientId)
    Utils.writeShortString(buffer, topic)
    buffer.putInt(partition)
    buffer.putLong(time)
    buffer.putInt(maxNumOffsets)
  }

  def sizeInBytes(): Int = 2 + (2 + clientId.length()) + (2 + topic.length) + 4 + 8 + 4

  override def toString(): String= "OffsetRequest(version:" + versionId + ", client id:" + clientId +
          ", topic:" + topic + ", part:" + partition + ", time:" + time + ", maxNumOffsets:" + maxNumOffsets + ")"
}

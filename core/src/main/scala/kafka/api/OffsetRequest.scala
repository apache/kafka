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
import kafka.utils.{nonthreadsafe, Utils}
import kafka.network.{Send, Request}
import java.nio.channels.GatheringByteChannel
import kafka.common.ErrorMapping

object OffsetRequest {
  val SmallestTimeString = "smallest"
  val LargestTimeString = "largest"
  val LatestTime = -1L
  val EarliestTime = -2L

  def readFrom(buffer: ByteBuffer): OffsetRequest = {
    val topic = Utils.readShortString(buffer, "UTF-8")
    val partition = buffer.getInt()
    val offset = buffer.getLong
    val maxNumOffsets = buffer.getInt
    new OffsetRequest(topic, partition, offset, maxNumOffsets)
  }

  def serializeOffsetArray(offsets: Array[Long]): ByteBuffer = {
    val size = 4 + 8 * offsets.length
    val buffer = ByteBuffer.allocate(size)
    buffer.putInt(offsets.length)
    for (i <- 0 until offsets.length)
      buffer.putLong(offsets(i))
    buffer.rewind
    buffer
  }

  def deserializeOffsetArray(buffer: ByteBuffer): Array[Long] = {
    val size = buffer.getInt
    val offsets = new Array[Long](size)
    for (i <- 0 until offsets.length)
      offsets(i) = buffer.getLong
    offsets
  }
}

class OffsetRequest(val topic: String,
                    val partition: Int,
                    val time: Long,
                    val maxNumOffsets: Int) extends Request(RequestKeys.Offsets) {

  def writeTo(buffer: ByteBuffer) {
    Utils.writeShortString(buffer, topic, "UTF-8")
    buffer.putInt(partition)
    buffer.putLong(time)
    buffer.putInt(maxNumOffsets)
  }

  def sizeInBytes(): Int = 2 + topic.length + 4 + 8 + 4

  override def toString(): String= "OffsetRequest(topic:" + topic + ", part:" + partition + ", time:" + time +
          ", maxNumOffsets:" + maxNumOffsets + ")"
}

@nonthreadsafe
private[kafka] class OffsetArraySend(offsets: Array[Long]) extends Send {
  private var size: Long = offsets.foldLeft(4)((sum, _) => sum + 8)
  private val header = ByteBuffer.allocate(6)
  header.putInt(size.asInstanceOf[Int] + 2)
  header.putShort(ErrorMapping.NoError.asInstanceOf[Short])
  header.rewind()
  private val contentBuffer = OffsetRequest.serializeOffsetArray(offsets)

  var complete: Boolean = false

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    if(header.hasRemaining)
      written += channel.write(header)
    if(!header.hasRemaining && contentBuffer.hasRemaining)
      written += channel.write(contentBuffer)

    if(!contentBuffer.hasRemaining)
      complete = true
    written
  }
}

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

import org.apache.kafka.common.KafkaException

object LogOffsetMetadata {
  val UnknownOffsetMetadata = new LogOffsetMetadata(-1, 0, 0)
  val UnknownSegBaseOffset = -1L
  val UnknownFilePosition = -1

  class OffsetOrdering extends Ordering[LogOffsetMetadata] {
    override def compare(x: LogOffsetMetadata , y: LogOffsetMetadata ): Int = {
      x.offsetDiff(y).toInt
    }
  }

}

/*
 * A log offset structure, including:
 *  1. the message offset
 *  2. the base message offset of the located segment
 *  3. the physical position on the located segment
 */
case class LogOffsetMetadata(messageOffset: Long,
                             segmentBaseOffset: Long = LogOffsetMetadata.UnknownSegBaseOffset,
                             relativePositionInSegment: Int = LogOffsetMetadata.UnknownFilePosition) {

  // check if this offset is already on an older segment compared with the given offset
  def offsetOnOlderSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly())
      throw new KafkaException("%s cannot compare its segment info with %s since it only has message offset info".format(this, that))

    this.segmentBaseOffset < that.segmentBaseOffset
  }

  // check if this offset is on the same segment with the given offset
  def offsetOnSameSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly())
      throw new KafkaException("%s cannot compare its segment info with %s since it only has message offset info".format(this, that))

    this.segmentBaseOffset == that.segmentBaseOffset
  }

  // check if this offset is before the given offset. We need to compare both message offset and segment base offset.
  def precedes(that: LogOffsetMetadata): Boolean = this.messageOffset < that.messageOffset ||
    (this.messageOffset == that.messageOffset && this.segmentBaseOffset < that.segmentBaseOffset)

  // compute the number of messages between this offset to the given offset
  def offsetDiff(that: LogOffsetMetadata): Long = {
    this.messageOffset - that.messageOffset
  }

  // compute the number of bytes between this offset to the given offset
  // if they are on the same segment and this offset precedes the given offset
  def positionDiff(that: LogOffsetMetadata): Int = {
    if(!offsetOnSameSegment(that))
      throw new KafkaException("%s cannot compare its segment position with %s since they are not on the same segment".format(this, that))
    if(messageOffsetOnly())
      throw new KafkaException("%s cannot compare its segment position with %s since it only has message offset info".format(this, that))

    this.relativePositionInSegment - that.relativePositionInSegment
  }

  // decide if the offset metadata only contains message offset info
  def messageOffsetOnly(): Boolean = {
    segmentBaseOffset == LogOffsetMetadata.UnknownSegBaseOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition
  }

  override def toString = messageOffset.toString + " [" + segmentBaseOffset + " : " + relativePositionInSegment + "]"

}

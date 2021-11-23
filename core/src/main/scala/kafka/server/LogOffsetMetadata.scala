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

import kafka.log.UnifiedLog
import org.apache.kafka.common.KafkaException

object LogOffsetMetadata {
  val UnknownOffsetMetadata = LogOffsetMetadata(-1, 0, 0)
  val UnknownFilePosition = -1

  class OffsetOrdering extends Ordering[LogOffsetMetadata] {
    override def compare(x: LogOffsetMetadata, y: LogOffsetMetadata): Int = {
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
                             segmentBaseOffset: Long = UnifiedLog.UnknownOffset,
                             relativePositionInSegment: Int = LogOffsetMetadata.UnknownFilePosition) {

  // check if this offset is already on an older segment compared with the given offset
  def onOlderSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly)
      throw new KafkaException(s"$this cannot compare its segment info with $that since it only has message offset info")

    this.segmentBaseOffset < that.segmentBaseOffset
  }

  // check if this offset is on the same segment with the given offset
  def onSameSegment(that: LogOffsetMetadata): Boolean = {
    if (messageOffsetOnly)
      throw new KafkaException(s"$this cannot compare its segment info with $that since it only has message offset info")

    this.segmentBaseOffset == that.segmentBaseOffset
  }

  // compute the number of messages between this offset to the given offset
  def offsetDiff(that: LogOffsetMetadata): Long = {
    this.messageOffset - that.messageOffset
  }

  // compute the number of bytes between this offset to the given offset
  // if they are on the same segment and this offset precedes the given offset
  def positionDiff(that: LogOffsetMetadata): Int = {
    if(!onSameSegment(that))
      throw new KafkaException(s"$this cannot compare its segment position with $that since they are not on the same segment")
    if(messageOffsetOnly)
      throw new KafkaException(s"$this cannot compare its segment position with $that since it only has message offset info")

    this.relativePositionInSegment - that.relativePositionInSegment
  }

  // decide if the offset metadata only contains message offset info
  def messageOffsetOnly: Boolean = {
    segmentBaseOffset == UnifiedLog.UnknownOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition
  }

  override def toString = s"(offset=$messageOffset segment=[$segmentBaseOffset:$relativePositionInSegment])"

}

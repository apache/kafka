/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.KafkaException;

/*
 * A log offset structure, including:
 *  1. the message offset
 *  2. the base message offset of the located segment
 *  3. the physical position on the located segment
 */
public final class LogOffsetMetadata {

    //TODO KAFKA-14484 remove once UnifiedLog has been moved to the storage module
    private static final long UNIFIED_LOG_UNKNOWN_OFFSET = -1L;

    public static final LogOffsetMetadata UNKNOWN_OFFSET_METADATA = new LogOffsetMetadata(-1L, 0L, 0);

    private static final int UNKNOWN_FILE_POSITION = -1;

    public final long messageOffset;
    public final long segmentBaseOffset;
    public final int relativePositionInSegment;

    public LogOffsetMetadata(long messageOffset) {
        this(messageOffset, UNIFIED_LOG_UNKNOWN_OFFSET, UNKNOWN_FILE_POSITION);
    }

    public LogOffsetMetadata(long messageOffset,
                             long segmentBaseOffset,
                             int relativePositionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    // check if this offset is already on an older segment compared with the given offset
    public boolean onOlderSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly())
            throw new KafkaException(this + " cannot compare its segment info with " + that + " since it only has message offset info");

        return this.segmentBaseOffset < that.segmentBaseOffset;
    }

    // check if this offset is on the same segment with the given offset
    private boolean onSameSegment(LogOffsetMetadata that) {
        return this.segmentBaseOffset == that.segmentBaseOffset;
    }

    // compute the number of bytes between this offset to the given offset
    // if they are on the same segment and this offset precedes the given offset
    public int positionDiff(LogOffsetMetadata that) {
        if (messageOffsetOnly())
            throw new KafkaException(this + " cannot compare its segment position with " + that + " since it only has message offset info");
        if (!onSameSegment(that))
            throw new KafkaException(this + " cannot compare its segment position with " + that + " since they are not on the same segment");

        return this.relativePositionInSegment - that.relativePositionInSegment;
    }

    // decide if the offset metadata only contains message offset info
    public boolean messageOffsetOnly() {
        return segmentBaseOffset == UNIFIED_LOG_UNKNOWN_OFFSET && relativePositionInSegment == UNKNOWN_FILE_POSITION;
    }

    @Override
    public String toString() {
        return "(offset=" + messageOffset + "segment=[" + segmentBaseOffset + ":" + relativePositionInSegment + "])";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogOffsetMetadata that = (LogOffsetMetadata) o;
        return messageOffset == that.messageOffset
                && segmentBaseOffset == that.segmentBaseOffset
                && relativePositionInSegment == that.relativePositionInSegment;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(messageOffset);
        result = 31 * result + Long.hashCode(segmentBaseOffset);
        result = 31 * result + Integer.hashCode(relativePositionInSegment);
        return result;
    }
}

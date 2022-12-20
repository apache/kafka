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

import org.apache.kafka.common.record.DefaultRecordBatch;

import java.util.Objects;

public class BatchMetadata {

    public final int lastSeq;
    public final long lastOffset;
    public final int offsetDelta;
    public final long timestamp;

    public BatchMetadata(
            int lastSeq,
            long lastOffset,
            int offsetDelta,
            long timestamp) {
        this.lastSeq = lastSeq;
        this.lastOffset = lastOffset;
        this.offsetDelta = offsetDelta;
        this.timestamp = timestamp;
    }

    public int firstSeq() {
        return DefaultRecordBatch.decrementSequence(lastSeq, offsetDelta);
    }

    public long firstOffset() {
        return lastOffset - offsetDelta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchMetadata that = (BatchMetadata) o;
        return lastSeq == that.lastSeq && lastOffset == that.lastOffset && offsetDelta == that.offsetDelta && timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastSeq, lastOffset, offsetDelta, timestamp);
    }

    @Override
    public String toString() {
        return "BatchMetadata{" +
                "lastSeq=" + lastSeq +
                ", lastOffset=" + lastOffset +
                ", offsetDelta=" + offsetDelta +
                ", timestamp=" + timestamp +
                '}';
    }
}

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

import java.util.Objects;
import java.util.OptionalLong;

public final class TxnMetadata {
    public final long producerId;
    public final LogOffsetMetadata firstOffset;
    public OptionalLong lastOffset;

    public TxnMetadata(long producerId,
                       LogOffsetMetadata firstOffset,
                       OptionalLong lastOffset) {
        this.producerId = producerId;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        Objects.requireNonNull(firstOffset, "firstOffset must be non null");
    }
    public TxnMetadata(long producerId, long firstOffset) {
        this(producerId, new LogOffsetMetadata(firstOffset));
    }

    public TxnMetadata(long producerId, LogOffsetMetadata firstOffset) {
        this(producerId, firstOffset, OptionalLong.empty());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TxnMetadata that = (TxnMetadata) o;

        if (producerId != that.producerId) return false;
        if (!Objects.equals(firstOffset, that.firstOffset)) return false;
        return Objects.equals(lastOffset, that.lastOffset);
    }

    @Override
    public int hashCode() {
        int result = (int) (producerId ^ (producerId >>> 32));
        result = 31 * result + (firstOffset != null ? firstOffset.hashCode() : 0);
        result = 31 * result + (lastOffset != null ? lastOffset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TxnMetadata(" +
                "producerId=" + producerId +
                ", firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ')';
    }
}

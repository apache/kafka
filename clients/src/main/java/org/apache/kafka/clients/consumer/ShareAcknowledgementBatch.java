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
package org.apache.kafka.clients.consumer;

import java.util.List;
import java.util.Objects;

public final class ShareAcknowledgementBatch {
    private final long firstOffset;
    private final long lastOffset;
    private final List<Byte> acknowledgeTypes;

    public ShareAcknowledgementBatch(long firstOffset, long lastOffset, List<Byte> acknowledgeTypes) {
        if (lastOffset < firstOffset) {
            throw new IllegalArgumentException("lastOffset cannot be smaller than firstOffset");
        }
        Objects.requireNonNull(acknowledgeTypes, "acknowledgeTypes cannot be null");
        if (acknowledgeTypes.isEmpty()) {
            throw new IllegalArgumentException("acknowledgeTypes cannot be empty");
        }
        long recordCount = lastOffset - firstOffset + 1;
        if (acknowledgeTypes.size() != 1 && acknowledgeTypes.size() != recordCount) {
            throw new IllegalArgumentException("acknowledgeTypes must contain one type or one type per offset");
        }

        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.acknowledgeTypes = List.copyOf(acknowledgeTypes);
    }

    public long firstOffset() {
        return firstOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public List<Byte> acknowledgeTypes() {
        return acknowledgeTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShareAcknowledgementBatch)) return false;
        ShareAcknowledgementBatch that = (ShareAcknowledgementBatch) o;
        return firstOffset == that.firstOffset
            && lastOffset == that.lastOffset
            && acknowledgeTypes.equals(that.acknowledgeTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstOffset, lastOffset, acknowledgeTypes);
    }

    @Override
    public String toString() {
        return "ShareAcknowledgementBatch(" +
            "firstOffset=" + firstOffset +
            ", lastOffset=" + lastOffset +
            ", acknowledgeTypes=" + acknowledgeTypes +
            ")";
    }
}

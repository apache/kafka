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
package org.apache.kafka.storage.internals.log;

import java.util.Objects;

public class LoadedLogOffsets {
    public final long logStartOffset;
    public final long recoveryPoint;
    public final LogOffsetMetadata nextOffsetMetadata;

    public LoadedLogOffsets(final long logStartOffset,
                            final long recoveryPoint,
                            final LogOffsetMetadata nextOffsetMetadata) {
        this.logStartOffset = logStartOffset;
        this.recoveryPoint = recoveryPoint;
        this.nextOffsetMetadata = Objects.requireNonNull(nextOffsetMetadata, "nextOffsetMetadata should not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final LoadedLogOffsets that = (LoadedLogOffsets) o;
        return logStartOffset == that.logStartOffset
                && recoveryPoint == that.recoveryPoint
                && nextOffsetMetadata.equals(that.nextOffsetMetadata);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(logStartOffset);
        result = 31 * result + Long.hashCode(recoveryPoint);
        result = 31 * result + nextOffsetMetadata.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LoadedLogOffsets(" +
                "logStartOffset=" + logStartOffset +
                ", recoveryPoint=" + recoveryPoint +
                ", nextOffsetMetadata=" + nextOffsetMetadata +
                ')';
    }
}

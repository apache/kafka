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
package org.apache.kafka.raft;

import java.util.Objects;
import java.util.Optional;

/**
 * Metadata for specific local log offset
 */
public class LogOffsetMetadata {

    public final long offset;
    public final Optional<OffsetMetadata> metadata;

    public LogOffsetMetadata(long offset) {
        this(offset, Optional.empty());
    }

    public LogOffsetMetadata(long offset, Optional<OffsetMetadata> metadata) {
        this.offset = offset;
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "LogOffsetMetadata(offset=" + offset +
                ", metadata=" + metadata + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LogOffsetMetadata) {
            LogOffsetMetadata other = (LogOffsetMetadata) obj;
            return this.offset == other.offset &&
                   this.metadata.equals(other.metadata);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, metadata);
    }
}

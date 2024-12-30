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
package org.apache.kafka.coordinator.group.modern.consumer;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The metadata associated with a regular expression in a Consumer Group.
 */
public class ResolvedRegularExpression {
    public static final ResolvedRegularExpression EMPTY = new ResolvedRegularExpression(Collections.emptySet(), -1L, -1L);

    /**
     * The set of resolved topics.
     */
    public final Set<String> topics;

    /**
     * The version of the metadata image used to resolve the topics.
     */
    public final long version;

    /**
     * The timestamp at the time of the resolution.
     */
    public final long timestamp;

    public ResolvedRegularExpression(
        Set<String> topics,
        long version,
        long timestamp
    ) {
        this.topics = Collections.unmodifiableSet(Objects.requireNonNull(topics));
        this.version = version;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResolvedRegularExpression that = (ResolvedRegularExpression) o;

        if (version != that.version) return false;
        if (timestamp != that.timestamp) return false;
        return topics.equals(that.topics);
    }

    @Override
    public int hashCode() {
        int result = topics.hashCode();
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "RegularExpressionMetadata(" +
            "topics=" + topics +
            ", version=" + version +
            ", timestamp=" + timestamp +
            ')';
    }
}

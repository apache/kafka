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
 *
 * @param topics    The set of resolved topics.
 * @param version   The version of the metadata image used to resolve the topics.
 * @param timestamp The timestamp at the time of the resolution.
 */
public record ResolvedRegularExpression(Set<String> topics, long version, long timestamp) {
    public static final ResolvedRegularExpression EMPTY = new ResolvedRegularExpression(Set.of(), -1L, -1L);

    public ResolvedRegularExpression {
        topics = Collections.unmodifiableSet(Objects.requireNonNull(topics));
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

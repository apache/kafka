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
package org.apache.kafka.connect.mirror.tracking;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * [CUSTOM] Tracks the last-successfully-loaded offset per topic-partition.
 *
 * <p>The stored value is always <em>offset + 1</em> — i.e., the next offset MM2
 * expects to consume. This convention makes the truncation check arithmetic
 * straightforward: if {@code beginningOffset > storedValue} then messages were lost.
 *
 * <p>This class is intentionally a thin, pure data structure with no Kafka
 * dependency so it is easy to unit-test in isolation.
 */
public class PartitionOffsetTracker {

    /** Internal state: tp → nextExpectedOffset */
    private final Map<TopicPartition, Long> offsets = new HashMap<>();

    /**
     * Records that {@code offset} was the most recently consumed record on {@code tp}.
     * Stores {@code offset + 1} as the next expected value.
     */
    public void recordConsumed(TopicPartition tp, long offset) {
        offsets.put(tp, offset + 1L);
    }

    /**
     * Directly sets the next-expected offset (used for initialisation and reset recovery).
     */
    public void setNextExpected(TopicPartition tp, long nextOffset) {
        offsets.put(tp, nextOffset);
    }

    /**
     * Returns the next expected offset for {@code tp}, or {@code null} if the
     * partition has not been seen yet (first-run case — no check should be performed).
     */
    public Long getNextExpected(TopicPartition tp) {
        return offsets.get(tp);
    }

    /**
     * Returns {@code true} if the partition has been initialised (i.e., at least
     * one record has been consumed or an explicit seek has been recorded).
     */
    public boolean isTracked(TopicPartition tp) {
        return offsets.containsKey(tp);
    }

    /** Returns an unmodifiable snapshot of the internal state (useful for debugging). */
    public Map<TopicPartition, Long> snapshot() {
        return Collections.unmodifiableMap(new HashMap<>(offsets));
    }
}

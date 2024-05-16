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
package org.apache.kafka.raft.internals;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Map;

/**
 * A implementation for {@code LogHistory} which uses a red-black tree to store values sorted by offset.
 */
final public class TreeMapLogHistory<T> implements LogHistory<T> {
    private final NavigableMap<Long, T> history = new TreeMap<>();

    @Override
    public void addAt(long offset, T value) {
        if (offset < 0) {
            throw new IllegalArgumentException(
                String.format("Next offset %d must be greater than or equal to 0", offset)
            );
        }

        Map.Entry<Long, ?> lastEntry = history.lastEntry();
        if (lastEntry != null && offset <= lastEntry.getKey()) {
            throw new IllegalArgumentException(
                String.format("Next offset %d must be greater than the last offset %d", offset, lastEntry.getKey())
            );
        }

        history.put(offset, value);
    }

    @Override
    public Optional<T> valueAtOrBefore(long offset) {
        return Optional.ofNullable(history.floorEntry(offset)).map(Map.Entry::getValue);
    }

    @Override
    public Optional<Entry<T>> lastEntry() {
        return Optional.ofNullable(history.lastEntry()).map(entry -> new Entry<>(entry.getKey(), entry.getValue()));
    }

    @Override
    public void truncateNewEntries(long endOffset) {
        history.tailMap(endOffset, true).clear();
    }

    @Override
    public void truncateOldEntries(long startOffset) {
        NavigableMap<Long, T> lesserValues = history.headMap(startOffset, true);
        while (lesserValues.size() > 1) {
            // Poll and ignore the entry to remove the first entry
            lesserValues.pollFirstEntry();
        }
    }

    @Override
    public void clear() {
        history.clear();
    }
}

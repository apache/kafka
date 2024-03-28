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

// TODO: document this type
// TODO: add unittest
final public class TreeMapHistory<T> implements History<T> {
    private final NavigableMap<Long, T> history = new TreeMap<>();

    @Override
    public void addAt(long offset, T newValue) {
        Map.Entry<Long, ?> lastEntry = history.lastEntry();
        if (lastEntry != null && offset <= lastEntry.getKey()) {
            throw new IllegalArgumentException(
                String.format("Next offset %d must be greater than the last offset %d", offset, lastEntry.getKey())
            );
        }

        history.compute(
            offset,
            (key, oldValue) -> {
                if (oldValue != null) {
                    throw new IllegalArgumentException(
                        String.format("Rejected %s since a value already exist at %d: %s", newValue, offset, oldValue)
                    );
                }

                return newValue;
            }
        );
    }

    @Override
    public Optional<T> valueAt(long offset) {
        return Optional.ofNullable(history.floorEntry(offset)).map(Map.Entry::getValue);
    }

    @Override
    public Optional<Entry<T>> lastEntry() {
        return Optional.ofNullable(history.lastEntry()).map(entry -> new Entry<>(entry.getKey(), entry.getValue()));
    }

    @Override
    public void truncateTo(long endOffset) {
        history.tailMap(endOffset, false).clear();
    }

    @Override
    public void trimPrefixTo(long startOffset) {
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

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
package org.apache.kafka.streams.state.internals;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class Position {
    private final ConcurrentMap<String, ConcurrentMap<Integer, AtomicLong>> position;

    public static Position emptyPosition() {
        final HashMap<String, Map<Integer, Long>> pos = new HashMap<>();
        return new Position(pos);
    }

    public static Position fromMap(final Map<String, Map<Integer, Long>> map) {
        return new Position(map);
    }

    private Position(final Map<String, Map<Integer, Long>> other) {
        this.position = new ConcurrentHashMap<>();
        merge(other, (t, e) -> update(t, e.getKey(), e.getValue().longValue()));
    }

    public Position update(final String topic, final int partition, final long offset) {
        if (topic != null) {
            position.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
            final ConcurrentMap<Integer, AtomicLong> topicMap = position.get(topic);
            topicMap.computeIfAbsent(partition, k -> new AtomicLong(0));
            topicMap.get(partition).getAndAccumulate(offset, Math::max);
        }
        return this;
    }

    public void merge(final Position other) {
        merge(other.position, (a, b) -> update(a, b.getKey(), b.getValue().longValue()));
    }

    @Override
    public String toString() {
        return "Position{" +
                "position=" + position +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Position other = (Position) o;
        final HashMap<String, HashMap<Integer, Long>> position1 = new HashMap<>();
        merge(position, (t, e) -> position1.computeIfAbsent(t, k -> new HashMap<Integer, Long>()).put(e.getKey(), e.getValue().longValue()));
        final HashMap<String, HashMap<Integer, Long>> position2 = new HashMap<>();
        merge(other.position, (t, e) -> position2.computeIfAbsent(t, k -> new HashMap<Integer, Long>()).put(e.getKey(), e.getValue().longValue()));

        return Objects.equals(position1, position2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }

    private void merge(final Map<String, ? extends Map<Integer, ? extends Number>> other, final BiConsumer<String, Entry<Integer, ? extends Number>> func) {
        for (final Entry<String, ? extends Map<Integer, ? extends Number>> entry : other.entrySet()) {
            final String topic = entry.getKey();
            final Map<Integer, ? extends Number> inputMap = entry.getValue();
            for (final Entry<Integer, ? extends Number> topicEntry : inputMap.entrySet()) {
                func.accept(topic, topicEntry);
            }
        }
    }
}
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


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

    public ByteBuffer serialize() {

        final byte version = (byte) 0;

        int arraySize = Byte.SIZE; // version

        final int nTopics = position.size();
        arraySize += Integer.SIZE;

        final ArrayList<Entry<String, ConcurrentMap<Integer, AtomicLong>>> entries =
                new ArrayList<>(position.entrySet());
        final byte[][] topics = new byte[entries.size()][];

        for (int i = 0; i < nTopics; i++) {
            final Entry<String, ConcurrentMap<Integer, AtomicLong>> entry = entries.get(i);
            final byte[] topicBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            topics[i] = topicBytes;
            arraySize += Integer.SIZE; // topic name length
            arraySize += topicBytes.length; // topic name itself

            final Map<Integer, AtomicLong> partitionOffsets = entry.getValue();
            arraySize += Integer.SIZE; // Number of PartitionOffset pairs
            arraySize += (Integer.SIZE + Long.SIZE)
                    * partitionOffsets.size(); // partitionOffsets themselves
        }

        final ByteBuffer buffer = ByteBuffer.allocate(arraySize);
        buffer.put(version);

        buffer.putInt(nTopics);
        for (int i = 0; i < nTopics; i++) {
            buffer.putInt(topics[i].length);
            buffer.put(topics[i]);

            final Entry<String, ConcurrentMap<Integer, AtomicLong>> entry = entries.get(i);
            final ConcurrentMap<Integer, AtomicLong> partitionOffsets = entry.getValue();
            buffer.putInt(partitionOffsets.size());
            for (final Entry<Integer, AtomicLong> partitionOffset : partitionOffsets.entrySet()) {
                buffer.putInt(partitionOffset.getKey());
                buffer.putLong(partitionOffset.getValue().longValue());
            }
        }

        buffer.flip();
        return buffer;

    }

    public static Position deserialize(final ByteBuffer buffer) {
        final byte version = buffer.get();

        switch (version) {
            case (byte) 0: // actual position, v0
                final int nTopics = buffer.getInt();
                final Map<String, Map<Integer, Long>> position = new HashMap<>(nTopics);
                for (int i = 0; i < nTopics; i++) {
                    final int topicNameLength = buffer.getInt();
                    final byte[] topicNameBytes = new byte[topicNameLength];
                    buffer.get(topicNameBytes);
                    final String topic = new String(topicNameBytes, StandardCharsets.UTF_8);

                    final int numPairs = buffer.getInt();
                    final Map<Integer, Long> partitionOffsets = new HashMap<>(numPairs);
                    for (int j = 0; j < numPairs; j++) {
                        partitionOffsets.put(buffer.getInt(), buffer.getLong());
                    }
                    position.put(topic, partitionOffsets);
                }
                return new Position(position);
            default:
                throw new IllegalArgumentException(
                        "Unknown version " + version + " when deserializing Position"
                );
        }
    }

    public boolean isUnbounded() {
        return position != null && position.isEmpty();
    }

    public Map<Integer, Long> getBound(final String topic) {
        final Map<Integer, Long> bounds = new HashMap<>();
        for (final Integer partition: position.get(topic).keySet()) {
            bounds.put(partition, position.get(topic).get(partition).longValue());
        }
        return bounds;
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
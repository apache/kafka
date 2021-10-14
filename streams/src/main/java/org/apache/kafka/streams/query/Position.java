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
package org.apache.kafka.streams.query;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class Position {

    private final Map<String, Map<Integer, Long>> position;

    private Position(final Map<String, Map<Integer, Long>> position) {
        this.position = position;
    }

    public static Position emptyPosition() {
        return new Position(new HashMap<>());
    }

    public static Position fromMap(final Map<String, Map<Integer, Long>> map) {
        return new Position(deepCopy(map));
    }

    public Position withComponent(final String topic, final int partition, final long offset) {
        final Map<String, Map<Integer, Long>> updated = deepCopy(position);
        updated.computeIfAbsent(topic, k -> new HashMap<>()).put(partition, offset);
        return new Position(updated);
    }

    public Position merge(final Position other) {
        if (other == null) {
            return this;
        } else {
            final Map<String, Map<Integer, Long>> copy = deepCopy(position);
            for (final Entry<String, Map<Integer, Long>> entry : other.position.entrySet()) {
                final String topic = entry.getKey();
                final Map<Integer, Long> partitionMap =
                    copy.computeIfAbsent(topic, k -> new HashMap<>());
                for (final Entry<Integer, Long> partitionOffset : entry.getValue().entrySet()) {
                    final Integer partition = partitionOffset.getKey();
                    final Long offset = partitionOffset.getValue();
                    if (!partitionMap.containsKey(partition)
                        || partitionMap.get(partition) < offset) {
                        partitionMap.put(partition, offset);
                    }
                }
            }
            return new Position(copy);
        }
    }

    public Set<String> getTopics() {
        return Collections.unmodifiableSet(position.keySet());
    }

    public Map<Integer, Long> getBound(final String topic) {
        return Collections.unmodifiableMap(position.get(topic));
    }

    public ByteBuffer serialize() {
        final byte version = (byte) 0;

        int arraySize = Byte.SIZE; // version

        final int nTopics = position.size();
        arraySize += Integer.SIZE;

        final ArrayList<Entry<String, Map<Integer, Long>>> entries =
            new ArrayList<>(position.entrySet());
        final byte[][] topics = new byte[entries.size()][];

        for (int i = 0; i < nTopics; i++) {
            final Entry<String, Map<Integer, Long>> entry = entries.get(i);
            final byte[] topicBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            topics[i] = topicBytes;
            arraySize += Integer.SIZE; // topic name length
            arraySize += topicBytes.length; // topic name itself

            final Map<Integer, Long> partitionOffsets = entry.getValue();
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

            final Entry<String, Map<Integer, Long>> entry = entries.get(i);
            final Map<Integer, Long> partitionOffsets = entry.getValue();
            buffer.putInt(partitionOffsets.size());
            for (final Entry<Integer, Long> partitionOffset : partitionOffsets.entrySet()) {
                buffer.putInt(partitionOffset.getKey());
                buffer.putLong(partitionOffset.getValue());
            }
        }

        buffer.flip();
        return buffer;
    }

    public static Position deserialize(final ByteBuffer buffer) {
        final byte version = buffer.get();

        switch (version) {
            case (byte) 0:
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
                return Position.fromMap(position);
            default:
                throw new IllegalArgumentException(
                    "Unknown version " + version + " when deserializing Position"
                );
        }
    }

    private static Map<String, Map<Integer, Long>> deepCopy(
        final Map<String, Map<Integer, Long>> map) {
        if (map == null) {
            return new HashMap<>();
        } else {
            final Map<String, Map<Integer, Long>> copy = new HashMap<>(map.size());
            for (final Entry<String, Map<Integer, Long>> entry : map.entrySet()) {
                copy.put(entry.getKey(), new HashMap<>(entry.getValue()));
            }
            return copy;
        }
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
        final Position position1 = (Position) o;
        return Objects.equals(position, position1.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}

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


import org.apache.kafka.common.annotation.InterfaceStability.Evolving;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A representation of a position vector with respect to a set of topic partitions. For example, in
 * Interactive Query ({@link org.apache.kafka.streams.KafkaStreams#query(StateQueryRequest)}, a
 * query result may contain information from multiple store partitions, each of which contains
 * information from multiple input topics' partitions. This class can be used to summarize all of
 * that positional information.
 * <p>
 * This class is threadsafe, although it is mutable. Readers are recommended to use {@link
 * Position#copy()} to avoid seeing mutations to the Position after they get the reference. For
 * examples, when a store executes a {@link org.apache.kafka.streams.processor.StateStore#query(Query,
 * PositionBound, QueryConfig)} request and returns its current position via {@link
 * QueryResult#setPosition(Position)}, it should pass a copy of its position instead of the mutable
 * reference.
 */
@Evolving
public class Position {

    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> position;

    private Position(final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> position) {
        this.position = position;
    }

    /**
     * Create a new, empty Position.
     */
    public static Position emptyPosition() {
        return new Position(new ConcurrentHashMap<>());
    }

    /**
     * Create a new Position and populate it with a mapping of topic -&gt; partition -&gt; offset.
     * <p>
     * Note, the resulting Position does not share any structure with the provided map, so
     * subsequent changes to the map or Position will not affect the other.
     */
    public static Position fromMap(final Map<String, ? extends Map<Integer, Long>> map) {
        return new Position(deepCopy(map));
    }

    /**
     * Augment an existing Position by setting a new offset for a topic and partition.
     * <p>
     * Note: enforces monotonicity on offsets. I.e., if there is already a component for the same
     * topic and partition with a larger offset, the update will succeed but not overwrite the
     * offset.
     * <p>
     * Returns a self-reference for chained calls. Note: this method mutates the Position.
     */
    public Position withComponent(final String topic, final int partition, final long offset) {
        position
            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
            .compute(
                partition,
                (integer, prior) -> prior == null || offset > prior ? offset : prior
            );
        return this;
    }

    /**
     * Create a deep copy of the Position.
     */
    public Position copy() {
        return new Position(deepCopy(position));
    }

    /**
     * Merges the provided Position into the current instance.
     * <p>
     * If both Positions contain the same topic -&gt; partition -&gt; offset mapping, the resulting
     * Position will contain a mapping with the larger of the two offsets.
     */
    public Position merge(final Position other) {
        if (other == null) {
            return this;
        } else {
            for (final Entry<String, ConcurrentHashMap<Integer, Long>> entry : other.position.entrySet()) {
                final String topic = entry.getKey();
                final Map<Integer, Long> partitionMap =
                    position.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
                for (final Entry<Integer, Long> partitionOffset : entry.getValue().entrySet()) {
                    final Integer partition = partitionOffset.getKey();
                    final Long offset = partitionOffset.getValue();
                    if (!partitionMap.containsKey(partition)
                        || partitionMap.get(partition) < offset) {
                        partitionMap.put(partition, offset);
                    }
                }
            }
            return this;
        }
    }

    /**
     * Return the topics that are represented in this Position.
     */
    public Set<String> getTopics() {
        return Collections.unmodifiableSet(position.keySet());
    }

    /**
     * Return the partition -&gt; offset mapping for a specific topic.
     */
    public Map<Integer, Long> getPartitionPositions(final String topic) {
        final ConcurrentHashMap<Integer, Long> bound = position.get(topic);
        return bound == null ? Collections.emptyMap() : Collections.unmodifiableMap(bound);
    }

    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> deepCopy(
        final Map<String, ? extends Map<Integer, Long>> map) {
        if (map == null) {
            return new ConcurrentHashMap<>();
        } else {
            final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> copy =
                new ConcurrentHashMap<>(map.size());
            for (final Entry<String, ? extends Map<Integer, Long>> entry : map.entrySet()) {
                copy.put(entry.getKey(), new ConcurrentHashMap<>(entry.getValue()));
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
        throw new UnsupportedOperationException(
            "This mutable object is not suitable as a hash key");
    }

    public boolean isEmpty() {
        return position.isEmpty();
    }
}

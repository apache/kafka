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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * This class is a useful building block for doing fetch requests where topic partitions have to be rotated via
 * round-robin to ensure fairness and some level of determinism given the existence of a limit on the fetch response
 * size. Because the serialization of fetch requests is more efficient if all partitions for the same topic are grouped
 * together, we do such grouping in the method `set`.
 *
 * As partitions are moved to the end, the same topic may be repeated more than once. In the optimal case, a single
 * topic would "wrap around" and appear twice. However, as partitions are fetched in different orders and partition
 * leadership changes, we will deviate from the optimal. If this turns out to be an issue in practice, we can improve
 * it by tracking the partitions per node or calling `set` every so often.
 *
 * Note that this class is not thread-safe with the exception of {@link #size()} which returns the number of
 * partitions currently tracked.
 */
public class PartitionStates<S> {

    // The order is kept by linking our own nodes rather than by a LinkedHashMap. Moving a partition to the end then
    // reuses its node. With LinkedHashMap, remove + put leaves the removed entry pointing at the next entry of its
    // bucket, so two partitions of one bucket updated in turn link every removed entry to the next one; once any of
    // them is promoted to the old generation, young GC keeps copying all the entries that follow it.
    private static final class Node<S> implements Map.Entry<TopicPartition, S> {
        private final TopicPartition topicPartition;
        private S state;
        private Node<S> prev;
        private Node<S> next;

        private Node(TopicPartition topicPartition, S state) {
            this.topicPartition = topicPartition;
            this.state = state;
        }

        @Override
        public TopicPartition getKey() {
            return topicPartition;
        }

        @Override
        public S getValue() {
            return state;
        }

        @Override
        public S setValue(S value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            return topicPartition.equals(e.getKey()) && Objects.equals(state, e.getValue());
        }

        @Override
        public int hashCode() {
            return topicPartition.hashCode() ^ Objects.hashCode(state);
        }
    }

    private final Map<TopicPartition, Node<S>> nodes = new HashMap<>();
    private final Set<TopicPartition> partitionSetView = Collections.unmodifiableSet(nodes.keySet());
    private final Map<TopicPartition, S> partitionStateMapView = new StateMapView();
    private Node<S> head;
    private Node<S> tail;

    /* the number of partitions that are currently assigned available in a thread safe manner */
    private volatile int size = 0;

    public PartitionStates() {}

    public void moveToEnd(TopicPartition topicPartition) {
        Node<S> node = nodes.get(topicPartition);
        if (node != null)
            moveNodeToEnd(node);
    }

    public void updateAndMoveToEnd(TopicPartition topicPartition, S state) {
        Node<S> node = nodes.get(topicPartition);
        if (node == null) {
            addLast(topicPartition, state);
        } else {
            node.state = state;
            moveNodeToEnd(node);
        }
        updateSize();
    }

    public void update(TopicPartition topicPartition, S state) {
        Node<S> node = nodes.get(topicPartition);
        if (node == null)
            addLast(topicPartition, state);
        else
            node.state = state;
        updateSize();
    }

    public void remove(TopicPartition topicPartition) {
        Node<S> node = nodes.remove(topicPartition);
        if (node != null)
            unlink(node);
        updateSize();
    }

    /**
     * Returns an unmodifiable view of the partitions in random order.
     * changes to this PartitionStates instance will be reflected in this view.
     */
    public Set<TopicPartition> partitionSet() {
        return partitionSetView;
    }

    public void clear() {
        while (head != null)
            unlink(head);
        nodes.clear();
        updateSize();
    }

    public boolean contains(TopicPartition topicPartition) {
        return nodes.containsKey(topicPartition);
    }

    public Iterator<S> stateIterator() {
        return new Iterator<>() {
            private Node<S> next = head;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public S next() {
                if (next == null)
                    throw new NoSuchElementException();
                S state = next.state;
                next = next.next;
                return state;
            }
        };
    }

    public void forEach(BiConsumer<TopicPartition, S> biConsumer) {
        for (Node<S> node = head; node != null; node = node.next)
            biConsumer.accept(node.topicPartition, node.state);
    }

    public Map<TopicPartition, S> partitionStateMap() {
        return partitionStateMapView;
    }

    /**
     * Returns the partition state values in order.
     */
    public List<S> partitionStateValues() {
        List<S> values = new ArrayList<>(nodes.size());
        for (Node<S> node = head; node != null; node = node.next)
            values.add(node.state);
        return values;
    }

    public S stateValue(TopicPartition topicPartition) {
        Node<S> node = nodes.get(topicPartition);
        return node == null ? null : node.state;
    }

    /**
     * Get the number of partitions that are currently being tracked. This is thread-safe.
     */
    public int size() {
        return size;
    }

    /**
     * Update the builder to have the received map as its state (i.e. the previous state is cleared). The builder will
     * "batch by topic", so if we have a, b and c, each with two partitions, we may end up with something like the
     * following (the order of topics and partitions within topics is dependent on the iteration order of the received
     * map): a0, a1, b1, b0, c0, c1.
     */
    public void set(Map<TopicPartition, S> partitionToState) {
        clear();
        update(partitionToState);
        updateSize();
    }

    private void updateSize() {
        size = nodes.size();
    }

    private void update(Map<TopicPartition, S> partitionToState) {
        LinkedHashMap<String, List<TopicPartition>> topicToPartitions = new LinkedHashMap<>();
        for (TopicPartition tp : partitionToState.keySet()) {
            List<TopicPartition> partitions = topicToPartitions.computeIfAbsent(tp.topic(), k -> new ArrayList<>());
            partitions.add(tp);
        }
        for (Map.Entry<String, List<TopicPartition>> entry : topicToPartitions.entrySet()) {
            for (TopicPartition tp : entry.getValue()) {
                S state = partitionToState.get(tp);
                update(tp, state);
            }
        }
    }

    private void addLast(TopicPartition topicPartition, S state) {
        Node<S> node = new Node<>(topicPartition, state);
        nodes.put(topicPartition, node);
        linkLast(node);
    }

    private void moveNodeToEnd(Node<S> node) {
        if (node != tail) {
            unlink(node);
            linkLast(node);
        }
    }

    private void linkLast(Node<S> node) {
        node.prev = tail;
        if (tail == null)
            head = node;
        else
            tail.next = node;
        tail = node;
    }

    private void unlink(Node<S> node) {
        if (node.prev == null)
            head = node.next;
        else
            node.prev.next = node.next;
        if (node.next == null)
            tail = node.prev;
        else
            node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    // An unmodifiable view in partition order that reflects later changes, like Collections.unmodifiableMap over a
    // LinkedHashMap.
    private final class StateMapView extends AbstractMap<TopicPartition, S> {
        private final Set<Map.Entry<TopicPartition, S>> entries = new AbstractSet<>() {
            @Override
            public Iterator<Map.Entry<TopicPartition, S>> iterator() {
                return new Iterator<>() {
                    private Node<S> next = head;

                    @Override
                    public boolean hasNext() {
                        return next != null;
                    }

                    @Override
                    public Map.Entry<TopicPartition, S> next() {
                        if (next == null)
                            throw new NoSuchElementException();
                        Node<S> node = next;
                        next = next.next;
                        return node;
                    }
                };
            }

            @Override
            public int size() {
                return nodes.size();
            }
        };

        @Override
        public Set<Map.Entry<TopicPartition, S>> entrySet() {
            return entries;
        }

        @Override
        public int size() {
            return nodes.size();
        }

        @Override
        public boolean containsKey(Object key) {
            return nodes.containsKey(key);
        }

        @Override
        public S get(Object key) {
            Node<S> node = nodes.get(key);
            return node == null ? null : node.state;
        }

        @Override
        public void forEach(BiConsumer<? super TopicPartition, ? super S> action) {
            for (Node<S> node = head; node != null; node = node.next)
                action.accept(node.topicPartition, node.state);
        }
    }
}

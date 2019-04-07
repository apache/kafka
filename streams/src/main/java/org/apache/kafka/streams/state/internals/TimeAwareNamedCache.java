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

import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;
import java.util.ArrayList;
import java.util.HashMap;

import static java.lang.Math.toIntExact;

class TimeAwareNamedCache extends NamedCache {
    private static final Logger log = LoggerFactory.getLogger(TimeAwareNamedCache.class);
    public static final int WHEEL_COUNT = 8; // Each wheel represents 8 bits of the long integer

    private final HiearchalWheel[] wheels;
    private final HashMap<Bytes, Integer> wheelLocation;

    public TimeAwareNamedCache(final String name, final StreamsMetricsImpl metrics) {
        super(name, metrics);
        this.wheels = new HiearchalWheel[WHEEL_COUNT];
        this.wheelLocation = new HashMap<>();
        for (int i = 0; i < WHEEL_COUNT; i++) {
            wheels[i] = new HiearchalWheel(8 * i); // 8 * i: bits by which a number needs to be shifted
        }
    }

    private int findShift(final long remainingMs) {
        for (int i = 0; i < 8; i++) {
            if (remainingMs >> (8 * i) > 0) {
                continue;
            } else {
                return i - 1;
            }
        }
        return 8;
    }

    synchronized void put(final Bytes key, final LRUCacheEntry value, final Timer timer) {
        super.put(key, value);
        final int shift = findShift(timer.remainingMs());
        wheelLocation.put(key, shift);
        wheels[shift].put(new HiearchalWheelNode(key, timer));
    }

    synchronized LRUCacheEntry putIfAbsent(final Bytes key, 
                                           final LRUCacheEntry value, 
                                           final Timer timer) {
        final Integer location = wheelLocation.get(key);
        if (location == null) {
            put(key, value, timer);
        }
        return value;
    }

    private int evictNodesInWheels() {
        int nodesRemoved = 0;
        for (int i = 0; i < WHEEL_COUNT; i++) {
            final HiearchalWheelNode[] arr = wheels[i].evictNodes();
            for (final HiearchalWheelNode node : arr) {
                if (node.timer.isExpired()) {
                    super.delete(node.key); // node's lifetime has expired
                    nodesRemoved++;
                } else {
                    final int shift = findShift(node.timer.remainingMs());
                    wheelLocation.put(node.key, shift);
                    wheels[shift].put(node);
                }
            }
        }
        return nodesRemoved;
    }

    @Override
    synchronized void evict() {
        final int removed = evictNodesInWheels();
        if (removed == 0) {
            super.evict();
        }
    }

    @Override
    synchronized LRUCacheEntry delete(final Bytes key) {
        final LRUCacheEntry entry = super.delete(key);
        final Integer location = wheelLocation.get(key);
        if (location != null) {
            wheels[location].setEvicted(key);
        }
        return entry;
    }

    @Override
    synchronized void close() {
        super.close();
        wheelLocation.clear();
        for (int i = 0; i < WHEEL_COUNT; i++) {
            wheels[i].clear();
        }
    }

    /**
     * A simple class to store information relevant to the LRUCacheEntry being
     * stored in the wheel.
     */
    private static class HiearchalWheelNode {
        public final Bytes key;
        public final Timer timer;

        public HiearchalWheelNode(final Bytes key, final Timer timer) {
            this.key = key;
            this.timer = timer;
        }

        @Override
        public int hashCode() {
            return key.hashCode() * 103 + timer.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof HiearchalWheelNode)) {
                return false;
            }
            final HiearchalWheelNode node = (HiearchalWheelNode) o;
            return key.equals(node.key) && timer.equals(node.timer);
        }
    }

    private static class HiearchalWheel {
        public static final int WHEEL_SIZE = 1 << 8;

        private final ArrayList<Stack<HiearchalWheelNode>> timeSlots;
        private final HashMap<Bytes, Boolean> evicted;
        private final Timer timer;
        private long lastChecked;
        private long elapsed;
        private long remainder;
        private int index;
        private final long interval;
        private final int shift;

        public HiearchalWheel(final int bitShift) {
            this.timeSlots = new ArrayList<>(WHEEL_SIZE);
            for (int i = 0; i < WHEEL_SIZE; i++) {
                timeSlots.set(i, new Stack<HiearchalWheelNode>());
            }
            this.evicted = new HashMap<>();
            this.timer = Time.SYSTEM.timer(Long.MAX_VALUE);
            this.lastChecked = 0;
            this.elapsed = 0;
            this.remainder = 0;
            this.index = 0;
            this.interval = 1 << bitShift;
            this.shift = bitShift;
        }

        public void put(final HiearchalWheelNode node) {
            final long timeRemaining = timer.remainingMs();
            final long index = (timeRemaining >> shift + this.index) % WHEEL_SIZE;
            timeSlots.get(toIntExact(index)).push(node);
            evicted.put(node.key, false);
        }

        public HiearchalWheelNode[] evictNodes() {
            final long timeSinceLastEviction = timer.elapsedMs() - lastChecked;
            lastChecked = timer.currentTimeMs();
            elapsed = timeSinceLastEviction + remainder;
            final long rotations = elapsed / interval;
            remainder = elapsed % interval;

            final ArrayList<HiearchalWheelNode> nodes = new ArrayList<HiearchalWheelNode>();
            for (int i = 0, j = index; i < rotations; i++, j = (index + i) % WHEEL_SIZE) {
                if (i == WHEEL_SIZE) {
                    break;
                }
                while (!timeSlots.get(j).isEmpty()) {
                    final HiearchalWheelNode node = timeSlots.get(j).pop();
                    final Boolean result = evicted.get(node.key);
                    if (result != null && !result) {
                        nodes.add(node);
                    }
                }
            }
            index = toIntExact((rotations + index) % WHEEL_SIZE);
            return nodes.toArray(new HiearchalWheelNode[nodes.size()]);
        }

        public void setEvicted(final Bytes key) {
            evicted.put(key, true);
        }

        public void clear() {
            timeSlots.clear();
            evicted.clear();
        }
    }
}

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

class TimeAwareNamedCache {
    private static final Logger log = LoggerFactory.getLogger(TimeAwareNamedCache.class);
    public static final int WHEEL_COUNT = 8; // Each wheel represents 8 bits of the long integer

    private final NamedCache cache;
    private final HiearchalWheel[] wheels;

    public TimeAwareNamedCache(final String name, final StreamsMetricsImpl metrics) {
        this.cache = new NamedCache(name, metrics);
        this.wheels = new HiearchalWheel[WHEEL_COUNT];
        for (int i = 0; i < WHEEL_COUNT; i++) {
            wheels[i] = new HiearchalWheel(8 * i); // 8 * i: bits by which a number needs to be shifted
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
        public static final int BASE_MASK_INTERVAL = 1 << 9 - 1;

        private final ArrayList<Stack<HiearchalWheelNode>> timeSlots;
        private final HashMap<HiearchalWheelNode, Boolean> evicted;
        private final Timer timer;
        private long lastChecked;
        private long elapsed;
        private long remainder;
        private int index;
        private final long interval;
        private final int shift;
        private final long mask;

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
            this.mask = BASE_MASK_INTERVAL << bitShift;
            this.interval = 1 << bitShift;
            this.shift = bitShift;
        }

        public void put(final HiearchalWheelNode node) {
            final long timeRemaining = timer.remainingMs();
            final long index = (timeRemaining & mask) >> shift;
            timeSlots.get((int) index).push(node);
            evicted.put(node, false);
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
                    final Boolean result = evicted.get(node);
                    if (result != null && !result) {
                        nodes.add(node);
                    }
                }
            }
            index = (int) ((rotations + index) % WHEEL_SIZE);
            return nodes.toArray(new HiearchalWheelNode[nodes.size()]);
        }

        public void setEvicted(final HiearchalWheelNode node) {
            evicted.put(node, true);
        }
    }
}

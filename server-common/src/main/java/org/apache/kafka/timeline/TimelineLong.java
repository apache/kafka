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

package org.apache.kafka.timeline;

import java.util.Iterator;


/**
 * This is a mutable long which can be snapshotted.
 * <br>
 * This class requires external synchronization.
 */
public final class TimelineLong implements Revertable {
    public static final long INIT = 0;

    static class LongContainer implements Delta {
        private long value = INIT;

        long value() {
            return value;
        }

        void setValue(long value) {
            this.value = value;
        }

        @Override
        public void mergeFrom(long destinationEpoch, Delta delta) {
            // Nothing to do
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private long value;

    public TimelineLong(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = snapshotRegistry;
        this.value = INIT;

        snapshotRegistry.register(this);
    }

    public long get() {
        return value;
    }

    public long get(long epoch) {
        if (epoch == SnapshotRegistry.LATEST_EPOCH) return value;
        Iterator<Snapshot> iterator = snapshotRegistry.iterator(epoch);
        while (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            LongContainer container = snapshot.getDelta(TimelineLong.this);
            if (container != null) return container.value();
        }
        return value;
    }

    public void set(long newValue) {
        Iterator<Snapshot> iterator = snapshotRegistry.reverseIterator();
        if (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            LongContainer prevContainer = snapshot.getDelta(TimelineLong.this);
            if (prevContainer == null) {
                prevContainer = new LongContainer();
                snapshot.setDelta(TimelineLong.this, prevContainer);
                prevContainer.setValue(value);
            }
        }
        this.value = newValue;
    }

    public void increment() {
        set(get() + 1L);
    }

    public void decrement() {
        set(get() - 1L);
    }

    @Override
    public void executeRevert(long targetEpoch, Delta delta) {
        LongContainer container = (LongContainer) delta;
        this.value = container.value();
    }

    @Override
    public void reset() {
        set(INIT);
    }

    @Override
    public int hashCode() {
        return ((int) value) ^ (int) (value >>> 32);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TimelineLong)) return false;
        TimelineLong other = (TimelineLong) o;
        return value == other.value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }
}

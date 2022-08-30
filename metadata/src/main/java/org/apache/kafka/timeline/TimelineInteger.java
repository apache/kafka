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
 * This is a mutable integer which can be snapshotted. 
 *
 * This class requires external synchronization.
 */
public class TimelineInteger implements Revertable {
    public static final int INIT = 0;

    static class IntegerContainer implements Delta {
        private int value = INIT;

        int value() {
            return value;
        }

        void setValue(int value) {
            this.value = value;
        }

        @Override
        public void mergeFrom(long destinationEpoch, Delta delta) {
            // Nothing to do
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private int value;

    public TimelineInteger(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = snapshotRegistry;
        this.value = INIT;

        snapshotRegistry.register(this);
    }

    public int get() {
        return value;
    }

    public int get(long epoch) {
        if (epoch == SnapshotRegistry.LATEST_EPOCH) return value;
        Iterator<Snapshot> iterator = snapshotRegistry.iterator(epoch);
        while (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            IntegerContainer container = snapshot.getDelta(TimelineInteger.this);
            if (container != null) return container.value();
        }
        return value;
    }

    public void set(int newValue) {
        Iterator<Snapshot> iterator = snapshotRegistry.reverseIterator();
        if (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            IntegerContainer container = snapshot.getDelta(TimelineInteger.this);
            if (container == null) {
                container = new IntegerContainer();
                snapshot.setDelta(TimelineInteger.this, container);
                container.setValue(value);
            }
        }
        this.value = newValue;
    }

    public void increment() {
        set(get() + 1);
    }

    public void decrement() {
        set(get() - 1);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void executeRevert(long targetEpoch, Delta delta) {
        IntegerContainer container = (IntegerContainer) delta;
        this.value = container.value;
    }

    @Override
    public void reset() {
        set(INIT);
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TimelineInteger)) return false;
        TimelineInteger other = (TimelineInteger) o;
        return value == other.value;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }
}

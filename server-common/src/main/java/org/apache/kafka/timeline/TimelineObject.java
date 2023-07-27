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
import java.util.Objects;


/**
 * This is a mutable reference to an immutable object. It can be snapshotted.
 *
 * This class requires external synchronization.
 */
public class TimelineObject<T> implements Revertable {
    static class ObjectContainer<T> implements Delta {
        private T value;

        ObjectContainer(T initialValue) {
            this.value = initialValue;
        }

        T value() {
            return value;
        }

        void setValue(T value) {
            this.value = value;
        }

        @Override
        public void mergeFrom(long destinationEpoch, Delta delta) {
            // Nothing to do
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private final T initialValue;
    private T value;

    public TimelineObject(SnapshotRegistry snapshotRegistry, T initialValue) {
        Objects.requireNonNull(initialValue);
        this.snapshotRegistry = snapshotRegistry;
        this.initialValue = initialValue;
        this.value = initialValue;
        snapshotRegistry.register(this);
    }

    public T get() {
        return value;
    }

    public T get(long epoch) {
        if (epoch == SnapshotRegistry.LATEST_EPOCH) return value;
        Iterator<Snapshot> iterator = snapshotRegistry.iterator(epoch);
        while (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            ObjectContainer<T> container = snapshot.getDelta(TimelineObject.this);
            if (container != null) return container.value();
        }
        return value;
    }

    public void set(T newValue) {
        Objects.requireNonNull(newValue);
        Iterator<Snapshot> iterator = snapshotRegistry.reverseIterator();
        if (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            ObjectContainer<T> prevContainer = snapshot.getDelta(TimelineObject.this);
            if (prevContainer == null) {
                prevContainer = new ObjectContainer<>(initialValue);
                snapshot.setDelta(TimelineObject.this, prevContainer);
                prevContainer.setValue(value);
            }
        }
        this.value = newValue;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void executeRevert(long targetEpoch, Delta delta) {
        ObjectContainer<T> container = (ObjectContainer<T>) delta;
        this.value = container.value();
    }

    @Override
    public void reset() {
        set(initialValue);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TimelineObject)) return false;
        TimelineObject other = (TimelineObject) o;
        return value.equals(other.value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}

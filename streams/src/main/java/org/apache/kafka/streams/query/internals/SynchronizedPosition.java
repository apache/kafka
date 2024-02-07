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
package org.apache.kafka.streams.query.internals;

import org.apache.kafka.streams.query.Position;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class SynchronizedPosition extends Position {
    private final ReentrantLock lock = new ReentrantLock();

    public SynchronizedPosition(final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> position) {
        super(position);
    }

    public static SynchronizedPosition emptyPosition() {
        return new SynchronizedPosition(new ConcurrentHashMap<>());
    }

    public static SynchronizedPosition fromMap(final Map<String, ? extends Map<Integer, Long>> map) {
        return new SynchronizedPosition(deepCopy(map));
    }

    public static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> deepCopy(
        final Map<String, ? extends Map<Integer, Long>> map) {
        if (map == null) {
            return new ConcurrentHashMap<>();
        } else {
            final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> copy =
                new ConcurrentHashMap<>(map.size());
            for (final Map.Entry<String, ? extends Map<Integer, Long>> entry : map.entrySet()) {
                copy.put(entry.getKey(), new ConcurrentHashMap<>(entry.getValue()));
            }
            return copy;
        }
    }


    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }
    public Position withComponent(final String topic, final int partition, final long offset) {
        verifyLock();
        return super.withComponent(topic, partition, offset);
    }

    public Position copy() {
        verifyLock();
        return super.copy();
    }

    public Position merge(final SynchronizedPosition other) {
        verifyLock();
        return super.merge(other);
    }

    // need to overwrite this to make Spotbugs happy
    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    // need to overwrite this to make Spotbugs happy
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    private void verifyLock() {
        if (!lock.isLocked()) {
            throw new IllegalStateException("SynchronizedPosition must be locked before usage");
        }
    }
}

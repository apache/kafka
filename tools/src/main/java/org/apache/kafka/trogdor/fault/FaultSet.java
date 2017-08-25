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

package org.apache.kafka.trogdor.fault;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public class FaultSet {
    private final static long NS_PER_MS = 1000000L;

    /**
     * Maps fault start times in nanoseconds to faults.
     */
    private final TreeMap<Long, Fault> byStart = new TreeMap<Long, Fault>();

    /**
     * Maps fault end times in nanoseconds to faults.
     */
    private final TreeMap<Long, Fault> byEnd = new TreeMap<Long, Fault>();

    /**
     * Return an iterator that iterates over the fault set in start time order.
     */
    public FaultSetIterator iterateByStart() {
        return new FaultSetIterator(byStart);
    }

    /**
     * Return an iterator that iterates over the fault set in end time order.
     */
    public FaultSetIterator iterateByEnd() {
        return new FaultSetIterator(byEnd);
    }

    /**
     * Add a new fault to the FaultSet.
     */
    public void add(Fault fault) {
        insertUnique(byStart, fault.spec().startMs() * NS_PER_MS, fault);
        long endMs = fault.spec().startMs() + fault.spec().durationMs();
        insertUnique(byEnd, endMs * NS_PER_MS, fault);
    }

    /**
     * Insert a new fault to a TreeMap.
     *
     * If there is already a fault with the given key, the fault will be stored
     * with the next available key.
     */
    private void insertUnique(TreeMap<Long, Fault> map, long key, Fault fault) {
        while (true) {
            Fault existing = map.get(key);
            if (existing == null) {
                map.put(key, fault);
                return;
            } else if (existing == fault) {
                return;
            } else {
                key++;
            }
        }
    }

    /**
     * Remove a fault from the TreeMap.  The fault is removed by object equality.
     */
    public void remove(Fault fault) {
        removeUnique(byStart, fault.spec().startMs() * NS_PER_MS, fault);
        long endMs = fault.spec().startMs() + fault.spec().durationMs();
        removeUnique(byEnd, endMs * NS_PER_MS, fault);
    }

    /**
     * Helper function to remove a fault from a map.  We will search every
     * element of the map equal to or higher than the given key.
     */
    private void removeUnique(TreeMap<Long, Fault> map, long key, Fault fault) {
        while (true) {
            Map.Entry<Long, Fault> existing = map.ceilingEntry(key);
            if (existing == null) {
                throw new NoSuchElementException("No such element as " + fault);
            } else if (existing.getValue() == fault) {
                map.remove(existing.getKey());
                return;
            } else {
                key = existing.getKey() + 1;
            }
        }
    }

    /**
     * An iterator over the FaultSet.
     */
    class FaultSetIterator implements Iterator<Fault> {
        private final TreeMap<Long, Fault> map;
        private Fault cur = null;
        private long prevKey = -1;

        FaultSetIterator(TreeMap<Long, Fault> map) {
            this.map = map;
        }

        @Override
        public boolean hasNext() {
            Map.Entry<Long, Fault> entry = map.higherEntry(prevKey);
            return entry != null;
        }

        @Override
        public Fault next() {
            Map.Entry<Long, Fault> entry = map.higherEntry(prevKey);
            if (entry == null) {
                throw new NoSuchElementException();
            }
            prevKey = entry.getKey();
            cur = entry.getValue();
            return cur;
        }

        @Override
        public void remove() {
            if (cur == null) {
                throw new IllegalStateException();
            }
            FaultSet.this.remove(cur);
            cur = null;
        }
    }
};

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * SnapshottableHashTable implements a hash table that supports creating point-in-time
 * snapshots.  Each snapshot is immutable once it is created; the past cannot be changed.
 * We handle divergences between the current state and historical state by copying a
 * reference to elements that have been deleted or overwritten into the most recent
 * snapshot tier.
 * <br>
 * Note that there are no keys in SnapshottableHashTable, only values.  So it more similar
 * to a hash set than a hash map.  The subclasses implement full-featured maps and sets
 * using this class as a building block.
 * <br>
 * Each snapshot tier contains a size and a hash table.  The size reflects the size at
 * the time the snapshot was taken.  Note that, as an optimization, snapshot tiers will
 * be null if they don't contain anything.  So for example, if snapshot 20 of Object O
 * contains the same entries as snapshot 10 of that object, the snapshot 20 tier for
 * object O will be null.
 * <br>
 * The current tier's data is stored in the fields inherited from BaseHashTable.  It
 * would be conceptually simpler to have a separate BaseHashTable object, but since Java
 * doesn't have value types, subclassing is the only way to avoid another pointer
 * indirection and the associated extra memory cost.
 * <br>
 * Note that each element in the hash table contains a start epoch, and a value.  The
 * start epoch is there to identify when the object was first inserted.  This in turn
 * determines which snapshots it is a member of.
 * <br>
 * In order to retrieve an object from snapshot E, we start by checking to see if the
 * object exists in the "current" hash tier.  If it does, and its startEpoch extends back
 * to E, we return that object.  Otherwise, we check all the snapshot tiers, starting
 * with E, and ending with the most recent snapshot, to see if the object is there.
 * As an optimization, if we encounter the object in a snapshot tier but its epoch is too
 * new, we know that its value at epoch E must be null, so we can return that immediately.
 * <br>
 * The class hierarchy looks like this:
 * <pre>
 *        Revertable       BaseHashTable
 *              ↑              ↑
 *           SnapshottableHashTable → SnapshotRegistry → Snapshot
 *               ↑             ↑
 *   TimelineHashSet       TimelineHashMap
 * </pre>
 * BaseHashTable is a simple hash table that uses separate chaining.  The interface is
 * pretty bare-bones since this class is not intended to be used directly by end-users.
 * <br>
 * This class, SnapshottableHashTable, has the logic for snapshotting and iterating over
 * snapshots.  This is the core of the snapshotted hash table code and handles the
 * tiering.
 * <br>
 * TimelineHashSet and TimelineHashMap are mostly wrappers around this
 * SnapshottableHashTable class.  They implement standard Java APIs for Set and Map,
 * respectively.  There's a fair amount of boilerplate for this, but it's necessary so
 * that timeline data structures can be used while writing idiomatic Java code.
 * The accessor APIs have two versions -- one that looks at the current state, and one
 * that looks at a historical snapshotted state.  Mutation APIs only ever mutate the
 * current state.
 * <br>
 * One very important feature of SnapshottableHashTable is that we support iterating
 * over a snapshot even while changes are being made to the current state.  See the
 * Javadoc for the iterator for more information about how this is accomplished.
 * <br>
 * All of these classes require external synchronization, and don't support null keys or
 * values.
 */
class SnapshottableHashTable<T extends SnapshottableHashTable.ElementWithStartEpoch>
        extends BaseHashTable<T> implements Revertable {

    /**
     * A special epoch value that represents the latest data.
     */
    final static long LATEST_EPOCH = Long.MAX_VALUE;

    interface ElementWithStartEpoch {
        void setStartEpoch(long startEpoch);
        long startEpoch();
    }

    static class HashTier<T extends SnapshottableHashTable.ElementWithStartEpoch> implements Delta {
        private final int size;
        private BaseHashTable<T> deltaTable;

        HashTier(int size) {
            this.size = size;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void mergeFrom(long epoch, Delta source) {
            HashTier<T> other = (HashTier<T>) source;
            // As an optimization, the deltaTable might not exist for a new key
            // as there is no previous value
            if (other.deltaTable != null) {
                List<T> list = new ArrayList<>();
                Object[] otherElements = other.deltaTable.baseElements();
                for (int slot = 0; slot < otherElements.length; slot++) {
                    BaseHashTable.unpackSlot(list, otherElements, slot);
                    for (T element : list) {
                        // When merging in a later hash tier, we want to keep only the elements
                        // that were present at our epoch.
                        if (element.startEpoch() <= epoch) {
                            if (deltaTable == null) {
                                deltaTable = new BaseHashTable<>(1);
                            }
                            deltaTable.baseAddOrReplace(element);
                        }
                    }
                }
            }
        }
    }

    /**
     * Iterate over the values that currently exist in the hash table.
     *
     * You can use this iterator even if you are making changes to the map.
     * The changes may or may not be visible while you are iterating.
     */
    class CurrentIterator implements Iterator<T> {
        private final Object[] topTier;
        private final List<T> ready;
        private int slot;
        private T lastReturned;

        CurrentIterator(Object[] topTier) {
            this.topTier = topTier;
            this.ready = new ArrayList<>();
            this.slot = 0;
            this.lastReturned = null;
        }

        @Override
        public boolean hasNext() {
            while (ready.isEmpty()) {
                if (slot == topTier.length) {
                    return false;
                }
                BaseHashTable.unpackSlot(ready, topTier, slot);
                slot++;
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            lastReturned = ready.remove(ready.size() - 1);
            return lastReturned;
        }

        @Override
        public void remove() {
            if (lastReturned == null) {
                throw new UnsupportedOperationException("remove");
            }
            snapshottableRemove(lastReturned);
            lastReturned = null;
        }
    }

    /**
     * Iterate over the values that existed in the hash table during a specific snapshot.
     *
     * You can use this iterator even if you are making changes to the map.
     * The snapshot is immutable and will always show up the same.
     */
    class HistoricalIterator implements Iterator<T> {
        private final Object[] topTier;
        private final Snapshot snapshot;
        private final List<T> temp;
        private final List<T> ready;
        private int slot;

        HistoricalIterator(Object[] topTier, Snapshot snapshot) {
            this.topTier = topTier;
            this.snapshot = snapshot;
            this.temp = new ArrayList<>();
            this.ready = new ArrayList<>();
            this.slot = 0;
        }

        @Override
        public boolean hasNext() {
            while (ready.isEmpty()) {
                if (slot == topTier.length) {
                    return false;
                }
                BaseHashTable.unpackSlot(temp, topTier, slot);
                for (T object : temp) {
                    if (object.startEpoch() <= snapshot.epoch()) {
                        ready.add(object);
                    }
                }
                temp.clear();

                /*
                 * As we iterate over the SnapshottableHashTable, elements may move from
                 * the top tier into the snapshot tiers.  This would happen if something
                 * were deleted in the top tier, for example, but still retained in the
                 * snapshot.
                 *
                 * We don't want to return any elements twice, though.  Therefore, we
                 * iterate over the top tier and the snapshot tier at the
                 * same time.  The key to understanding how this works is realizing that
                 * both hash tables use the same hash function, but simply choose a
                 * different number of significant bits based on their size.
                 * So if the top tier has size 4 and the snapshot tier has size 2, we have
                 * the following mapping:
                 *
                 * Elements that would be in slot 0 or 1 in the top tier can only be in
                 * slot 0 in the snapshot tier.
                 * Elements that would be in slot 2 or 3 in the top tier can only be in
                 * slot 1 in the snapshot tier.
                 *
                 * Therefore, we can do something like this:
                 * 1. check slot 0 in the top tier and slot 0 in the snapshot tier.
                 * 2. check slot 1 in the top tier and slot 0 in the snapshot tier.
                 * 3. check slot 2 in the top tier and slot 1 in the snapshot tier.
                 * 4. check slot 3 in the top tier and slot 1 in the snapshot tier.
                 *
                 * If elements move from the top tier to the snapshot tier, then
                 * we'll still find them and report them exactly once.
                 *
                 * Note that while I used 4 and 2 as example sizes here, the same pattern
                 * holds for different powers of two.  The "snapshot slot" of an element
                 * will be the top few bits of the top tier slot of that element.
                 */
                Iterator<Snapshot> iterator = snapshotRegistry.iterator(snapshot);
                while (iterator.hasNext()) {
                    Snapshot curSnapshot = iterator.next();
                    HashTier<T> tier = curSnapshot.getDelta(SnapshottableHashTable.this);
                    if (tier != null && tier.deltaTable != null) {
                        BaseHashTable<T> deltaTable = tier.deltaTable;
                        int shift = Integer.numberOfLeadingZeros(deltaTable.baseElements().length) -
                            Integer.numberOfLeadingZeros(topTier.length);
                        int tierSlot = slot >>> shift;
                        BaseHashTable.unpackSlot(temp, deltaTable.baseElements(), tierSlot);
                        for (T object : temp) {
                            if (object.startEpoch() <= snapshot.epoch()) {
                                if (BaseHashTable.findSlot(object, topTier.length) == slot) {
                                    ready.add(object);
                                }
                            }
                        }
                        temp.clear();
                    }
                }
                slot++;
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return ready.remove(ready.size() - 1);
        }
    }

    private final SnapshotRegistry snapshotRegistry;

    SnapshottableHashTable(SnapshotRegistry snapshotRegistry, int expectedSize) {
        super(expectedSize);
        this.snapshotRegistry = snapshotRegistry;
        snapshotRegistry.register(this);
    }

    int snapshottableSize(long epoch) {
        if (epoch == LATEST_EPOCH) {
            return baseSize();
        } else {
            Iterator<Snapshot> iterator = snapshotRegistry.iterator(epoch);
            while (iterator.hasNext()) {
                Snapshot snapshot = iterator.next();
                HashTier<T> tier = snapshot.getDelta(SnapshottableHashTable.this);
                if (tier != null) {
                    return tier.size;
                }
            }
            return baseSize();
        }
    }

    T snapshottableGet(Object key, long epoch) {
        T result = baseGet(key);
        if (result != null && result.startEpoch() <= epoch) {
            return result;
        }
        if (epoch == LATEST_EPOCH) {
            return null;
        }
        Iterator<Snapshot> iterator = snapshotRegistry.iterator(epoch);
        while (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            HashTier<T> tier = snapshot.getDelta(SnapshottableHashTable.this);
            if (tier != null && tier.deltaTable != null) {
                result = tier.deltaTable.baseGet(key);
                if (result != null) {
                    if (result.startEpoch() <= epoch) {
                        return result;
                    } else {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    boolean snapshottableAddUnlessPresent(T object) {
        T prev = baseGet(object);
        if (prev != null) {
            return false;
        }
        object.setStartEpoch(snapshotRegistry.latestEpoch() + 1);
        int prevSize = baseSize();
        baseAddOrReplace(object);
        updateTierData(prevSize);
        return true;
    }

    T snapshottableAddOrReplace(T object) {
        object.setStartEpoch(snapshotRegistry.latestEpoch() + 1);
        int prevSize = baseSize();
        T prev = baseAddOrReplace(object);
        if (prev == null) {
            updateTierData(prevSize);
        } else {
            updateTierData(prev, prevSize);
        }
        return prev;
    }

    T snapshottableRemove(Object object) {
        T prev = baseRemove(object);
        if (prev == null) {
            return null;
        } else {
            updateTierData(prev, baseSize() + 1);
            return prev;
        }
    }

    private void updateTierData(int prevSize) {
        Iterator<Snapshot> iterator = snapshotRegistry.reverseIterator();
        if (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            HashTier<T> tier = snapshot.getDelta(SnapshottableHashTable.this);
            if (tier == null) {
                tier = new HashTier<>(prevSize);
                snapshot.setDelta(SnapshottableHashTable.this, tier);
            }
        }
    }

    private void updateTierData(T prev, int prevSize) {
        Iterator<Snapshot> iterator = snapshotRegistry.reverseIterator();
        if (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            // If the previous element was present in the most recent snapshot, add it to
            // that snapshot's hash tier.
            if (prev.startEpoch() <= snapshot.epoch()) {
                HashTier<T> tier = snapshot.getDelta(SnapshottableHashTable.this);
                if (tier == null) {
                    tier = new HashTier<>(prevSize);
                    snapshot.setDelta(SnapshottableHashTable.this, tier);
                }
                if (tier.deltaTable == null) {
                    tier.deltaTable = new BaseHashTable<>(1);
                }
                tier.deltaTable.baseAddOrReplace(prev);
            }
        }
    }

    Iterator<T> snapshottableIterator(long epoch) {
        if (epoch == LATEST_EPOCH) {
            return new CurrentIterator(baseElements());
        } else {
            return new HistoricalIterator(baseElements(), snapshotRegistry.getSnapshot(epoch));
        }
    }

    String snapshottableToDebugString() {
        StringBuilder bld = new StringBuilder();
        bld.append(String.format("SnapshottableHashTable{%n"));
        bld.append("top tier: ");
        bld.append(baseToDebugString());
        bld.append(String.format(",%nsnapshot tiers: [%n"));
        String prefix = "";
        for (Iterator<Snapshot> iter = snapshotRegistry.iterator(); iter.hasNext(); ) {
            Snapshot snapshot = iter.next();
            bld.append(prefix);
            bld.append("epoch ").append(snapshot.epoch()).append(": ");
            HashTier<T> tier = snapshot.getDelta(this);
            if (tier == null) {
                bld.append("null");
            } else {
                bld.append("HashTier{");
                bld.append("size=").append(tier.size);
                bld.append(", deltaTable=");
                if (tier.deltaTable == null) {
                    bld.append("null");
                } else {
                    bld.append(tier.deltaTable.baseToDebugString());
                }
                bld.append("}");
            }
            bld.append(String.format("%n"));
        }
        bld.append(String.format("]}%n"));
        return bld.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void executeRevert(long targetEpoch, Delta delta) {
        HashTier<T> tier = (HashTier<T>) delta;
        Iterator<T> iter = snapshottableIterator(LATEST_EPOCH);
        while (iter.hasNext()) {
            T element = iter.next();
            if (element.startEpoch() > targetEpoch) {
                iter.remove();
            }
        }
        BaseHashTable<T> deltaTable = tier.deltaTable;
        if (deltaTable != null) {
            List<T> out = new ArrayList<>();
            for (int i = 0; i < deltaTable.baseElements().length; i++) {
                BaseHashTable.unpackSlot(out, deltaTable.baseElements(), i);
                for (T value : out) {
                    baseAddOrReplace(value);
                }
                out.clear();
            }
        }
    }

    @Override
    public void reset() {
        Iterator<T> iter = snapshottableIterator(SnapshottableHashTable.LATEST_EPOCH);
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
    }
}

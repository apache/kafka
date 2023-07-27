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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;


/**
 * A registry containing snapshots of timeline data structures.
 * We generally expect a small number of snapshots-- perhaps 1 or 2 at a time.
 * Therefore, we use ArrayLists here rather than a data structure with higher overhead.
 */
public class SnapshotRegistry {
    public final static long LATEST_EPOCH = Long.MAX_VALUE;

    /**
     * Iterate through the list of snapshots in order of creation, such that older
     * snapshots come first.
     */
    class SnapshotIterator implements Iterator<Snapshot> {
        Snapshot cur;
        Snapshot result = null;

        SnapshotIterator(Snapshot start) {
            cur = start;
        }

        @Override
        public boolean hasNext() {
            return cur != head;
        }

        @Override
        public Snapshot next() {
            result = cur;
            cur = cur.next();
            return result;
        }

        @Override
        public void remove() {
            if (result == null) {
                throw new IllegalStateException();
            }
            deleteSnapshot(result);
            result = null;
        }
    }

    /**
     * Iterate through the list of snapshots in reverse order of creation, such that
     * the newest snapshot is first.
     */
    class ReverseSnapshotIterator implements Iterator<Snapshot> {
        Snapshot cur;

        ReverseSnapshotIterator() {
            cur = head.prev();
        }

        @Override
        public boolean hasNext() {
            return cur != head;
        }

        @Override
        public Snapshot next() {
            Snapshot result = cur;
            cur = cur.prev();
            return result;
        }
    }

    private final Logger log;

    /**
     * A map from snapshot epochs to snapshot data structures.
     */
    private final HashMap<Long, Snapshot> snapshots = new HashMap<>();

    /**
     * The head of a list of snapshots, sorted by epoch.
     */
    private final Snapshot head = new Snapshot(Long.MIN_VALUE);

    /**
     * Collection of all Revertable registered with this registry
     */
    private final List<Revertable> revertables = new ArrayList<>();

    public SnapshotRegistry(LogContext logContext) {
        this.log = logContext.logger(SnapshotRegistry.class);
    }

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the
     * lowest epoch to those with the highest.
     */
    public Iterator<Snapshot> iterator() {
        return new SnapshotIterator(head.next());
    }

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the
     * lowest epoch to those with the highest, starting at the snapshot with the
     * given epoch.
     */
    public Iterator<Snapshot> iterator(long epoch) {
        return iterator(getSnapshot(epoch));
    }

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the
     * lowest epoch to those with the highest, starting at the given snapshot.
     */
    public Iterator<Snapshot> iterator(Snapshot snapshot) {
        return new SnapshotIterator(snapshot);
    }

    /**
     * Returns a reverse snapshot iterator that iterates from the snapshots with the
     * highest epoch to those with the lowest.
     */
    public Iterator<Snapshot> reverseIterator() {
        return new ReverseSnapshotIterator();
    }

    /**
     * Returns a sorted list of snapshot epochs.
     */
    public List<Long> epochsList() {
        List<Long> result = new ArrayList<>();
        for (Iterator<Snapshot> iterator = iterator(); iterator.hasNext(); ) {
            result.add(iterator.next().epoch());
        }
        return result;
    }

    public boolean hasSnapshot(long epoch) {
        return snapshots.containsKey(epoch);
    }

    /**
     * Gets the snapshot for a specific epoch.
     */
    public Snapshot getSnapshot(long epoch) {
        Snapshot snapshot = snapshots.get(epoch);
        if (snapshot == null) {
            throw new RuntimeException("No in-memory snapshot for epoch " + epoch + ". Snapshot " +
                "epochs are: " + epochsList().stream().map(e -> e.toString()).
                    collect(Collectors.joining(", ")));
        }
        return snapshot;
    }

    /**
     * Creates a new snapshot at the given epoch.
     *
     * If {@code epoch} already exists and it is the last snapshot then just return that snapshot.
     *
     * @param epoch             The epoch to create the snapshot at.  The current epoch
     *                          will be advanced to one past this epoch.
     */
    public Snapshot getOrCreateSnapshot(long epoch) {
        Snapshot last = head.prev();
        if (last.epoch() > epoch) {
            throw new RuntimeException("Can't create a new in-memory snapshot at epoch " + epoch +
                " because there is already a snapshot with epoch " + last.epoch());
        } else if (last.epoch() == epoch) {
            return last;
        }
        Snapshot snapshot = new Snapshot(epoch);
        last.appendNext(snapshot);
        snapshots.put(epoch, snapshot);
        log.debug("Creating in-memory snapshot {}", epoch);
        return snapshot;
    }

    /**
     * Reverts the state of all data structures to the state at the given epoch.
     *
     * @param targetEpoch       The epoch of the snapshot to revert to.
     */
    public void revertToSnapshot(long targetEpoch) {
        Snapshot target = getSnapshot(targetEpoch);
        Iterator<Snapshot> iterator = iterator(target);
        iterator.next();
        while (iterator.hasNext()) {
            Snapshot snapshot = iterator.next();
            log.debug("Deleting in-memory snapshot {} because we are reverting to {}",
                snapshot.epoch(), targetEpoch);
            iterator.remove();
        }
        target.handleRevert();
    }

    /**
     * Deletes the snapshot with the given epoch.
     *
     * @param targetEpoch       The epoch of the snapshot to delete.
     */
    public void deleteSnapshot(long targetEpoch) {
        deleteSnapshot(getSnapshot(targetEpoch));
    }

    /**
     * Deletes the given snapshot.
     *
     * @param snapshot          The snapshot to delete.
     */
    public void deleteSnapshot(Snapshot snapshot) {
        Snapshot prev = snapshot.prev();
        if (prev != head) {
            prev.mergeFrom(snapshot);
        } else {
            snapshot.erase();
        }
        log.debug("Deleting in-memory snapshot {}", snapshot.epoch());
        snapshots.remove(snapshot.epoch(), snapshot);
    }

    /**
     * Deletes all the snapshots up to the given epoch
     *
     * @param targetEpoch       The epoch to delete up to.
     */
    public void deleteSnapshotsUpTo(long targetEpoch) {
        for (Iterator<Snapshot> iterator = iterator(); iterator.hasNext(); ) {
            Snapshot snapshot = iterator.next();
            if (snapshot.epoch() >= targetEpoch) {
                return;
            }
            iterator.remove();
        }
    }

    /**
     * Return the latest epoch.
     */
    public long latestEpoch() {
        return head.prev().epoch();
    }

    /**
     * Associate a revertable with this registry.
     */
    public void register(Revertable revertable) {
        revertables.add(revertable);
    }

    /**
     * Delete all snapshots and resets all of the Revertable object registered.
     */
    public void reset() {
        deleteSnapshotsUpTo(LATEST_EPOCH);

        for (Revertable revertable : revertables) {
            revertable.reset();
        }
    }
}

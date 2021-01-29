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
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;


/**
 * A registry containing snapshots of timeline data structures.
 * We generally expect a small number of snapshots-- perhaps 1 or 2 at a time.
 * Therefore, we use ArrayLists here rather than a data structure with higher overhead.
 */
public class SnapshotRegistry {
    class SnapshotIterator implements ListIterator<Snapshot> {
        private Snapshot cur;
        private Snapshot lastResult = null;

        SnapshotIterator(Snapshot startAfter) {
            this.cur = startAfter;
        }

        @Override
        public boolean hasNext() {
            return cur.next() != head;
        }

        @Override
        public Snapshot next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            cur = cur.next();
            lastResult = cur;
            return cur;
        }

        @Override
        public boolean hasPrevious() {
            return cur != head;
        }

        @Override
        public Snapshot previous() {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }
            Snapshot result = cur;
            cur = cur.prev();
            lastResult = result;
            return result;
        }

        @Override
        public int nextIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int previousIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove() {
            if (lastResult == null) {
                throw new IllegalStateException();
            }
            if (cur == lastResult) {
                cur = cur.prev();
            }
            snapshots.remove(lastResult.epoch());
            lastResult.removeSelfFromList();
            lastResult = null;
        }

        @Override
        public void set(Snapshot snapshot) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(Snapshot snapshot) {
            if (cur.epoch() >= snapshot.epoch()) {
                throw new IllegalArgumentException("Can't add snapshot " +
                    snapshot.epoch() + " after snapshot " + cur.epoch());
            }
            cur.add(snapshot);
            cur = snapshot;
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

    public SnapshotRegistry(LogContext logContext) {
        this.log = logContext.logger(SnapshotRegistry.class);
    }

    /**
     * Returns an iterator that moves through snapshots from the lowest to the highest epoch.
     */
    public ListIterator<Snapshot> iterator() {
        return new SnapshotIterator(head);
    }

    /**
     * Returns an iterator that moves through snapshots from the lowest to the highest epoch.
     *
     * @param startAfter    A snapshot that we should start after.
     */
    ListIterator<Snapshot> iterator(Snapshot startAfter) {
        return new SnapshotIterator(startAfter);
    }

    /**
     * Returns a sorted list of snapshot epochs.
     */
    List<Long> epochsList() {
        List<Long> result = new ArrayList<>();
        for (ListIterator<Snapshot> iterator = iterator(); iterator.hasNext(); ) {
            result.add(iterator.next().epoch());
        }
        return result;
    }

    /**
     * Gets the snapshot for a specific epoch.
     */
    public Snapshot get(long epoch) {
        Snapshot snapshot = snapshots.get(epoch);
        if (snapshot == null) {
            throw new RuntimeException("No snapshot for epoch " + epoch + ". Snapshot " +
                "epochs are: " + epochsList().stream().map(e -> e.toString()).
                    collect(Collectors.joining(", ")));
        }
        return snapshot;
    }

    /**
     * Creates a new snapshot at the given epoch.
     *
     * @param epoch             The epoch to create the snapshot at.  The current epoch
     *                          will be advanced to one past this epoch.
     */
    public Snapshot createSnapshot(long epoch) {
        Snapshot last = head.prev();
        if (last.epoch() >= epoch) {
            throw new RuntimeException("Can't create a new snapshot at epoch " + epoch +
                " because there is already a snapshot with epoch " + last.epoch());
        }
        Snapshot snapshot = new Snapshot(epoch);
        last.add(snapshot);
        snapshots.put(epoch, snapshot);
        log.debug("Creating snapshot {}", epoch);
        System.out.println("WATERMELON: snapshots = " + snapshots);
        Snapshot n = head.next();
        while (n != head) {
            System.out.println("WATERMELON: n = " + n.epoch() + " n.prev = " + n.prev() + " n.next = " + n.next());
            n = n.next();
        }
        return snapshot;
    }

    /**
     * Reverts the state of all data structures to the state at the given epoch.
     *
     * @param targetEpoch       The epoch of the snapshot to revert to.
     */
    public void revertToSnapshot(long targetEpoch) {
        Snapshot target = get(targetEpoch);
        for (Iterator<Snapshot> iterator = iterator(target); iterator.hasNext(); ) {
            Snapshot snapshot = iterator.next();
            log.debug("Deleting snapshot {} because we are reverting to {}",
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
        Snapshot snapshot = snapshots.remove(targetEpoch);
        if (snapshot == null) {
            throw new RuntimeException("No snapshot for epoch " + targetEpoch + ". Snapshot " +
                "epochs are: " + epochsList().stream().map(e -> e.toString()).
                collect(Collectors.joining(", ")));
        }
        log.debug("Deleting snapshot {}", snapshot.epoch());
        snapshot.removeSelfFromList();
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
            log.debug("Deleting snapshot {}", snapshot.epoch());
            iterator.remove();
        }
    }

    /**
     * Return the latest epoch.
     */
    public long latestEpoch() {
        return head.prev().epoch();
    }
}

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

import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A registry containing snapshots of timeline data structures. All timeline data structures must
 * be registered here, so that they can be reverted to the expected state when desired.
 * Because the registry only keeps a weak reference to each timeline data structure, it does not
 * prevent them from being garbage collected.
 */
public class SnapshotRegistry {
    public static final long LATEST_EPOCH = Long.MAX_VALUE;

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
     * A collection of all Revertable objects registered here. Since we store only weak
     * references, every time we access a revertable through this list, we must check to
     * see if it has been garbage collected. If so, WeakReference.get will return null.
     *
     * Although the garbage collector handles freeing the underlying Revertables, over
     * time slots in the ArrayList will fill up with expired references. Therefore, after
     * enough registrations, we scrub the ArrayList of the expired references by creating
     * a new arraylist.
     */
    private List<WeakReference<Revertable>> revertables = new ArrayList<>();

    /**
     * The maximum number of registrations to allow before we compact the revertable list.
     */
    private final int maxRegistrationsSinceScrub;

    /**
     * The number of registrations we have done since removing all expired weak references.
     */
    private int numRegistrationsSinceScrub = 0;

    /**
     * The number of scrubs that we have done.
     */
    private long numScrubs = 0;

    public SnapshotRegistry(LogContext logContext) {
        this(logContext, 10_000);
    }

    public SnapshotRegistry(LogContext logContext, int maxRegistrationsSinceScrub) {
        this.log = logContext.logger(SnapshotRegistry.class);
        this.maxRegistrationsSinceScrub = maxRegistrationsSinceScrub;
    }

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the
     * lowest epoch to those with the highest.
     */
    Iterator<Snapshot> iterator() {
        return new SnapshotIterator(head.next());
    }

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the
     * lowest epoch to those with the highest, starting at the snapshot with the
     * given epoch.
     */
    Iterator<Snapshot> iterator(long epoch) {
        return iterator(getSnapshot(epoch));
    }

    /**
     * Returns a snapshot iterator that iterates from the snapshots with the
     * lowest epoch to those with the highest, starting at the given snapshot.
     */
    Iterator<Snapshot> iterator(Snapshot snapshot) {
        return new SnapshotIterator(snapshot);
    }

    /**
     * Returns a reverse snapshot iterator that iterates from the snapshots with the
     * highest epoch to those with the lowest.
     */
    Iterator<Snapshot> reverseIterator() {
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

    private String epochsToString() {
        return epochsList()
            .stream()
            .map(Object::toString)
            .collect(Collectors.joining(", "));
    }
    /**
     * Gets the snapshot for a specific epoch.
     */
    Snapshot getSnapshot(long epoch) {
        Snapshot snapshot = snapshots.get(epoch);
        if (snapshot == null) {
            throw new RuntimeException("No in-memory snapshot for epoch " + epoch + ". Snapshot " +
                "epochs are: " + epochsToString());
        }
        return snapshot;
    }

    /**
     * Creates a new snapshot at the given epoch.
     * <p>
     * If {@code epoch} already exists, and it is the last snapshot then just return that snapshot.
     *
     * @param epoch             The epoch to create the snapshot at. The current epoch
     *                          will be advanced to one past this epoch.
     */
    Snapshot getOrCreateSnapshot(long epoch) {
        Snapshot last = head.prev();
        if (last.epoch() > epoch) {
            throw new RuntimeException("Can't create a new in-memory snapshot at epoch " + epoch +
                " because there is already a snapshot with epoch " + last.epoch() + ". Snapshot epochs are " +
                epochsToString());
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
     * Creates a new snapshot at the given epoch.
     * <p>
     * If {@code epoch} already exists, and it is the last snapshot then this operation will do nothing.
     *
     * @param epoch             The epoch to create the snapshot at. The current epoch
     *                          will be advanced to one past this epoch.
     */
    public void idempotentCreateSnapshot(long epoch) {
        getOrCreateSnapshot(epoch);
    }

    /**
     * Reverts the state of all data structures to the state at the given epoch.
     *
     * @param targetEpoch       The epoch of the snapshot to revert to.
     */
    public void revertToSnapshot(long targetEpoch) {
        log.debug("Reverting to in-memory snapshot {}", targetEpoch);
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
    void deleteSnapshot(Snapshot snapshot) {
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
     * Return the number of scrub operations that we have done.
     */
    public long numScrubs() {
        return numScrubs;
    }

    /**
     * Associate a revertable with this registry.
     */
    void register(Revertable revertable) {
        numRegistrationsSinceScrub++;
        if (numRegistrationsSinceScrub > maxRegistrationsSinceScrub) {
            scrub();
        }
        revertables.add(new WeakReference<>(revertable));
    }

    /**
     * Remove all expired weak references from the revertable list.
     */
    void scrub() {
        ArrayList<WeakReference<Revertable>> newRevertables =
            new ArrayList<>(revertables.size() / 2);
        for (WeakReference<Revertable> ref : revertables) {
            if (ref.get() != null) {
                newRevertables.add(ref);
            }
        }
        numScrubs++;
        this.revertables = newRevertables;
        numRegistrationsSinceScrub = 0;
    }

    /**
     * Delete all snapshots and reset all of the Revertable objects.
     */
    public void reset() {
        deleteSnapshotsUpTo(LATEST_EPOCH);

        ArrayList<WeakReference<Revertable>> newRevertables = new ArrayList<>();
        for (WeakReference<Revertable> ref : revertables) {
            Revertable revertable = ref.get();
            if (revertable != null) {
                try {
                    revertable.reset();
                } catch (Exception e) {
                    log.error("Error reverting {}", revertable, e);
                }
                newRevertables.add(ref);
            }
        }
        numScrubs++;
        this.revertables = newRevertables;
        numRegistrationsSinceScrub = 0;
    }
}

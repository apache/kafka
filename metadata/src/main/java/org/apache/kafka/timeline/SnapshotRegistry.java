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
import java.util.stream.Collectors;

/**
 * A registry containing snapshots of timeline data structures.
 * We generally expect a small number of snapshots-- perhaps 1 or 2 at a time.
 * Therefore, we use ArrayLists here rather than a data structure with higher overhead.
 */
public class SnapshotRegistry {
    private final static int EXPECTED_NUM_SNAPSHOTS = 2;

    /**
     * The current epoch.  All snapshot epochs are lower than this number.
     */
    private long curEpoch;

    /**
     * An ArrayList of snapshots, kept in sorted order.
     */
    private final ArrayList<Snapshot> snapshots;

    public SnapshotRegistry(long startEpoch) {
        this.curEpoch = startEpoch;
        this.snapshots = new ArrayList<>(EXPECTED_NUM_SNAPSHOTS);
    }

    /**
     * Returns an iterator that moves through snapshots from the lowest to the highest epoch.
     */
    public Iterator<Snapshot> snapshots() {
        return snapshots.iterator();
    }

    /**
     * Gets the snapshot for a specific epoch.
     */
    public Snapshot get(long epoch) {
        for (Snapshot snapshot : snapshots) {
            if (snapshot.epoch() == epoch) {
                return snapshot;
            }
        }
        throw new RuntimeException("No snapshot for epoch " + epoch);
    }

    /**
     * Creates a new snapshot at the given epoch.
     *
     * @param epoch             The epoch to create the snapshot at.  The current epoch
     *                          will be advanced to one past this epoch.
     */
    public Snapshot createSnapshot(long epoch) {
        if (epoch < curEpoch) {
            throw new RuntimeException("Can't create a new snapshot at epoch " + epoch +
                    " because the current epoch is " + curEpoch);
        }
        Snapshot snapshot = new Snapshot(epoch);
        snapshots.add(snapshot);
        curEpoch = epoch + 1;
        return snapshot;
    }

    /**
     * Deletes the snapshot with the given epoch.
     *
     * @param epoch             The epoch of the snapshot to delete.
     */
    public void deleteSnapshot(long epoch) {
        Iterator<Snapshot> iter = snapshots.iterator();
        while (iter.hasNext()) {
            Snapshot snapshot = iter.next();
            if (snapshot.epoch() == epoch) {
                iter.remove();
                return;
            }
        }
        throw new RuntimeException(String.format(
            "No snapshot at epoch %d found. Snapshot epochs are %s.", epoch,
                snapshots.stream().map(snapshot -> String.valueOf(snapshot.epoch())).
                    collect(Collectors.joining(", "))));
    }

    /**
     * Reverts the state of all data structures to the state at the given epoch.
     *
     * @param epoch             The epoch of the snapshot to revert to.
     */
    public void revertToSnapshot(long epoch) {
        Snapshot target = null;
        for (Iterator<Snapshot> iter = snapshots.iterator(); iter.hasNext(); ) {
            Snapshot snapshot = iter.next();
            if (target == null) {
                if (snapshot.epoch() == epoch) {
                    target = snapshot;
                    iter.remove();
                }
            } else {
                iter.remove();
            }
        }
        target.handleRevert();
        curEpoch = epoch;
    }

    /**
     * Returns the current epoch.
     */
    public long curEpoch() {
        return curEpoch;
    }
}

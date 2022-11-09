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

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * A snapshot of some timeline data structures.
 *
 * The snapshot contains historical data for several timeline data structures.
 * We use an IdentityHashMap to store this data.  This way, we can easily drop all of
 * the snapshot data.
 */
class Snapshot {
    private final long epoch;
    private IdentityHashMap<Revertable, Delta> map = new IdentityHashMap<>(4);
    private Snapshot prev = this;
    private Snapshot next = this;

    Snapshot(long epoch) {
        this.epoch = epoch;
    }

    long epoch() {
        return epoch;
    }

    @SuppressWarnings("unchecked")
    <T extends Delta> T getDelta(Revertable owner) {
        return (T) map.get(owner);
    }

    void setDelta(Revertable owner, Delta delta) {
        map.put(owner, delta);
    }

    void handleRevert() {
        for (Map.Entry<Revertable, Delta> entry : map.entrySet()) {
            entry.getKey().executeRevert(epoch, entry.getValue());
        }
    }

    void mergeFrom(Snapshot source) {
        // Merge the deltas from the source snapshot into this snapshot.
        for (Map.Entry<Revertable, Delta> entry : source.map.entrySet()) {
            // We first try to just copy over the object reference.  That will work if
            //we have no entry at all for the given Revertable.
            Delta destinationDelta = map.putIfAbsent(entry.getKey(), entry.getValue());
            if (destinationDelta != null) {
                // If we already have an entry for the Revertable, we need to merge the
                // source delta into our delta.
                destinationDelta.mergeFrom(epoch, entry.getValue());
            }
        }
        // Delete the source snapshot to make sure nobody tries to reuse it.  We might now
        // share some delta entries with it.
        source.erase();
    }

    Snapshot prev() {
        return prev;
    }

    Snapshot next() {
        return next;
    }

    void appendNext(Snapshot newNext) {
        newNext.prev = this;
        newNext.next = next;
        next.prev = newNext;
        next = newNext;
    }

    void erase() {
        map = null;
        next.prev = prev;
        prev.next = next;
        prev = this;
        next = this;
    }
}

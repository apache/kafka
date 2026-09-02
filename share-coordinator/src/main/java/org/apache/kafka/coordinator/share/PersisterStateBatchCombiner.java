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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.server.share.persister.PersisterStateBatch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

/**
 * Combines an existing list of {@link PersisterStateBatch} entries with a list of newly produced
 * entries and returns the shortest non-overlapping, {@code (deliveryState, deliveryCount)}-distinct
 * cover of the union, clipped at {@code startOffset} (SPSO).
 *
 * <p>The merge is performed by an event-driven sweep-line over the union of both inputs. Each
 * input batch contributes one BEGIN event at {@code firstOffset} and one END event at
 * {@code lastOffset + 1}. Events are processed in offset order (END before BEGIN at the same
 * offset). A counted ordered map tracks the currently active priorities; the first key defines the
 * {@code (state, count)} that wins on the current sub-range. Successive sub-ranges with identical
 * {@code (state, count)} are coalesced on the fly.
 *
 * <p>Complexity: {@code O((n + k) log p)} where {@code n} is the total number of input batches,
 * {@code k} is the number of overlap transitions encountered, and {@code p} is the number of
 * distinct {@code (deliveryState, deliveryCount)} priorities.
 */
public class PersisterStateBatchCombiner {
    private static final Comparator<BatchPriority> PRIORITY_DESC = (a, b) -> {
        int cmpCount = Short.compare(b.deliveryCount(), a.deliveryCount());
        if (cmpCount != 0) {
            return cmpCount;
        }
        return Byte.compare(b.deliveryState(), a.deliveryState());
    };

    private final List<PersisterStateBatch> batchesSoFar;
    private final List<PersisterStateBatch> newBatches;
    private final long startOffset;

    public PersisterStateBatchCombiner(
        List<PersisterStateBatch> batchesSoFar,
        List<PersisterStateBatch> newBatches,
        long startOffset
    ) {
        this.batchesSoFar = batchesSoFar == null ? List.of() : batchesSoFar;
        this.newBatches = newBatches == null ? List.of() : newBatches;
        this.startOffset = startOffset;
    }

    /**
     * Produces the merged, pruned, non-overlapping batch list.
     */
    public List<PersisterStateBatch> combineStateBatches() {
        List<PersisterStateBatch> pruned = prune();
        if (pruned.isEmpty()) {
            return pruned;
        }
        if (pruned.size() == 1) {
            return pruned;
        }
        return sweepMerge(pruned);
    }

    /**
     * Drops or clips ranges below {@code startOffset}. Returns a new list; never modifies inputs.
     */
    private List<PersisterStateBatch> prune() {
        int estimate = batchesSoFar.size() + newBatches.size();
        List<PersisterStateBatch> out = new ArrayList<>(estimate);
        addPruned(out, batchesSoFar);
        addPruned(out, newBatches);
        return out;
    }

    private void addPruned(List<PersisterStateBatch> out, List<PersisterStateBatch> src) {
        for (PersisterStateBatch b : src) {
            if (startOffset != -1 && b.lastOffset() < startOffset) {
                // batch fully expired
                continue;
            }
            if (startOffset == -1 || b.firstOffset() >= startOffset) {
                out.add(b);
            } else {
                // start offset intersects batch -> clip
                out.add(new PersisterStateBatch(startOffset, b.lastOffset(), b.deliveryState(), b.deliveryCount()));
            }
        }
    }

    /**
     * Event-driven sweep. Linear time after the initial event sort.
     */
    private List<PersisterStateBatch> sweepMerge(List<PersisterStateBatch> batches) {
        int n = batches.size();
        Event[] events = new Event[n * 2];
        for (int i = 0; i < n; i++) {
            PersisterStateBatch b = batches.get(i);
            BatchPriority priority = BatchPriority.from(b);
            events[i * 2] = new Event(b.firstOffset(), true, priority);
            events[i * 2 + 1] = new Event(b.lastOffset() + 1, false, priority);
        }
        // END (isBegin=false) sorts before BEGIN (isBegin=true) at the same offset so that
        // contiguous same-state ranges meeting at offset X collapse to a single emit.
        java.util.Arrays.sort(events, (e1, e2) -> {
            int cmp = Long.compare(e1.offset, e2.offset);
            if (cmp != 0) {
                return cmp;
            }
            return Boolean.compare(e1.isBegin, e2.isBegin);
        });

        TreeMap<BatchPriority, Integer> active = new TreeMap<>(PRIORITY_DESC);
        List<PersisterStateBatch> out = new ArrayList<>();
        long openFrom = -1;
        BatchPriority openWinner = null;

        int i = 0;
        while (i < events.length) {
            long offset = events[i].offset;

            // Emit the sub-range that ended at `offset - 1` (if one was open).
            if (openWinner != null && offset > openFrom) {
                appendCoalesced(out, openFrom, offset - 1, openWinner);
            }

            // Apply every event at this offset (END before BEGIN by sort order).
            while (i < events.length && events[i].offset == offset) {
                Event e = events[i++];
                if (e.isBegin) {
                    active.merge(e.priority, 1, Integer::sum);
                } else {
                    decrement(active, e.priority);
                }
            }

            openWinner = active.isEmpty() ? null : active.firstKey();
            openFrom = offset;
        }
        return out;
    }

    private void decrement(TreeMap<BatchPriority, Integer> active, BatchPriority priority) {
        int count = active.get(priority);
        if (count == 1) {
            active.remove(priority);
        } else {
            active.put(priority, count - 1);
        }
    }

    private void appendCoalesced(List<PersisterStateBatch> out, long from, long to, BatchPriority winner) {
        if (!out.isEmpty()) {
            PersisterStateBatch tail = out.get(out.size() - 1);
            if (tail.lastOffset() + 1 == from
                && tail.deliveryState() == winner.deliveryState()
                && tail.deliveryCount() == winner.deliveryCount()) {
                out.set(out.size() - 1, new PersisterStateBatch(
                    tail.firstOffset(), to, winner.deliveryState(), winner.deliveryCount()));
                return;
            }
        }
        out.add(new PersisterStateBatch(from, to, winner.deliveryState(), winner.deliveryCount()));
    }

    private record BatchPriority(short deliveryCount, byte deliveryState) {
        private static BatchPriority from(PersisterStateBatch batch) {
            return new BatchPriority(batch.deliveryCount(), batch.deliveryState());
        }
    }

    private record Event(long offset, boolean isBegin, BatchPriority priority) {
    }
}

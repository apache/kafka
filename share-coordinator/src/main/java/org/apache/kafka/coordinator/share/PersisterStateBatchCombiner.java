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
 * entries and returns the shortest non-overlapping, state-distinct cover of the union, clipped at
 * {@code startOffset} (SPSO).
 *
 * <p>The merge is performed by an event-driven sweep-line over the union of both inputs. Each
 * input batch contributes one BEGIN event at {@code firstOffset} and one END event at
 * {@code lastOffset + 1}. Events are processed in offset order (END before BEGIN at the same
 * offset). A counted ordered map tracks the currently active priorities; the first key defines the
 * state that wins on the current sub-range. Successive sub-ranges with identical state are
 * coalesced on the fly.
 *
 * <p>Complexity: {@code O((n + k) log p)} where {@code n} is the total number of input batches,
 * {@code k} is the number of overlap transitions encountered, and {@code p} is the number of
 * distinct state priorities.
 */
public class PersisterStateBatchCombiner {
    private static final Comparator<BatchPriority> PRIORITY_DESC = (a, b) -> {
        int cmpCount = Short.compare(b.deliveryCount(), a.deliveryCount());
        if (cmpCount != 0) {
            return cmpCount;
        }
        int cmpState = Byte.compare(b.deliveryState(), a.deliveryState());
        if (cmpState != 0) {
            return cmpState;
        }
        int cmpProducerId = Long.compare(b.stagedProducerId(), a.stagedProducerId());
        if (cmpProducerId != 0) {
            return cmpProducerId;
        }
        int cmpProducerEpoch = Short.compare(b.stagedProducerEpoch(), a.stagedProducerEpoch());
        if (cmpProducerEpoch != 0) {
            return cmpProducerEpoch;
        }
        int cmpAckType = Byte.compare(b.stagedAckType(), a.stagedAckType());
        if (cmpAckType != 0) {
            return cmpAckType;
        }
        return Byte.compare(b.stagedDeliveryState(), a.stagedDeliveryState());
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
                continue;
            }
            if (startOffset == -1 || b.firstOffset() >= startOffset) {
                out.add(b);
            } else {
                out.add(copyWithOffsets(b, startOffset, b.lastOffset()));
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

            if (openWinner != null && offset > openFrom) {
                appendCoalesced(out, openFrom, offset - 1, openWinner);
            }

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
            if (tail.lastOffset() + 1 == from && winner.sameState(tail)) {
                out.set(out.size() - 1, winner.toBatch(tail.firstOffset(), to));
                return;
            }
        }
        out.add(winner.toBatch(from, to));
    }

    private static PersisterStateBatch copyWithOffsets(PersisterStateBatch batch, long firstOffset, long lastOffset) {
        return new PersisterStateBatch(
            firstOffset,
            lastOffset,
            batch.deliveryState(),
            batch.deliveryCount(),
            batch.stagedProducerId(),
            batch.stagedProducerEpoch(),
            batch.stagedAckType(),
            batch.stagedDeliveryState()
        );
    }

    private record BatchPriority(
        short deliveryCount,
        byte deliveryState,
        long stagedProducerId,
        short stagedProducerEpoch,
        byte stagedAckType,
        byte stagedDeliveryState
    ) {
        private static BatchPriority from(PersisterStateBatch batch) {
            return new BatchPriority(
                batch.deliveryCount(),
                batch.deliveryState(),
                batch.stagedProducerId(),
                batch.stagedProducerEpoch(),
                batch.stagedAckType(),
                batch.stagedDeliveryState()
            );
        }

        private boolean sameState(PersisterStateBatch batch) {
            return deliveryCount == batch.deliveryCount()
                && deliveryState == batch.deliveryState()
                && stagedProducerId == batch.stagedProducerId()
                && stagedProducerEpoch == batch.stagedProducerEpoch()
                && stagedAckType == batch.stagedAckType()
                && stagedDeliveryState == batch.stagedDeliveryState();
        }

        private PersisterStateBatch toBatch(long firstOffset, long lastOffset) {
            return new PersisterStateBatch(
                firstOffset,
                lastOffset,
                deliveryState,
                deliveryCount,
                stagedProducerId,
                stagedProducerEpoch,
                stagedAckType,
                stagedDeliveryState
            );
        }
    }

    private record Event(long offset, boolean isBegin, BatchPriority priority) {
    }
}

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

import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.share.PersisterStateBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

public class StateBatchUtil {
    /**
     * Util method which takes in 2 lists containing {@link PersisterStateBatch}
     * and the startOffset.
     * This method removes any batches where the lastOffset < startOffset, if the startOffset > -1.
     * It then merges any contiguous intervals with same state. If states differ,
     * based on various conditions it creates new non-overlapping batches preferring new ones.
     * @param batchesSoFar - List containing current soft state of {@link PersisterStateBatch}
     * @param newBatches - List containing {@link PersisterStateBatch} in incoming request
     * @param startOffset - startOffset to consider when removing old batches.
     * @return List containing combined batches
     */
    public static List<PersisterStateBatch> combineStateBatches(
        List<PersisterStateBatch> batchesSoFar,
        List<PersisterStateBatch> newBatches,
        long startOffset
    ) {
        List<PersisterStateBatch> combinedList = new ArrayList<>(batchesSoFar.size() + newBatches.size());
        combinedList.addAll(batchesSoFar);
        combinedList.addAll(newBatches);

        return mergeBatches(
            pruneBatches(
                combinedList,
                startOffset
            )
        );
    }

    /**
     * Encapsulates the main merge algorithm. Consider 2 batches (A, B):
     * - Same state (delivery count and state)
     *  - If overlapping - merge into single batch
     *  - If contiguous (A.lastOffset + 1 == B.firstOffset) - merge batches into a single 1
     * - Different state (delivery count or state differ)
     *  - Based on various cases:
     *      - swallow lower priority batch within bounds of offsets
     *      - break batch into other non-overlapping batches
     * @param batches - List of {@link PersisterStateBatch}
     * @return List of non-overlapping {@link PersisterStateBatch}
     */
    private static List<PersisterStateBatch> mergeBatches(List<PersisterStateBatch> batches) {
        if (batches.size() < 2) {
            return batches;
        }
        TreeSet<PersisterStateBatch> sortedBatches = new TreeSet<>(batches);
        List<PersisterStateBatch> finalBatches = new ArrayList<>(batches.size() * 2); // heuristic size

        BatchOverlapState overlapState = getOverlappingState(sortedBatches);

        while (overlapState != BatchOverlapState.SENTINEL) {
            PersisterStateBatch last = overlapState.last();
            PersisterStateBatch candidate = overlapState.candidate();

            // remove non overlapping prefix from sortedBatches,
            // will make getting next overlapping pair efficient
            // as a prefix batch which is non overlapping will only
            // be checked once.
            if (overlapState.nonOverlapping() != null) {
                overlapState.nonOverlapping().forEach(sortedBatches::remove);
                finalBatches.addAll(overlapState.nonOverlapping());
            }

            if (candidate == null) {
                overlapState = BatchOverlapState.SENTINEL;
                continue;
            }

            // remove both last and candidate for easier
            // assessment about adding batches to sortedBatches
            sortedBatches.remove(last);
            sortedBatches.remove(candidate);

            // overlap and same state (last.firstOffset <= candidate.firstOffset) due to sort
            // covers:
            // case:        1        2          3            4          5           6          7 (contiguous)
            // last:        ______   _______    _______      _______   _______   ________    _______
            // candidate:   ______   ____       __________     ___        ____       _______        _______
            if (compareBatchState(candidate, last) == 0) {
                sortedBatches.add(new PersisterStateBatch(
                    last.firstOffset(),
                    // cover cases
                    // last:      ______   ________       _________
                    // candidate:   ___       __________           _____
                    Math.max(candidate.lastOffset(), last.lastOffset()),
                    last.deliveryState(),
                    last.deliveryCount()
                ));
            } else if (candidate.firstOffset() <= last.lastOffset()) { // non-contiguous overlap
                // overlap and different state
                // covers:
                // case:        1        2*          3            4          5           6
                // last:        ______   _______    _______      _______    _______     ________
                // candidate:   ______   ____       _________      ____        ____          _______
                // max batches: 1           2       2                3          2            2
                // min batches: 1           1       1                1          1            2
                // * not possible with treeset

                if (candidate.firstOffset() == last.firstOffset()) {
                    if (candidate.lastOffset() == last.lastOffset()) {  // case 1
                        // candidate can never have lower or equal priority
                        // since sortedBatches order takes that into account.
                        sortedBatches.add(candidate);
                    } else {
                        // case 2 is not possible with TreeSet. It is symmetric to case 3.
                        // case 3
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);
                            sortedBatches.add(new PersisterStateBatch(
                                last.lastOffset() + 1,
                                candidate.lastOffset(),
                                candidate.deliveryState(),
                                candidate.deliveryCount()
                            ));
                        } else {
                            // candidate priority is >= last
                            sortedBatches.add(candidate);
                        }
                    }
                } else {    // candidate.firstOffset() > last.firstOffset()
                    if (candidate.lastOffset() < last.lastOffset()) {    // case 4
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);
                        } else {
                            sortedBatches.add(new PersisterStateBatch(
                                last.firstOffset(),
                                candidate.firstOffset() - 1,
                                last.deliveryState(),
                                last.deliveryCount()
                            ));

                            sortedBatches.add(candidate);

                            sortedBatches.add(new PersisterStateBatch(
                                candidate.lastOffset() + 1,
                                last.lastOffset(),
                                last.deliveryState(),
                                last.deliveryCount()
                            ));
                        }
                    } else if (candidate.lastOffset() == last.lastOffset()) {    // case 5
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);
                        } else {
                            sortedBatches.add(new PersisterStateBatch(
                                last.firstOffset(),
                                candidate.firstOffset() - 1,
                                last.deliveryState(),
                                last.deliveryCount()
                            ));

                            sortedBatches.add(candidate);
                        }
                    } else {    // case 6
                        if (compareBatchState(candidate, last) < 0) {
                            sortedBatches.add(last);

                            sortedBatches.add(new PersisterStateBatch(
                                last.lastOffset() + 1,
                                candidate.lastOffset(),
                                candidate.deliveryState(),
                                candidate.deliveryCount()
                            ));
                        } else {
                            // candidate has higher priority
                            sortedBatches.add(new PersisterStateBatch(
                                last.firstOffset(),
                                candidate.firstOffset() - 1,
                                last.deliveryState(),
                                last.deliveryCount()
                            ));

                            sortedBatches.add(candidate);
                        }
                    }
                }
            }
            overlapState = getOverlappingState(sortedBatches);
        }
        finalBatches.addAll(sortedBatches);   // some non overlapping batches might have remained
        return finalBatches;
    }

    /**
     * Accepts a sorted set of state batches and finds the first 2 batches which overlap.
     * Overlap means that they have some offsets in common or, they are contiguous with the same state.
     * Along with the 2 overlapping batches, also returns a list of non overlapping intervals
     * prefixing them. For example
     * _____ ____  _____ _____      _____
     *                      ______     __
     * <---------------> <-------->
     *  non-overlapping   1st overlapping pair
     *
     * @param sortedBatches - TreeSet representing sorted set of {@link PersisterStateBatch}
     * @return object of {@link BatchOverlapState} representing overlapping pair and non-overlapping prefix
     */
    private static BatchOverlapState getOverlappingState(TreeSet<PersisterStateBatch> sortedBatches) {
        if (sortedBatches == null || sortedBatches.isEmpty()) {
            return BatchOverlapState.SENTINEL;
        }
        Iterator<PersisterStateBatch> iter = sortedBatches.iterator();
        PersisterStateBatch last = iter.next();
        List<PersisterStateBatch> nonOverlapping = new ArrayList<>(sortedBatches.size());
        while (iter.hasNext()) {
            PersisterStateBatch candidate = iter.next();
            if (candidate.firstOffset() <= last.lastOffset() || // overlap
                last.lastOffset() + 1 == candidate.firstOffset() && compareBatchState(last, candidate) == 0) {  // contiguous
                return new BatchOverlapState(
                    last,
                    candidate,
                    nonOverlapping
                );
            }
            nonOverlapping.add(last);
            last = candidate;
        }
        return new BatchOverlapState(null, null, nonOverlapping);
    }

    /**
     * Compares the state of 2 batches i.e. the deliveryCount and deliverState.
     * Uses standard compareTo contract x < y => +int, x > y => -int, x == y => 0
     * @param b1 - {@link PersisterStateBatch} to compare
     * @param b2 - {@link PersisterStateBatch} to compare
     * @return int representing comparison result.
     */
    private static int compareBatchState(PersisterStateBatch b1, PersisterStateBatch b2) {
        int deltaCount = Short.compare(b1.deliveryCount(), b2.deliveryCount());

        // Delivery state could be:
        // 0 - AVAILABLE (non-terminal)
        // 1 - ACQUIRED - should not be persisted yet
        // 2 - ACKNOWLEDGED (terminal)
        // 3 - ARCHIVING - not implemented in KIP-932 - non-terminal - leads only to ARCHIVED
        // 4 - ARCHIVED (terminal)

        if (deltaCount == 0) {   // same delivery count
            return Byte.compare(b1.deliveryState(), b2.deliveryState());
        }
        return deltaCount;
    }

    /**
     * Accepts a list of {@link PersisterStateBatch} and removes/prunes ones which lie
     * before the start offset.
     * @param batches - List of {@link PersisterStateBatch}
     * @param startOffset - long representing the start offset
     * @return List of pruned {@link PersisterStateBatch}
     */
    public static List<PersisterStateBatch> pruneBatches(List<PersisterStateBatch> batches, long startOffset) {
        if (startOffset != -1) {
            List<PersisterStateBatch> prunedList = new ArrayList<>(batches.size());
            batches.forEach(batch -> {
                if (batch.firstOffset() >= startOffset) {
                    // covers:
                    //   ______
                    // | -> start offset
                    prunedList.add(batch);
                } else if (batch.lastOffset() >= startOffset) {
                    // covers:
                    //  ________
                    //       | -> start offset
                    prunedList.add(new PersisterStateBatch(startOffset, batch.lastOffset(), batch.deliveryState(), batch.deliveryCount()));
                }
                // in all other cases, the batch is completely expired.
            });
            return prunedList;
        }
        return batches;
    }

    /**
     * Converts a {@link ShareUpdateValue.StateBatch} type state batch to {@link PersisterStateBatch}
     * @param batch - The object representing {@link ShareUpdateValue.StateBatch}
     * @return {@link PersisterStateBatch}
     */
    public static PersisterStateBatch toPersisterStateBatch(ShareUpdateValue.StateBatch batch) {
        return new PersisterStateBatch(
            batch.firstOffset(),
            batch.lastOffset(),
            batch.deliveryState(),
            batch.deliveryCount()
        );
    }

    /**
     * Holder class for intermediate state
     * used in the batch merge algorithm.
     */
    private static class BatchOverlapState {
        private final PersisterStateBatch last;
        private final PersisterStateBatch candidate;
        private final List<PersisterStateBatch> nonOverlapping;
        public static final BatchOverlapState SENTINEL = new BatchOverlapState(null, null, Collections.emptyList());

        public BatchOverlapState(
            PersisterStateBatch last,
            PersisterStateBatch candidate,
            List<PersisterStateBatch> nonOverlapping
        ) {
            this.last = last;
            this.candidate = candidate;
            this.nonOverlapping = nonOverlapping;
        }

        public PersisterStateBatch last() {
            return last;
        }

        public PersisterStateBatch candidate() {
            return candidate;
        }

        public List<PersisterStateBatch> nonOverlapping() {
            return nonOverlapping;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BatchOverlapState)) return false;
            BatchOverlapState that = (BatchOverlapState) o;
            return Objects.equals(last, that.last) && Objects.equals(candidate, that.candidate) && Objects.equals(nonOverlapping, that.nonOverlapping);
        }

        @Override
        public int hashCode() {
            return Objects.hash(last, candidate, nonOverlapping);
        }
    }
}

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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

public class PersisterStateBatchCombiner {
    private List<PersisterStateBatch> combinedBatchList;    // link between pruning and merging
    private final long startOffset;
    private TreeSet<PersisterStateBatch> sortedBatches;
    private List<PersisterStateBatch> finalBatchList;   // final list is built here

    public PersisterStateBatchCombiner(
        List<PersisterStateBatch> batchesSoFar,
        List<PersisterStateBatch> newBatches,
        long startOffset
    ) {
        initializeCombinedList(batchesSoFar, newBatches);
        int estimatedResultSize = (combinedBatchList.size() * 3) / 2;   // heuristic size - 50% overallocation
        finalBatchList = new ArrayList<>(estimatedResultSize);
        this.startOffset = startOffset;
    }

    private void initializeCombinedList(List<PersisterStateBatch> batchesSoFar, List<PersisterStateBatch> newBatches) {
        boolean soFarEmpty = batchesSoFar == null || batchesSoFar.isEmpty();
        boolean newBatchesEmpty = newBatches == null || newBatches.isEmpty();

        if (soFarEmpty && newBatchesEmpty) {
            combinedBatchList = new ArrayList<>();
        } else if (soFarEmpty) {
            combinedBatchList = new ArrayList<>(newBatches);    // new list as the original one could be unmodifiable
        } else if (newBatchesEmpty) {
            combinedBatchList = new ArrayList<>(batchesSoFar);  // new list as the original one could be unmodifiable
        } else {
            combinedBatchList = new ArrayList<>(batchesSoFar.size() + newBatches.size());
            combinedBatchList.addAll(batchesSoFar);
            combinedBatchList.addAll(newBatches);
        }
    }

    /**
     * Algorithm: Merge current state batches and new batches into a single non-overlapping batch list.
     * Input: batchesSoFar, newBatches, startOffset
     * Output: combined list with non-overlapping batches (finalBatchList)
     * <p>
     * - Add both currentBatches and newBatches into a single list combinedBatchList
     * - if combinedBatchList.size() <= 1 return combinedBatchList
     * <p>
     * - Remove/prune any batches from the combinedBatchList:
     * -    if batch.lastOffset < startOffset then remove batch from combinedBatchList
     * -    else if batch.firstOffset > startOffset then we will keep the batch
     * -    else if batch.firstOffset <= startOffset <= batch.lastOffset then keep [startOffset, batch.lastOffset] part only and discard rest.
     * <p>
     * - create a treeset sortedBatches using pruned combinedBatchList
     * - find first 2 mergeable batches in sortedBatches set, say, prev and candidate.
     * - remove any non-overlapping batches from sortedBatches encountered during the find operation and add them to a finalBatchList
     * - do repeat until a mergeable pair is not found:
     * -    based on various conditions of offset overlap and batch state differences combine the batches or
     *      create new batches, if required, and add to the sortedBatches.
     * -    find first 2 mergeable batches in sortedBatches set, say, prev and candidate.
     * -    remove any non-mergeable batches from sortedBatches encountered during the find operation and add them to a finalBatchList
     * - done
     * - return the finalBatchList
     *
     * @return list of {@link PersisterStateBatch} representing non-overlapping combined batches
     */
    public List<PersisterStateBatch> combineStateBatches() {
        pruneBatches();
        mergeBatches();
        return finalBatchList;
    }

    private void mergeBatches() {
        if (combinedBatchList.size() < 2) {
            finalBatchList = combinedBatchList;
            return;
        }

        sortedBatches = new TreeSet<>(combinedBatchList);

        MergeCandidatePair overlapState = getMergeCandidatePair();

        while (overlapState != MergeCandidatePair.EMPTY) {
            PersisterStateBatch prev = overlapState.prev();
            PersisterStateBatch candidate = overlapState.candidate();

            // remove both previous and candidate for easier
            // assessment about adding batches to sortedBatches
            sortedBatches.remove(prev);
            sortedBatches.remove(candidate);

            if (compareBatchDeliveryInfo(candidate, prev) == 0) {  // same state and overlap or contiguous
                // overlap and same state (prev.firstOffset <= candidate.firstOffset) due to sort
                // covers:
                // case:        1        2          3            4          5           6          7 (contiguous)
                // prev:        ------   -------    -------      -------   -------   --------    -------
                // candidate:   ------   ----       ----------     ---        ----       -------        -------
                handleSameStateMerge(prev, candidate);  // pair can be contiguous or overlapping
            } else {
                // If we reach here then it is guaranteed that the batch pair is overlapping and
                // non-contiguous because getMergeCandidatePair only returns contiguous pair if
                // the constituents have the same delivery count and state.
                // covers:
                // case:        1        2*          3            4          5           6             7*
                // prev:        ------   -------    -------      -------    -------     --------      ------
                // candidate:   ------   ----       ---------      ----        ----        -------   -------
                // max batches: 1           2       2                3          2            2          2
                // min batches: 1           1       1                1          1            2          1
                // * not possible with treeset
                handleDiffStateOverlap(prev, candidate);
            }
            overlapState = getMergeCandidatePair();
        }
        finalBatchList.addAll(sortedBatches);   // some non overlapping batches might have remained
    }

    /**
     * Compares the non-offset state of 2 batches i.e. the deliveryCount and deliverState.
     * <p>
     * Uses standard compareTo contract x < y => +int, x > y => -int, x == y => 0
     *
     * @param b1 - {@link PersisterStateBatch} to compare
     * @param b2 - {@link PersisterStateBatch} to compare
     * @return int representing comparison result.
     */
    private int compareBatchDeliveryInfo(PersisterStateBatch b1, PersisterStateBatch b2) {
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
     * Accepts a sorted set of state batches and finds the first 2 batches which can be merged.
     * Merged implies that they have some offsets in common or, they are contiguous with the same state.
     * <p>
     * Any non-mergeable batches prefixing a good mergeable pair are removed from the sortedBatches.
     * For example:
     * ----- ----  ----- -----      -----
     *                      ------
     * <---------------> <-------->
     * non-overlapping   1st overlapping pair
     *
     * @return object representing the overlap state
     */
    private MergeCandidatePair getMergeCandidatePair() {
        if (sortedBatches == null || sortedBatches.isEmpty()) {
            return MergeCandidatePair.EMPTY;
        }
        Iterator<PersisterStateBatch> iter = sortedBatches.iterator();
        PersisterStateBatch prev = iter.next();
        List<PersisterStateBatch> nonOverlapping = new LinkedList<>();
        while (iter.hasNext()) {
            PersisterStateBatch candidate = iter.next();
            if (candidate.firstOffset() <= prev.lastOffset() || // overlap
                prev.lastOffset() + 1 == candidate.firstOffset() && compareBatchDeliveryInfo(prev, candidate) == 0) {  // contiguous
                updateBatchContainers(nonOverlapping);
                return new MergeCandidatePair(
                    prev,
                    candidate
                );
            }
            nonOverlapping.add(prev);
            prev = candidate;
        }

        updateBatchContainers(nonOverlapping);
        return MergeCandidatePair.EMPTY;
    }

    private void updateBatchContainers(List<PersisterStateBatch> nonOverlappingBatches) {
        nonOverlappingBatches.forEach(sortedBatches::remove);
        finalBatchList.addAll(nonOverlappingBatches);
    }

    /**
     * Accepts a list of {@link PersisterStateBatch} and checks:
     * - last offset is < start offset => batch is removed
     * - first offset > start offset => batch is preserved
     * - start offset intersects the batch => part of batch before start offset is removed and
     * the part after it is preserved.
     */
    private void pruneBatches() {
        if (startOffset != -1) {
            List<PersisterStateBatch> retainedBatches = new ArrayList<>(combinedBatchList.size());
            combinedBatchList.forEach(batch -> {
                if (batch.lastOffset() < startOffset) {
                    // batch is expired, skip current iteration
                    // -------
                    //         | -> start offset
                    return;
                }

                if (batch.firstOffset() >= startOffset) {
                    // complete batch is valid
                    //    ---------
                    //  | -> start offset
                    retainedBatches.add(batch);
                } else {
                    // start offset intersects batch
                    //   ---------
                    //       |     -> start offset
                    retainedBatches.add(new PersisterStateBatch(startOffset, batch.lastOffset(), batch.deliveryState(), batch.deliveryCount()));
                }
            });
            // update the instance variable
            combinedBatchList = retainedBatches;
        }
    }

    private void handleSameStateMerge(PersisterStateBatch prev, PersisterStateBatch candidate) {
        sortedBatches.add(new PersisterStateBatch(
            prev.firstOffset(),
            // cover cases
            // prev:      ------   --------       ---------
            // candidate:   ---       ----------           -----
            Math.max(candidate.lastOffset(), prev.lastOffset()),
            prev.deliveryState(),
            prev.deliveryCount()
        ));
    }

    private void handleDiffStateOverlap(PersisterStateBatch prev, PersisterStateBatch candidate) {
        if (candidate.firstOffset() == prev.firstOffset()) {
            handleDiffStateOverlapFirstOffsetAligned(prev, candidate);
        } else {    // candidate.firstOffset() > prev.firstOffset()
            handleDiffStateOverlapFirstOffsetNotAligned(prev, candidate);
        }
    }

    private void handleDiffStateOverlapFirstOffsetAligned(PersisterStateBatch prev, PersisterStateBatch candidate) {
        if (candidate.lastOffset() == prev.lastOffset()) {  // case 1
            // candidate can never have lower or equal priority
            // since sortedBatches order takes that into account.
            // -------
            // -------
            sortedBatches.add(candidate);
        } else {
            // case 2 is not possible with TreeSet. It is symmetric to case 3.
            // case 3
            // --------
            // -----------
            if (compareBatchDeliveryInfo(candidate, prev) < 0) {
                sortedBatches.add(prev);
                sortedBatches.add(new PersisterStateBatch(
                    prev.lastOffset() + 1,
                    candidate.lastOffset(),
                    candidate.deliveryState(),
                    candidate.deliveryCount()
                ));
            } else {
                // candidate priority is >= prev
                sortedBatches.add(candidate);
            }
        }
    }

    private void handleDiffStateOverlapFirstOffsetNotAligned(PersisterStateBatch prev, PersisterStateBatch candidate) {
        if (candidate.lastOffset() < prev.lastOffset()) {    // case 4
            handleDiffStateOverlapPrevSwallowsCandidate(prev, candidate);
        } else if (candidate.lastOffset() == prev.lastOffset()) {    // case 5
            handleDiffStateOverlapLastOffsetAligned(prev, candidate);
        } else {    // case 6
            handleDiffStateOverlapCandidateOffsetsLarger(prev, candidate);
        }
    }

    private void handleDiffStateOverlapPrevSwallowsCandidate(PersisterStateBatch prev, PersisterStateBatch candidate) {
        // --------
        //   ----
        if (compareBatchDeliveryInfo(candidate, prev) < 0) {
            sortedBatches.add(prev);
        } else {
            sortedBatches.add(new PersisterStateBatch(
                prev.firstOffset(),
                candidate.firstOffset() - 1,
                prev.deliveryState(),
                prev.deliveryCount()
            ));

            sortedBatches.add(candidate);

            sortedBatches.add(new PersisterStateBatch(
                candidate.lastOffset() + 1,
                prev.lastOffset(),
                prev.deliveryState(),
                prev.deliveryCount()
            ));
        }
    }

    private void handleDiffStateOverlapLastOffsetAligned(PersisterStateBatch prev, PersisterStateBatch candidate) {
        // --------
        //    -----
        if (compareBatchDeliveryInfo(candidate, prev) < 0) {
            sortedBatches.add(prev);
        } else {
            sortedBatches.add(new PersisterStateBatch(
                prev.firstOffset(),
                candidate.firstOffset() - 1,
                prev.deliveryState(),
                prev.deliveryCount()
            ));

            sortedBatches.add(candidate);
        }
    }

    private void handleDiffStateOverlapCandidateOffsetsLarger(PersisterStateBatch prev, PersisterStateBatch candidate) {
        //   -------
        //      -------
        if (compareBatchDeliveryInfo(candidate, prev) < 0) {
            sortedBatches.add(prev);

            sortedBatches.add(new PersisterStateBatch(
                prev.lastOffset() + 1,
                candidate.lastOffset(),
                candidate.deliveryState(),
                candidate.deliveryCount()
            ));
        } else {
            // candidate has higher priority
            sortedBatches.add(new PersisterStateBatch(
                prev.firstOffset(),
                candidate.firstOffset() - 1,
                prev.deliveryState(),
                prev.deliveryCount()
            ));

            sortedBatches.add(candidate);
        }
    }

    /**
     * Holder class for intermediate state
     * used in the batch merge algorithm.
     */
    static class MergeCandidatePair {
        private final PersisterStateBatch prev;
        private final PersisterStateBatch candidate;
        public static final MergeCandidatePair EMPTY = new MergeCandidatePair(null, null);

        public MergeCandidatePair(
            PersisterStateBatch prev,
            PersisterStateBatch candidate
        ) {
            this.prev = prev;
            this.candidate = candidate;
        }

        public PersisterStateBatch prev() {
            return prev;
        }

        public PersisterStateBatch candidate() {
            return candidate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MergeCandidatePair)) return false;
            MergeCandidatePair that = (MergeCandidatePair) o;
            return Objects.equals(prev, that.prev) && Objects.equals(candidate, that.candidate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prev, candidate);
        }
    }
}

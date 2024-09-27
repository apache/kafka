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

import org.apache.kafka.server.share.PersisterStateBatch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PersisterStateBatchCombinerTest {
    static class BatchTestHolder {
        final String testName;
        final List<PersisterStateBatch> curList;
        final List<PersisterStateBatch> newList;
        final List<PersisterStateBatch> expectedResult;
        final long startOffset;
        final boolean shouldRun;

        BatchTestHolder(String testName,
                        List<PersisterStateBatch> curList,
                        List<PersisterStateBatch> newList,
                        List<PersisterStateBatch> expectedResult,
                        long startOffset) {
            this(testName, curList, newList, expectedResult, startOffset, false);
        }

        BatchTestHolder(String testName,
                        List<PersisterStateBatch> curList,
                        List<PersisterStateBatch> newList,
                        List<PersisterStateBatch> expectedResult,
                        long startOffset,
                        boolean shouldRun) {
            this.testName = testName;
            this.curList = curList;
            this.newList = newList;
            this.expectedResult = expectedResult;
            this.startOffset = startOffset;
            this.shouldRun = shouldRun;
        }

        static List<PersisterStateBatch> singleBatch(
            long firstOffset,
            long prevOffset,
            int deliveryState,
            int deliveryCount
        ) {
            return Collections.singletonList(
                new PersisterStateBatch(firstOffset, prevOffset, (byte) deliveryState, (short) deliveryCount)
            );
        }

        static class MultiBatchBuilder {
            private final List<PersisterStateBatch> batchList = new LinkedList<>();

            MultiBatchBuilder addBatch(
                long firstOffset,
                long prevOffset,
                int deliveryState,
                int deliveryCount
            ) {
                batchList.add(new PersisterStateBatch(firstOffset, prevOffset, (byte) deliveryState, (short) deliveryCount));
                return this;
            }

            List<PersisterStateBatch> build() {
                return batchList;
            }
        }

        static MultiBatchBuilder multiBatch() {
            return new MultiBatchBuilder();
        }

        @Override
        public String toString() {
            return this.testName;
        }
    }

    private static Stream<BatchTestHolder> generatorCornerCases() {
        return Stream.of(
            new BatchTestHolder(
                "Current batches with start offset midway are pruned.",
                BatchTestHolder.singleBatch(100, 130, 0, 1),
                Collections.emptyList(),
                BatchTestHolder.singleBatch(120, 130, 0, 1),
                120
            ),

            new BatchTestHolder(
                "New batches with start offset midway are pruned.",
                Collections.emptyList(),
                BatchTestHolder.singleBatch(100, 130, 0, 1),
                BatchTestHolder.singleBatch(120, 130, 0, 1),
                120
            ),

            new BatchTestHolder(
                "Both current and new batches empty.",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                120
            )
        );
    }

    private static Stream<BatchTestHolder> generatorSameState() {
        return Stream.of(
            // same state
            new BatchTestHolder(
                "Same state. prev and candidate have same first and prev offset.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                -1
            ),

            new BatchTestHolder(
                "Same state. prev and candidate have same first offset, candidate prev offset strictly smaller.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 105, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                -1
            ),

            new BatchTestHolder(
                "Same state. prev and candidate have same first offset, candidate prev offset strictly larger.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 115, 0, 1),
                BatchTestHolder.singleBatch(100, 115, 0, 1),
                -1
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset strictly larger and prev offset strictly smaller than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(105, 108, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                -1,
                true
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset strictly larger and prev offset equal to prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(105, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                -1
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset strictly larger and prev offset strictly larger than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(105, 115, 0, 1),
                BatchTestHolder.singleBatch(100, 115, 0, 1),
                -1
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset is prev first offset + 1 (contiguous).",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(111, 115, 0, 1),
                BatchTestHolder.singleBatch(100, 115, 0, 1),
                -1
            )
        );
    }

    private static Stream<BatchTestHolder> generatorComplex() {
        return Stream.of(
            new BatchTestHolder(
                "Handle overlapping batches within each list.",
                BatchTestHolder.multiBatch()
                    .addBatch(100, 110, 0, 1)
                    .addBatch(121, 130, 0, 1)
                    .addBatch(105, 115, 0, 1) // overlap with 1st batch
                    .addBatch(123, 125, 0, 1)  // overlap with 2nd batch
                    .build(),  //[(100-115, 0, 1), (121-130, 0, 1)]
                BatchTestHolder.multiBatch()
                    .addBatch(111, 119, 2, 2)
                    .addBatch(116, 123, 2, 2)  // overlap with first batch
                    .build(),       // ,  //[(111-123, 2, 2)]
                BatchTestHolder.multiBatch()
                    .addBatch(100, 110, 0, 1)
                    .addBatch(111, 123, 2, 2)
                    .addBatch(124, 130, 0, 1)
                    .build(),
                -1
            ),

            new BatchTestHolder(
                "Handle overlapping batches with different priority.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),  //[(100-115, 0, 1), (121-130, 0, 1)]
                BatchTestHolder.multiBatch()
                    .addBatch(101, 105, 1, 2)
                    .addBatch(101, 115, 2, 2)
                    .addBatch(101, 120, 3, 2)  //[(111-123, 2, 2)]
                    .build(),
                BatchTestHolder.multiBatch()
                    .addBatch(100, 100, 0, 1)
                    .addBatch(101, 120, 3, 2)
                    .build(),
                -1
            ),

            new BatchTestHolder(
                "Handle overlapping batches within each list with pruning.",
                BatchTestHolder.multiBatch()
                    .addBatch(100, 110, 0, 1)
                    .addBatch(121, 130, 0, 1)
                    .addBatch(105, 115, 0, 1) // overlap with 1st batch //[(100-115, 0, 1), (121-130, 0, 1)]
                    .build(),
                BatchTestHolder.multiBatch()
                    .addBatch(111, 119, 2, 2)
                    .addBatch(116, 123, 2, 2)  // overlap with first batch //[(111-123, 2, 2)]
                    .build(),
                BatchTestHolder.multiBatch()
                    .addBatch(120, 123, 2, 2)
                    .addBatch(124, 130, 0, 1)
                    .build(),
                120
            ),

            new BatchTestHolder(
                "Multiple higher state batch updates.",
                BatchTestHolder.singleBatch(111, 120, 0, 1),
                BatchTestHolder.multiBatch()
                    .addBatch(111, 113, 0, 2)
                    .addBatch(114, 114, 2, 1)
                    .addBatch(115, 119, 0, 2)  //[(111-123, 2, 2)]
                    .build(),
                BatchTestHolder.multiBatch()
                    .addBatch(111, 113, 0, 2)
                    .addBatch(114, 114, 2, 1)
                    .addBatch(115, 119, 0, 2)
                    .addBatch(120, 120, 0, 1)
                    .build(),
                -1
            )
        );
    }

    private static Stream<BatchTestHolder> generatorDifferentStates() {
        return Stream.of(
            // different states
            new BatchTestHolder(
                "Candidate higher state. Candidate first offset and prev offset match prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 2),
                BatchTestHolder.singleBatch(100, 110, 0, 2),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset and prev offset match prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 3),
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 3),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset same and prev offset smaller than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 105, 0, 2),
                BatchTestHolder.multiBatch()
                    .addBatch(100, 105, 0, 2)
                    .addBatch(106, 110, 0, 1)
                    .build(),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset same and prev offset smaller than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 3),
                BatchTestHolder.singleBatch(100, 105, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 3),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset same and prev offset strictly larger than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 115, 0, 2),
                BatchTestHolder.singleBatch(100, 115, 0, 2),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset same and prev offset strictly larger than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 3),
                BatchTestHolder.singleBatch(100, 115, 0, 1),
                BatchTestHolder.multiBatch()
                    .addBatch(100, 110, 0, 3)
                    .addBatch(111, 115, 0, 1)
                    .build(),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset strictly larger and prev offset strictly smaller than prev.",
                BatchTestHolder.singleBatch(100, 115, 0, 1),
                BatchTestHolder.singleBatch(105, 110, 1, 1),
                BatchTestHolder.multiBatch()
                    .addBatch(100, 104, 0, 1)
                    .addBatch(105, 110, 1, 1)
                    .addBatch(111, 115, 0, 1)
                    .build(),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset strictly larger and prev offset strictly smaller than prev.",
                BatchTestHolder.singleBatch(100, 115, 1, 1),
                BatchTestHolder.singleBatch(105, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 115, 1, 1),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset strictly larger and prev offset same as prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(105, 110, 0, 2),
                BatchTestHolder.multiBatch()
                    .addBatch(100, 104, 0, 1)
                    .addBatch(105, 110, 0, 2)
                    .build(),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset strictly larger and prev offset same as prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 2),
                BatchTestHolder.singleBatch(105, 110, 0, 1),
                BatchTestHolder.singleBatch(100, 110, 0, 2),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first and prev offsets strictly larger than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 1),
                BatchTestHolder.singleBatch(105, 115, 0, 2),
                BatchTestHolder.multiBatch()
                    .addBatch(100, 104, 0, 1)
                    .addBatch(105, 115, 0, 2)
                    .build(),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first and prev offsets strictly larger than prev.",
                BatchTestHolder.singleBatch(100, 110, 0, 2),
                BatchTestHolder.singleBatch(105, 115, 0, 1),
                BatchTestHolder.multiBatch()
                    .addBatch(100, 110, 0, 2)
                    .addBatch(111, 115, 0, 1)
                    .build(),
                -1
            )
        );
    }

    @ParameterizedTest
    @MethodSource("generatorDifferentStates")
    public void testStateBatchCombineDifferentStates(BatchTestHolder test) {
        if (test.shouldRun) {
            assertEquals(test.expectedResult,
                new PersisterStateBatchCombiner(
                    test.curList,
                    test.newList,
                    test.startOffset)
                    .combineStateBatches(),
                test.testName
            );
        }
    }

    @ParameterizedTest
    @MethodSource("generatorSameState")
    public void testStateBatchCombineSameState(BatchTestHolder test) {
        if (test.shouldRun) {
            assertEquals(test.expectedResult,
                new PersisterStateBatchCombiner(
                    test.curList,
                    test.newList,
                    test.startOffset)
                    .combineStateBatches(),
                test.testName
            );
        }
    }

    @ParameterizedTest
    @MethodSource("generatorComplex")
    public void testStateBatchCombineComplexCases(BatchTestHolder test) {
        if (test.shouldRun) {
            assertEquals(test.expectedResult,
                new PersisterStateBatchCombiner(
                    test.curList,
                    test.newList,
                    test.startOffset)
                    .combineStateBatches(),
                test.testName
            );
        }
    }

    @ParameterizedTest
    @MethodSource("generatorCornerCases")
    public void testStateBatchCombineCornerCases(BatchTestHolder test) {
        if (test.shouldRun) {
            assertEquals(test.expectedResult,
                new PersisterStateBatchCombiner(
                    test.curList,
                    test.newList,
                    test.startOffset)
                    .combineStateBatches(),
                test.testName
            );
        }
    }
}

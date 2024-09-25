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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StateBatchUtilTest {
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
            this(testName, curList, newList, expectedResult, startOffset, true);
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

        @Override
        public String toString() {
            return this.testName;
        }
    }

    @SuppressWarnings({"MethodLength"})
    private static Stream<BatchTestHolder> generator() {
        return Stream.of(
            new BatchTestHolder(
                "Current batches with start offset midway are pruned.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 130, (byte) 0, (short) 1)
                ),
                Collections.emptyList(),
                Collections.singletonList(
                    new PersisterStateBatch(120, 130, (byte) 0, (short) 1)
                ),
                120
            ),

            new BatchTestHolder(
                "New batches with start offset midway are pruned.",
                Collections.emptyList(),
                Collections.singletonList(
                    new PersisterStateBatch(100, 130, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(120, 130, (byte) 0, (short) 1)
                ),
                120
            ),

            new BatchTestHolder(
                "Both current and new batches empty.",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                120
            ),

            // same state
            new BatchTestHolder(
                "Same state. Last and candidate have same first and last offset.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Same state. Last and candidate have same first offset, candidate last offset strictly smaller.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 105, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Same state. Last and candidate have same first offset, candidate last offset strictly larger.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset strictly larger and last offset strictly smaller than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 108, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset strictly larger and last offset equal to last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset strictly larger and last offset strictly larger than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 115, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Same state. Candidate first offset is last first offset + 1 (contiguous).",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(111, 115, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 1)
                ),
                -1
            ),

            // different states
            new BatchTestHolder(
                "Candidate higher state. Candidate first offset and last offset match last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 2)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 2)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset and last offset match last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 3)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 3)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset same and last offset smaller than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 105, (byte) 0, (short) 2)
                ),
                Arrays.asList(
                    new PersisterStateBatch(100, 105, (byte) 0, (short) 2),
                    new PersisterStateBatch(106, 110, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset same and last offset smaller than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 3)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 105, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 3)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset same and last offset strictly larger than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 2)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 2)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset same and last offset strictly larger than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 3)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 1)
                ),
                Arrays.asList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 3),
                    new PersisterStateBatch(111, 115, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset strictly larger and last offset strictly smaller than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 110, (byte) 1, (short) 1)
                ),
                Arrays.asList(
                    new PersisterStateBatch(100, 104, (byte) 0, (short) 1),
                    new PersisterStateBatch(105, 110, (byte) 1, (short) 1),
                    new PersisterStateBatch(111, 115, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset strictly larger and last offset strictly smaller than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 1, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 115, (byte) 1, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first offset strictly larger and last offset same as last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 110, (byte) 0, (short) 2)
                ),
                Arrays.asList(
                    new PersisterStateBatch(100, 104, (byte) 0, (short) 1),
                    new PersisterStateBatch(105, 110, (byte) 0, (short) 2)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first offset strictly larger and last offset same as last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 2)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 2)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate higher state. Candidate first and last offsets strictly larger than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 115, (byte) 0, (short) 2)
                ),
                Arrays.asList(
                    new PersisterStateBatch(100, 104, (byte) 0, (short) 1),
                    new PersisterStateBatch(105, 115, (byte) 0, (short) 2)
                ),
                -1
            ),

            new BatchTestHolder(
                "Candidate lower state. Candidate first and last offsets strictly larger than last.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 2)
                ),
                Collections.singletonList(
                    new PersisterStateBatch(105, 115, (byte) 0, (short) 1)
                ),
                Arrays.asList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 2),
                    new PersisterStateBatch(111, 115, (byte) 0, (short) 1)
                ),
                -1
            ),

            // random cases
            new BatchTestHolder(
                "Handle overlapping batches within each list.",
                Arrays.asList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1),
                    new PersisterStateBatch(121, 130, (byte) 0, (short) 1),
                    new PersisterStateBatch(105, 115, (byte) 0, (short) 1), // overlap with 1st batch
                    new PersisterStateBatch(123, 125, (byte) 0, (short) 1)  // overlap with 2nd batch

                ),  //[(100-115, 0, 1), (121-130, 0, 1)]
                Arrays.asList(
                    new PersisterStateBatch(111, 119, (byte) 2, (short) 2),
                    new PersisterStateBatch(116, 123, (byte) 2, (short) 2)  // overlap with first batch
                ),  //[(111-123, 2, 2)]
                Arrays.asList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1),
                    new PersisterStateBatch(111, 123, (byte) 2, (short) 2),
                    new PersisterStateBatch(124, 130, (byte) 0, (short) 1)
                ),
                -1
            ),

            new BatchTestHolder(
                "Handle overlapping batches with different priority.",
                Collections.singletonList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1)

                ),  //[(100-115, 0, 1), (121-130, 0, 1)]
                Arrays.asList(
                    new PersisterStateBatch(101, 105, (byte) 1, (short) 2),
                    new PersisterStateBatch(101, 115, (byte) 2, (short) 2),
                    new PersisterStateBatch(101, 120, (byte) 3, (short) 2)
                ),  //[(111-123, 2, 2)]
                Arrays.asList(
                    new PersisterStateBatch(100, 100, (byte) 0, (short) 1),
                    new PersisterStateBatch(101, 120, (byte) 3, (short) 2)
                ),
                -1
            ),

            new BatchTestHolder(
                "Handle overlapping batches within each list with pruning.",
                Arrays.asList(
                    new PersisterStateBatch(100, 110, (byte) 0, (short) 1),
                    new PersisterStateBatch(121, 130, (byte) 0, (short) 1),
                    new PersisterStateBatch(105, 115, (byte) 0, (short) 1) // overlap with 1st batch
                ),  //[(100-115, 0, 1), (121-130, 0, 1)]
                Arrays.asList(
                    new PersisterStateBatch(111, 119, (byte) 2, (short) 2),
                    new PersisterStateBatch(116, 123, (byte) 2, (short) 2)  // overlap with first batch
                ),  //[(111-123, 2, 2)]
                Arrays.asList(
                    new PersisterStateBatch(120, 123, (byte) 2, (short) 2),
                    new PersisterStateBatch(124, 130, (byte) 0, (short) 1)
                ),
                120
            ),

            // complex tests
            new BatchTestHolder(
                "Multiple higher state batch updates.",
                Collections.singletonList(
                    new PersisterStateBatch(111, 120, (byte) 0, (short) 1)
                ),
                Arrays.asList(
                    new PersisterStateBatch(111, 113, (byte) 0, (short) 2),
                    new PersisterStateBatch(114, 114, (byte) 2, (short) 1),
                    new PersisterStateBatch(115, 119, (byte) 0, (short) 2)
                ),  //[(111-123, 2, 2)]
                Arrays.asList(
                    new PersisterStateBatch(111, 113, (byte) 0, (short) 2),
                    new PersisterStateBatch(114, 114, (byte) 2, (short) 1),
                    new PersisterStateBatch(115, 119, (byte) 0, (short) 2),
                    new PersisterStateBatch(120, 120, (byte) 0, (short) 1)
                ),
                -1
            )
        );
    }

    @ParameterizedTest
    @MethodSource("generator")
    public void testStateBatchCombine(BatchTestHolder test) {
        if (test.shouldRun) {
            assertEquals(test.expectedResult,
                StateBatchUtil.combineStateBatches(
                    test.curList,
                    test.newList,
                    test.startOffset),
                test.testName
            );
        }
    }
}

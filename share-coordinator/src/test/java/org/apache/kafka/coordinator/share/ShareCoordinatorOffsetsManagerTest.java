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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ShareCoordinatorOffsetsManagerTest {

    private ShareCoordinatorOffsetsManager manager;
    private static final SharePartitionKey KEY1 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 0);
    private static final SharePartitionKey KEY2 = SharePartitionKey.getInstance("gs2", Uuid.randomUuid(), 0);
    private static final SharePartitionKey KEY3 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 1);
    private static final SharePartitionKey KEY4 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 7);

    @BeforeEach
    public void setUp() {
        manager = new ShareCoordinatorOffsetsManager(new SnapshotRegistry(new LogContext()));
    }

    @Test
    public void testUpdateStateAddsToInternalState() {
        manager.updateState(KEY1, 0L, false);
        assertEquals(Optional.empty(), manager.lastRedundantOffset());

        manager.updateState(KEY1, 10L, false);
        assertEquals(Optional.of(10L), manager.lastRedundantOffset()); // [0-9] offsets are redundant.

        manager.updateState(KEY2, 15L, false);
        assertEquals(Optional.of(10L), manager.lastRedundantOffset());  // No update to last redundant after adding 15L so, still 10L.

        manager.updateState(KEY1, 25L, true);
        assertEquals(Optional.of(15L), manager.lastRedundantOffset());  // KEY1 deleted, no longer part of calculation.

        assertNull(manager.curState().get(KEY1));
        assertEquals(15L, manager.curState().get(KEY2));
    }

    private static class ShareOffsetTestHolder {
        static class TestTuple {
            final SharePartitionKey key;
            final long offset;
            final Optional<Long> expectedOffset;
            final boolean isDelete;

            private TestTuple(SharePartitionKey key, long offset, Optional<Long> expectedOffset, boolean isDelete) {
                this.key = key;
                this.offset = offset;
                this.expectedOffset = expectedOffset;
                this.isDelete = isDelete;
            }

            static TestTuple instance(SharePartitionKey key, long offset, Optional<Long> expectedOffset) {
                return new TestTuple(key, offset, expectedOffset, false);
            }

            static TestTuple instance(SharePartitionKey key, long offset, Optional<Long> expectedOffset, boolean isDelete) {
                return new TestTuple(key, offset, expectedOffset, isDelete);
            }
        }

        private final String testName;
        private final List<TestTuple> tuples;
        private final boolean shouldRun;

        ShareOffsetTestHolder(String testName, List<TestTuple> tuples) {
            this(testName, tuples, true);
        }

        ShareOffsetTestHolder(String testName, List<TestTuple> tuples, boolean shouldRun) {
            this.testName = testName;
            this.tuples = tuples;
            this.shouldRun = shouldRun;
        }
    }

    static Stream<ShareOffsetTestHolder> generateNoRedundantStateCases() {
        return Stream.of(
            new ShareOffsetTestHolder(
                "no redundant state single key.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L))
                )
            ),

            new ShareOffsetTestHolder(
                "no redundant state single key with delete.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L), true)
                )
            ),

            new ShareOffsetTestHolder(
                "no redundant state multiple keys.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY4, 11L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 13L, Optional.of(10L))
                )
            ),

            new ShareOffsetTestHolder(
                "no redundant state multiple keys with delete.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L), true),
                    ShareOffsetTestHolder.TestTuple.instance(KEY4, 11L, Optional.of(11L), true),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 13L, Optional.of(13L), true)
                )
            )
        );
    }

    static Stream<ShareOffsetTestHolder> generateRedundantStateCases() {
        return Stream.of(
            new ShareOffsetTestHolder(
                "redundant state single key.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 11L, Optional.of(11L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 15L, Optional.of(15L))
                )
            ),

            new ShareOffsetTestHolder(
                "redundant state multiple keys.",
                // KEY1: 10 17
                // KEY2: 11 16
                // KEY3: 15
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 16L, Optional.of(10L)),  // KEY2 11 redundant but should not be returned
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 17L, Optional.of(15L))
                )
            ),

            new ShareOffsetTestHolder(
                "redundant state multiple keys with delete.",
                // KEY1: 10 17
                // KEY2: 11 16
                // KEY3: 15
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.of(10L), true),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 16L, Optional.of(10L)),  // KEY2 11 redundant but should not be returned
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 17L, Optional.of(16L))   // Because we have removed KEY3 from calculation
                )
            )
        );

    }

    static Stream<ShareOffsetTestHolder> generateComplexCases() {
        return Stream.of(
            new ShareOffsetTestHolder(
                "redundant state reverse key order.",
                // Requests come in order KEY1, KEY2, KEY3, KEY3, KEY2, KEY1.
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 18L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 20L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 25L, Optional.of(18L))
                )
            ),

            new ShareOffsetTestHolder(
                "redundant state reverse key order with delete.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 18L, Optional.of(10L), true),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 20L, Optional.of(10L), true),
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 25L, Optional.of(25L))   // Because KEY2 and KEY3 are gone.
                )
            ),

            new ShareOffsetTestHolder(
                "redundant state infrequently written partition.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 18L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 20L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 22L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 25L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 27L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 28L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 30L, Optional.of(27L))
                )
            ),

            new ShareOffsetTestHolder(
                "redundant state infrequently written partition with delete.",
                List.of(
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 18L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY3, 20L, Optional.of(10L), true),    //KEY3 no longer party to calculation
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 22L, Optional.of(10L)),
                    ShareOffsetTestHolder.TestTuple.instance(KEY2, 27L, Optional.of(10L), true),    //KEY2 no longer party to calculation
                    ShareOffsetTestHolder.TestTuple.instance(KEY1, 30L, Optional.of(30L))
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("generateNoRedundantStateCases")
    public void testUpdateStateNoRedundantState(ShareOffsetTestHolder holder) {
        if (holder.shouldRun) {
            holder.tuples.forEach(tuple -> {
                manager.updateState(tuple.key, tuple.offset, tuple.isDelete);
                assertEquals(tuple.expectedOffset, manager.lastRedundantOffset(), holder.testName);
            });
        }
    }

    @ParameterizedTest
    @MethodSource("generateRedundantStateCases")
    public void testUpdateStateRedundantState(ShareOffsetTestHolder holder) {
        if (holder.shouldRun) {
            holder.tuples.forEach(tuple -> {
                manager.updateState(tuple.key, tuple.offset, tuple.isDelete);
                assertEquals(tuple.expectedOffset, manager.lastRedundantOffset(), holder.testName);
            });
        }
    }

    @ParameterizedTest
    @MethodSource("generateComplexCases")
    public void testUpdateStateComplexCases(ShareOffsetTestHolder holder) {
        if (holder.shouldRun) {
            holder.tuples.forEach(tuple -> {
                manager.updateState(tuple.key, tuple.offset, tuple.isDelete);
                assertEquals(tuple.expectedOffset, manager.lastRedundantOffset(), holder.testName);
            });
        }
    }
}
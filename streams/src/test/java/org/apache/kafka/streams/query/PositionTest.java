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

package org.apache.kafka.streams.query;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PositionTest {

    private static final Random RANDOM = new Random();

    @Test
    public void shouldCreateFromMap() {
        final Map<String, Map<Integer, Long>> map = mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        );

        final Position position = Position.fromMap(map);
        assertEquals(Set.of("topic", "topic1"), position.getTopics());
        assertEquals(mkMap(mkEntry(0, 5L)), position.getPartitionPositions("topic"));

        // Should be a copy of the constructor map

        map.get("topic1").put(99, 99L);

        // so the position is still the original one
        assertEquals(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L)
        ), position.getPartitionPositions("topic1"));
    }

    @Test
    public void shouldCreateFromNullMap() {
        final Position position = Position.fromMap(null);
        assertEquals(Collections.emptySet(), position.getTopics());
    }

    @Test
    public void shouldMerge() {
        final Position position = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        ));

        final Position position1 = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 7L))), // update offset
            mkEntry("topic1", mkMap(mkEntry(8, 1L))), // add partition
            mkEntry("topic2", mkMap(mkEntry(9, 5L))) // add topic
        ));

        final Position merged = position.merge(position1);

        assertEquals(Set.of("topic", "topic1", "topic2"), merged.getTopics());
        assertEquals(mkMap(mkEntry(0, 7L)), merged.getPartitionPositions("topic"));
        assertEquals(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L),
            mkEntry(8, 1L)
        ), merged.getPartitionPositions("topic1"));
        assertEquals(mkMap(mkEntry(9, 5L)), merged.getPartitionPositions("topic2"));
    }

    @Test
    public void shouldUpdateComponentMonotonically() {
        final Position position = Position.emptyPosition();
        position.withComponent("topic", 3, 5L);
        position.withComponent("topic", 3, 4L);
        assertEquals(5L, position.getPartitionPositions("topic").get(3));
        position.withComponent("topic", 3, 6L);
        assertEquals(6L, position.getPartitionPositions("topic").get(3));
    }

    @Test
    public void shouldCopy() {
        final Position position = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        ));

        final Position copy = position.copy();

        // mutate original
        position.withComponent("topic", 0, 6L);
        position.withComponent("topic1", 8, 1L);
        position.withComponent("topic2", 2, 4L);

        // copy has not changed
        assertEquals(Set.of("topic", "topic1"), copy.getTopics());
        assertEquals(mkMap(mkEntry(0, 5L)), copy.getPartitionPositions("topic"));
        assertEquals(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L)
        ), copy.getPartitionPositions("topic1"));

        // original has changed
        assertEquals(Set.of("topic", "topic1", "topic2"), position.getTopics());
        assertEquals(mkMap(mkEntry(0, 6L)), position.getPartitionPositions("topic"));
        assertEquals(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L),
            mkEntry(8, 1L)
        ), position.getPartitionPositions("topic1"));
        assertEquals(mkMap(mkEntry(2, 4L)), position.getPartitionPositions("topic2"));
    }

    @Test
    public void shouldMergeNull() {
        final Position position = Position.fromMap(mkMap(
            mkEntry("topic", mkMap(mkEntry(0, 5L))),
            mkEntry("topic1", mkMap(
                mkEntry(0, 5L),
                mkEntry(7, 0L)
            ))
        ));

        final Position merged = position.merge(null);

        assertEquals(Set.of("topic", "topic1"), merged.getTopics());
        assertEquals(mkMap(mkEntry(0, 5L)), merged.getPartitionPositions("topic"));
        assertEquals(mkMap(
            mkEntry(0, 5L),
            mkEntry(7, 0L)
        ), merged.getPartitionPositions("topic1"));
    }

    @Test
    public void shouldMatchOnEqual() {
        final Position position1 = Position.emptyPosition();
        final Position position2 = Position.emptyPosition();
        position1.withComponent("topic1", 0, 1);
        position2.withComponent("topic1", 0, 1);

        position1.withComponent("topic1", 1, 2);
        position2.withComponent("topic1", 1, 2);

        position1.withComponent("topic1", 2, 1);
        position2.withComponent("topic1", 2, 1);

        position1.withComponent("topic2", 0, 0);
        position2.withComponent("topic2", 0, 0);

        assertEquals(position1, position2);
    }

    @Test
    public void shouldNotMatchOnUnEqual() {
        final Position position1 = Position.emptyPosition();
        final Position position2 = Position.emptyPosition();
        position1.withComponent("topic1", 0, 1);
        position2.withComponent("topic1", 0, 1);

        position1.withComponent("topic1", 1, 2);

        position1.withComponent("topic1", 2, 1);
        position2.withComponent("topic1", 2, 1);

        position1.withComponent("topic2", 0, 0);
        position2.withComponent("topic2", 0, 0);

        assertNotEquals(position1, position2);
    }

    @Test
    public void shouldNotMatchNull() {
        final Position position = Position.emptyPosition();
        assertNotEquals(position, null);
    }

    @Test
    public void shouldMatchSelf() {
        final Position position = Position.emptyPosition();
        assertEquals(position, position);
    }

    @Test
    public void shouldNotHash() {
        final Position position = Position.emptyPosition();
        assertThrows(UnsupportedOperationException.class, position::hashCode);

        // going overboard...
        final HashSet<Position> set = new HashSet<>();
        assertThrows(UnsupportedOperationException.class, () -> set.add(position));

        final HashMap<Position, Integer> map = new HashMap<>();
        assertThrows(UnsupportedOperationException.class, () -> map.put(position, 5));
    }

    @Test
    public void shouldMonotonicallyIncreasePartitionPosition() throws InterruptedException, ExecutionException, TimeoutException {
        final int threadCount = 10;
        final int maxTopics = 50;
        final int maxPartitions = 50;
        final int maxOffset = 1000;
        final CountDownLatch startLatch = new CountDownLatch(threadCount);
        final Position mergePosition = Position.emptyPosition();
        final Position withComponentPosition = Position.emptyPosition();
        final List<Future<?>> futures = new ArrayList<>();
        ExecutorService executorService = null;

        try {
            executorService = Executors.newFixedThreadPool(threadCount);

            for (int i = 0; i < threadCount; i++) {
                futures.add(executorService.submit(() -> {
                    final Position threadPosition = Position.emptyPosition();
                    final int topicCount = RANDOM.nextInt(maxTopics) + 1;

                    // build the thread's position
                    for (int topicNum = 0; topicNum < topicCount; topicNum++) {
                        final String topic = "topic-" + topicNum;
                        final int partitionCount = RANDOM.nextInt(maxPartitions) + 1;
                        for (int partitionNum = 0; partitionNum < partitionCount; partitionNum++) {
                            final long offset = RANDOM.nextInt(maxOffset) + 1;
                            threadPosition.withComponent(topic, partitionNum, offset);
                        }
                    }

                    startLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (final InterruptedException e) {
                        // convert to unchecked exception so the future completes exceptionally and fails the test
                        throw new RuntimeException(e);
                    }

                    // merge with the shared position
                    mergePosition.merge(threadPosition);
                    // duplicate the shared position to get a snapshot of its state
                    final Position threadMergePositionState = mergePosition.copy();

                    // update the shared position using withComponent
                    for (final String topic : threadPosition.getTopics()) {
                        for (final Map.Entry<Integer, Long> partitionOffset : threadPosition
                                .getPartitionPositions(topic)
                                .entrySet()) {
                            withComponentPosition.withComponent(topic, partitionOffset.getKey(), partitionOffset.getValue());
                        }
                    }
                    // duplicate the shared position to get a snapshot of its state
                    final Position threadWithComponentPositionState = withComponentPosition.copy();

                    // validate that any offsets in the merged position and the withComponent position are >= the thread position
                    for (final String topic : threadPosition.getTopics()) {
                        final Map<Integer, Long> threadOffsets = threadPosition.getPartitionPositions(topic);
                        final Map<Integer, Long> mergedOffsets = threadMergePositionState.getPartitionPositions(topic);
                        final Map<Integer, Long> withComponentOffsets = threadWithComponentPositionState.getPartitionPositions(topic);

                        for (final Map.Entry<Integer, Long> threadOffset : threadOffsets.entrySet()) {
                            final int partition = threadOffset.getKey();
                            final long offsetValue = threadOffset.getValue();

                            // merge checks
                            assertTrue(
                                    mergedOffsets.containsKey(partition),
                                    "merge method failure. Missing partition " + partition + " for topic " + topic
                            );
                            assertTrue(
                                    mergedOffsets.get(partition) >= offsetValue,
                                    "merge method failure. Offset for topic " +
                                            topic +
                                            " partition " +
                                            partition +
                                            " expected >= " +
                                            offsetValue +
                                            " but got " +
                                            mergedOffsets.get(partition)
                            );

                            // withComponent checks
                            assertTrue(
                                    withComponentOffsets.containsKey(partition),
                                    "withComponent method failure. Missing partition " + partition + " for topic " + topic
                            );
                            assertTrue(
                                    withComponentOffsets.get(partition) >= offsetValue,
                                    "withComponent method failure. Offset for topic " +
                                            topic +
                                            " partition " +
                                            partition +
                                            " expected >= " +
                                            offsetValue +
                                            " but got " +
                                            withComponentOffsets.get(partition)
                            );
                        }
                    }
                }));
            }

            for (final Future<?> future : futures) {
                // Wait for all threads to complete
                future.get(1, TimeUnit.SECONDS); // Check for exceptions
            }
        } finally {
            if (executorService != null) {
                executorService.shutdown();
                assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }
}

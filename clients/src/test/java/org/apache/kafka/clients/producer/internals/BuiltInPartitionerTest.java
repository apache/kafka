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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BuiltInPartitionerTest {
    private final static Node[] NODES = new Node[] {
        new Node(0, "localhost", 99),
        new Node(1, "localhost", 100),
        new Node(2, "localhost", 101),
        new Node(11, "localhost", 102)
    };
    final static String TOPIC_A = "topicA";
    final static String TOPIC_B = "topicB";
    final static String TOPIC_C = "topicC";
    final LogContext logContext = new LogContext();

    @AfterEach
    public void tearDown() {
        BuiltInPartitioner.mockRandom = null;
    }

    @Test
    public void testStickyPartitioning() {
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(TOPIC_A, 0, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_A, 1, NODES[1], NODES, NODES),
            new PartitionInfo(TOPIC_A, 2, NODES[2], NODES, NODES),
            new PartitionInfo(TOPIC_B, 0, NODES[0], NODES, NODES)
        );
        Cluster testCluster = new Cluster("clusterId", asList(NODES), allPartitions,
            Collections.emptySet(), Collections.emptySet());

        // Create partitions with "sticky" batch size to accommodate 3 records.
        BuiltInPartitioner builtInPartitionerA = new BuiltInPartitioner(logContext, TOPIC_A, 3);

        // Test the partition is not switched until sticky batch size is reached.
        // Mock random number generator with just sequential integer.
        AtomicInteger mockRandom = new AtomicInteger();
        BuiltInPartitioner.mockRandom = () -> mockRandom.getAndAdd(1);

        BuiltInPartitioner.StickyPartitionInfo partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster);
        int partA = partitionInfo.partition();
        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster);

        partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster);
        assertEquals(partA, partitionInfo.partition());
        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster);

        partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster);
        assertEquals(partA, partitionInfo.partition());
        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster);

        // After producing 3 records, partition must've switched.
        assertNotEquals(partA, builtInPartitionerA.peekCurrentPartitionInfo(testCluster).partition());

        // Check that switching works even when there is one partition.
        BuiltInPartitioner builtInPartitionerB = new BuiltInPartitioner(logContext, TOPIC_B, 1);
        for (int c = 10; c-- > 0; ) {
            partitionInfo = builtInPartitionerB.peekCurrentPartitionInfo(testCluster);
            assertEquals(0, partitionInfo.partition());
            builtInPartitionerB.updatePartitionInfo(partitionInfo, 1, testCluster);
        }
    }

    @Test
    public void unavailablePartitionsTest() {
        // Partition 1 in topic A, partition 0 in topic B and partition 0 in topic C are unavailable partitions.
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(TOPIC_A, 0, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_A, 1, null, NODES, NODES),
            new PartitionInfo(TOPIC_A, 2, NODES[2], NODES, NODES),
            new PartitionInfo(TOPIC_B, 0, null, NODES, NODES),
            new PartitionInfo(TOPIC_B, 1, NODES[0], NODES, NODES),
            new PartitionInfo(TOPIC_C, 0, null, NODES, NODES)
        );

        Cluster testCluster = new Cluster("clusterId", asList(NODES[0], NODES[1], NODES[2]), allPartitions,
            Collections.emptySet(), Collections.emptySet());

        // Create partitions with "sticky" batch size to accommodate 1 record.
        BuiltInPartitioner builtInPartitionerA = new BuiltInPartitioner(logContext, TOPIC_A, 1);

        // Assure we never choose partition 1 because it is unavailable.
        BuiltInPartitioner.StickyPartitionInfo partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster);
        int partA = partitionInfo.partition();
        builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster);

        boolean foundAnotherPartA = false;
        assertNotEquals(1, partA);
        for (int aPartitions = 0; aPartitions < 100; aPartitions++) {
            partitionInfo = builtInPartitionerA.peekCurrentPartitionInfo(testCluster);
            int anotherPartA = partitionInfo.partition();
            builtInPartitionerA.updatePartitionInfo(partitionInfo, 1, testCluster);

            assertNotEquals(1, anotherPartA);
            foundAnotherPartA = foundAnotherPartA || anotherPartA != partA;
        }
        assertTrue(foundAnotherPartA, "Expected to find partition other than " + partA);

        BuiltInPartitioner builtInPartitionerB = new BuiltInPartitioner(logContext, TOPIC_B, 1);
        // Assure we always choose partition 1 for topic B.
        partitionInfo = builtInPartitionerB.peekCurrentPartitionInfo(testCluster);
        int partB = partitionInfo.partition();
        builtInPartitionerB.updatePartitionInfo(partitionInfo, 1, testCluster);

        assertEquals(1, partB);
        for (int bPartitions = 0; bPartitions < 100; bPartitions++) {
            partitionInfo = builtInPartitionerB.peekCurrentPartitionInfo(testCluster);
            assertEquals(1, partitionInfo.partition());
            builtInPartitionerB.updatePartitionInfo(partitionInfo, 1, testCluster);
        }

        // Assure that we still choose the partition when there are no partitions available.
        BuiltInPartitioner builtInPartitionerC = new BuiltInPartitioner(logContext, TOPIC_C, 1);
        partitionInfo = builtInPartitionerC.peekCurrentPartitionInfo(testCluster);
        int partC = partitionInfo.partition();
        builtInPartitionerC.updatePartitionInfo(partitionInfo, 1, testCluster);
        assertEquals(0, partC);

        partitionInfo = builtInPartitionerC.peekCurrentPartitionInfo(testCluster);
        partC = partitionInfo.partition();
        assertEquals(0, partC);
    }

    @Test
    public void adaptivePartitionsTest() {
        // Mock random number generator with just sequential integer.
        AtomicInteger mockRandom = new AtomicInteger();
        BuiltInPartitioner.mockRandom = () -> mockRandom.getAndAdd(1);

        BuiltInPartitioner builtInPartitioner = new BuiltInPartitioner(logContext, TOPIC_A, 1);

        // Simulate partition queue sizes.
        int[] queueSizes = {5, 0, 3, 0, 1};
        int[] partitionIds = new int[queueSizes.length];
        int[] expectedFrequencies = new int[queueSizes.length];
        List<PartitionInfo> allPartitions = new ArrayList<>();
        for (int i = 0; i < partitionIds.length; i++) {
            partitionIds[i] = i;
            allPartitions.add(new PartitionInfo(TOPIC_A, i, NODES[i % NODES.length], NODES, NODES));
            expectedFrequencies[i] = 6 - queueSizes[i];  // 6 is max(queueSizes) + 1
        }

        builtInPartitioner.updatePartitionLoadStats(queueSizes, partitionIds, queueSizes.length);

        Cluster testCluster = new Cluster("clusterId", asList(NODES), allPartitions,
            Collections.emptySet(), Collections.emptySet());

        // Issue a certain number of partition calls to validate that the partitions would be
        // distributed with frequencies that are reciprocal to the queue sizes.  The number of
        // iterations is defined by the last element of the cumulative frequency table which is
        // the sum of all frequencies.  We do 2 cycles, just so it's more than 1.
        final int numberOfCycles = 2;
        int numberOfIterations = builtInPartitioner.loadStatsRangeEnd() * numberOfCycles;
        int[] frequencies = new int[queueSizes.length];

        for (int i = 0; i < numberOfIterations; i++) {
            BuiltInPartitioner.StickyPartitionInfo partitionInfo = builtInPartitioner.peekCurrentPartitionInfo(testCluster);
            ++frequencies[partitionInfo.partition()];
            builtInPartitioner.updatePartitionInfo(partitionInfo, 1, testCluster);
        }

        // Verify that frequencies are reciprocal of queue sizes.
        for (int i = 0; i < frequencies.length; i++) {
            assertEquals(expectedFrequencies[i] * numberOfCycles, frequencies[i],
                "Partition " + i + " was chosen " + frequencies[i] + " times");
        }
    }

    @Test
    void testStickyBatchSizeMoreThatZero() {
        assertThrows(IllegalArgumentException.class, () -> new BuiltInPartitioner(logContext, TOPIC_A, 0));
        assertDoesNotThrow(() -> new BuiltInPartitioner(logContext, TOPIC_A, 1));
    }
}

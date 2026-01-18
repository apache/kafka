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

import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdaptivePartitioningObservabilityTest {
    private static final Node[] NODES = new Node[]{new Node(0, "localhost", 99), new Node(1, "localhost", 100), new Node(2, "localhost", 101)};
    static final String TOPIC_A = "topicA";
    static final String TOPIC_B = "topicB";
    final LogContext logContext = new LogContext();

    @Test
    public void testAdaptivePartitioningOccurs() {
        LongAdder switches = new LongAdder();
        BuiltInPartitioner builtInPartitioner = new SequentialPartitioner(logContext, TOPIC_A, 1, switches);

        int[] queueSizes = {5, 0, 3};
        int[] partitionIds = {0, 1, 2};
        List<PartitionInfo> allPartitions = new ArrayList<>();
        for (int i = 0; i < partitionIds.length; i++) {
            allPartitions.add(new PartitionInfo(TOPIC_A, i, NODES[i % NODES.length], NODES, NODES));
        }

        builtInPartitioner.updatePartitionLoadStats(queueSizes, partitionIds, queueSizes.length);
        Cluster testCluster = new Cluster("clusterId", asList(NODES), allPartitions, Collections.emptySet(), Collections.emptySet());

        int[] frequencies = new int[queueSizes.length];
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            BuiltInPartitioner.StickyPartitionInfo partitionInfo = builtInPartitioner.peekCurrentPartitionInfo(testCluster);
            frequencies[partitionInfo.partition()]++;
            builtInPartitioner.updatePartitionInfo(partitionInfo, 1, testCluster);
        }

        assertTrue(frequencies[1] > frequencies[0]);
        assertTrue(frequencies[1] > frequencies[2]);
        assertTrue(switches.sum() >= iterations);

        double skew = builtInPartitioner.loadSkew();
        assertEquals(5.0, skew, "Partition load skew should be 5.0");
    }

    @Test
    public void testExcludedPartitionsMetric() throws Exception {
        long availabilityTimeoutMs = 100;
        Metrics metrics = new Metrics();
        Time time = new MockTime();

        RecordAccumulator.PartitionerConfig config = new RecordAccumulator.PartitionerConfig(true, availabilityTimeoutMs);
        RecordAccumulator accum = new RecordAccumulator(logContext, 1024, Compression.of(CompressionType.NONE).build(), 0, 0L, 0L, 60000, config, metrics, "producer-metrics", time, null, new BufferPool(1024 * 10, 1024, metrics, time, "producer-internal-metrics"));

        List<PartitionInfo> partitions = asList(new PartitionInfo(TOPIC_B, 0, NODES[0], NODES, NODES));
        Cluster cluster = new Cluster("clusterId", asList(NODES), partitions, Collections.emptySet(), Collections.emptySet());


        accum.append(TOPIC_B, RecordMetadata.UNKNOWN_PARTITION, 0L, null, new byte[10], null, null, 1000L, time.milliseconds(), cluster);

        int nodeId = 0;
        accum.updateNodeLatencyStats(nodeId, time.milliseconds(), true);
        time.sleep(availabilityTimeoutMs + 1);
        accum.updateNodeLatencyStats(nodeId, time.milliseconds(), false);

        TopicPartition tp = new TopicPartition(TOPIC_B, 0);
        List<Integer> replicaIds = Arrays.asList(0, 1);
        List<Integer> inSyncReplicaIds = Arrays.asList(0, 1);
        PartitionMetadata partitionMetadata = new PartitionMetadata(Errors.NONE, tp, Optional.of(NODES[0].id()), Optional.empty(), replicaIds, inSyncReplicaIds, Collections.emptyList());

        Map<Integer, Node> nodeMap = Arrays.stream(NODES).collect(Collectors.toMap(Node::id, n -> n));

        MetadataSnapshot metadataSnapshot = new MetadataSnapshot("clusterId", nodeMap, Collections.singletonList(partitionMetadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap(), cluster);

        accum.ready(metadataSnapshot, time.milliseconds());

        assertEquals(1.0, (Double) metrics.metric(metrics.metricName("adaptive-partition-unavailable-total", "producer-metrics")).metricValue(), 0.01);

        Map<String, String> tags = Collections.singletonMap("topic", TOPIC_B);
        assertEquals(1.0, (Double) metrics.metric(metrics.metricName("adaptive-partition-unavailable-total", "producer-topic-metrics", tags)).metricValue(), 0.01);

        accum.close();
        metrics.close();
    }

    private static class SequentialPartitioner extends BuiltInPartitioner {
        AtomicInteger mockRandom = new AtomicInteger();

        public SequentialPartitioner(LogContext logContext, String topic, int stickyBatchSize, LongAdder switches) {
            super(logContext, topic, stickyBatchSize, switches);
        }

        @Override
        int randomPartition() {
            return mockRandom.getAndAdd(1);
        }
    }
}

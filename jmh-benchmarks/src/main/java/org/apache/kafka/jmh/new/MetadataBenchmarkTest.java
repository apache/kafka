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
package org.apache.kafka.clients;

import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.RequestTestUtils.PartitionMetadataSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.MockClusterResourceListener;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MetadataBenchmarkTest {

    @Param({"10", "50", "100", "500", "1000", "5000", "10000"})
    private int threadCount;
    
    private long refreshBackoffMs = 100;
    private long refreshBackoffMaxMs = 1000;
    private long metadataExpireMs = 1000;
    private Metadata metadata = new Metadata(refreshBackoffMs, refreshBackoffMaxMs,
            metadataExpireMs, new LogContext(), new ClusterResourceListeners());
    
    // From MetadataTest
    @Benchmark
    public void testConcurrentUpdateAndFetchForSnapshotAndCluster() throws InterruptedException {
        Time time = new MockTime();
        metadata = new Metadata(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, new LogContext(), new ClusterResourceListeners());

        // Setup metadata with 10 nodes, 2 topics, topic1 & 2, both to be retained in the update. Both will have leader-epoch 100.
        int oldNodeCount = 10;
        String topic1 = "test_topic1";
        String topic2 = "test_topic2";
        TopicPartition topic1Part0 = new TopicPartition(topic1, 0);
        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        int oldPartitionCount = 1;
        topicPartitionCounts.put(topic1, oldPartitionCount);
        topicPartitionCounts.put(topic2, oldPartitionCount);
        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put(topic1, Uuid.randomUuid());
        topicIds.put(topic2, Uuid.randomUuid());
        int oldLeaderEpoch = 100;
        MetadataResponse metadataResponse =
            RequestTestUtils.metadataUpdateWithIds("cluster", oldNodeCount, Collections.emptyMap(), topicPartitionCounts, _tp -> oldLeaderEpoch, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, true, time.milliseconds());
        MetadataSnapshot snapshot = metadata.fetchMetadataSnapshot();
        Cluster cluster = metadata.fetch();
        // // Validate metadata snapshot & cluster are setup as expected.
        // assertEquals(oldNodeCount, snapshot.cluster().nodes().size());
        // assertEquals(oldPartitionCount, snapshot.cluster().partitionCountForTopic(topic1));
        // assertEquals(oldPartitionCount, snapshot.cluster().partitionCountForTopic(topic2));
        // assertEquals(OptionalIntcluster, snapshot.cluster());
        // assertEquals(.of(oldLeaderEpoch), snapshot.leaderEpochFor(topic1Part0));

        // Setup 6 threads, where 3 are updating metadata & 3 are reading snapshot/cluster.
        // Metadata will be updated with higher # of nodes, partition-counts, leader-epoch.
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        CountDownLatch allThreadsDoneLatch = new CountDownLatch(threadCount);
        CountDownLatch atleastMetadataUpdatedOnceLatch = new CountDownLatch(1);
        AtomicReference<MetadataSnapshot> newSnapshot = new AtomicReference<>();
        AtomicReference<Cluster> newCluster = new AtomicReference<>();
        for (int i = 0; i < threadCount; i++) {
            final int id = i + 1;
            service.execute(() -> {
                if (id % 2 == 0) { // Thread to update metadata.
                    String oldClusterId = "clusterId";
                    int nNodes = oldNodeCount + id;
                    Map<String, Integer> newTopicPartitionCounts = new HashMap<>();
                    newTopicPartitionCounts.put(topic1, oldPartitionCount + id);
                    newTopicPartitionCounts.put(topic2, oldPartitionCount + id);
                    MetadataResponse newMetadataResponse =
                        RequestTestUtils.metadataUpdateWithIds(oldClusterId, nNodes, Collections.emptyMap(), newTopicPartitionCounts, _tp -> oldLeaderEpoch + id, topicIds);
                    metadata.updateWithCurrentRequestVersion(newMetadataResponse, true, time.milliseconds());
                    atleastMetadataUpdatedOnceLatch.countDown();
                } else { // Thread to read metadata snapshot, once its updated
                    try {
                        if (!atleastMetadataUpdatedOnceLatch.await(5, TimeUnit.MINUTES)) {
                            // assertFalse(true, "Test had to wait more than 5 minutes, something went wrong.");
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    newSnapshot.set(metadata.fetchMetadataSnapshot());
                    newCluster.set(metadata.fetch());
                }
                allThreadsDoneLatch.countDown();
            });
        }
        if (!allThreadsDoneLatch.await(5, TimeUnit.MINUTES)) {
            // assertFalse(true, "Test had to wait more than 5 minutes, something went wrong.");
        }

        /* // Validate new snapshot is upto-date. And has higher partition counts, nodes & leader epoch than earlier.
        {
            int newNodeCount = newSnapshot.get().cluster().nodes().size();
            assertTrue(oldNodeCount < newNodeCount, "Unexpected value " + newNodeCount);
            int newPartitionCountTopic1 = newSnapshot.get().cluster().partitionCountForTopic(topic1);
            assertTrue(oldPartitionCount < newPartitionCountTopic1, "Unexpected value " + newPartitionCountTopic1);
            int newPartitionCountTopic2 = newSnapshot.get().cluster().partitionCountForTopic(topic2);
            assertTrue(oldPartitionCount < newPartitionCountTopic2, "Unexpected value " + newPartitionCountTopic2);
            int newLeaderEpoch = newSnapshot.get().leaderEpochFor(topic1Part0).getAsInt();
            assertTrue(oldLeaderEpoch < newLeaderEpoch, "Unexpected value " + newLeaderEpoch);
        }

        // Validate new cluster is upto-date. And has higher partition counts, nodes than earlier.
        {
            int newNodeCount = newCluster.get().nodes().size();
            assertTrue(oldNodeCount < newNodeCount, "Unexpected value " + newNodeCount);
            int newPartitionCountTopic1 = newCluster.get().partitionCountForTopic(topic1);
            assertTrue(oldPartitionCount < newPartitionCountTopic1, "Unexpected value " + newPartitionCountTopic1);
            int newPartitionCountTopic2 = newCluster.get()
                .partitionCountForTopic(topic2);
            assertTrue(oldPartitionCount < newPartitionCountTopic2, "Unexpected value " + newPartitionCountTopic2);
        }
	*/

        service.shutdown();
        // // Executor service should down much quickly, as all tasks are finished at this point.
        // assertTrue(service.awaitTermination(60, TimeUnit.SECONDS));
    }
   
}

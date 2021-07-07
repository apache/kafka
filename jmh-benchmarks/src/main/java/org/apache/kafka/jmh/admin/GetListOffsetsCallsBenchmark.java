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

package org.apache.kafka.jmh.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.internals.MetadataOperationContext;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.requests.MetadataResponse;
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
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class GetListOffsetsCallsBenchmark {
    @Param({"1", "10"})
    private int topicCount;

    @Param({"100", "1000", "10000"})
    private int partitionCount;

    private KafkaAdminClient admin;
    private MetadataOperationContext<ListOffsetsResult.ListOffsetsResultInfo, ListOffsetsOptions> context;
    private final Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
    private final Map<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> futures = new HashMap<>();
    private final int numNodes = 3;

    @Setup(Level.Trial)
    public void setup() {
        MetadataResponseData data = new MetadataResponseData();
        List<MetadataResponseTopic> mrTopicList = new ArrayList<>();
        Set<String> topics = new HashSet<>();

        for (int topicIndex = 0; topicIndex < topicCount; topicIndex++) {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic-" + topicIndex;
            MetadataResponseTopic mrTopic = new MetadataResponseTopic()
                    .setTopicId(topicId)
                    .setName(topicName)
                    .setErrorCode((short) 0)
                    .setIsInternal(false);

            List<MetadataResponsePartition> mrPartitionList = new ArrayList<>();

            for (int partition = 0; partition < partitionCount; partition++) {
                TopicPartition tp = new TopicPartition(topicName, partition);
                topics.add(tp.topic());
                futures.put(tp, new KafkaFutureImpl<>());
                topicPartitionOffsets.put(tp, OffsetSpec.latest());

                MetadataResponsePartition mrPartition = new MetadataResponsePartition()
                        .setLeaderId(partition % numNodes)
                        .setPartitionIndex(partition)
                        .setIsrNodes(Arrays.asList(0, 1, 2))
                        .setReplicaNodes(Arrays.asList(0, 1, 2))
                        .setOfflineReplicas(Collections.emptyList())
                        .setErrorCode((short) 0);

                mrPartitionList.add(mrPartition);
            }

            mrTopic.setPartitions(mrPartitionList);
            mrTopicList.add(mrTopic);
        }
        data.setTopics(new MetadataResponseData.MetadataResponseTopicCollection(mrTopicList.listIterator()));

        long deadline = 0L;
        short version = 0;
        context = new MetadataOperationContext<>(topics, new ListOffsetsOptions(), deadline, futures);
        context.setResponse(Optional.of(new MetadataResponse(data, version)));

        AdminClientUnitTestEnv adminEnv = new AdminClientUnitTestEnv(mockCluster());
        admin = (KafkaAdminClient) adminEnv.adminClient();
    }

    @Benchmark
    public Object testGetListOffsetsCalls() {
        return AdminClientTestUtils.getListOffsetsCalls(admin, context, topicPartitionOffsets, futures);
    }

    private Cluster mockCluster() {
        final int controllerIndex = 0;

        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; i++)
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        return new Cluster("mockClusterId", nodes.values(),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(controllerIndex));
    }
}

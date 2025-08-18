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
package org.apache.kafka.tiered.storage;

import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.tiered.storage.actions.AlterLogDirAction;
import org.apache.kafka.tiered.storage.actions.BounceBrokerAction;
import org.apache.kafka.tiered.storage.actions.ConsumeAction;
import org.apache.kafka.tiered.storage.actions.CreatePartitionsAction;
import org.apache.kafka.tiered.storage.actions.CreateTopicAction;
import org.apache.kafka.tiered.storage.actions.DeleteRecordsAction;
import org.apache.kafka.tiered.storage.actions.DeleteTopicAction;
import org.apache.kafka.tiered.storage.actions.EraseBrokerStorageAction;
import org.apache.kafka.tiered.storage.actions.ExpectEmptyRemoteStorageAction;
import org.apache.kafka.tiered.storage.actions.ExpectLeaderAction;
import org.apache.kafka.tiered.storage.actions.ExpectLeaderEpochCheckpointAction;
import org.apache.kafka.tiered.storage.actions.ExpectListOffsetsAction;
import org.apache.kafka.tiered.storage.actions.ExpectUserTopicMappedToMetadataPartitionsAction;
import org.apache.kafka.tiered.storage.actions.ProduceAction;
import org.apache.kafka.tiered.storage.actions.ReassignReplicaAction;
import org.apache.kafka.tiered.storage.actions.ShrinkReplicaAction;
import org.apache.kafka.tiered.storage.actions.StartBrokerAction;
import org.apache.kafka.tiered.storage.actions.StopBrokerAction;
import org.apache.kafka.tiered.storage.actions.UpdateTopicConfigAction;
import org.apache.kafka.tiered.storage.specs.ConsumableSpec;
import org.apache.kafka.tiered.storage.specs.DeletableSpec;
import org.apache.kafka.tiered.storage.specs.ExpandPartitionCountSpec;
import org.apache.kafka.tiered.storage.specs.FetchableSpec;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;
import org.apache.kafka.tiered.storage.specs.OffloadableSpec;
import org.apache.kafka.tiered.storage.specs.OffloadedSegmentSpec;
import org.apache.kafka.tiered.storage.specs.ProducableSpec;
import org.apache.kafka.tiered.storage.specs.RemoteDeleteSegmentSpec;
import org.apache.kafka.tiered.storage.specs.RemoteFetchCount;
import org.apache.kafka.tiered.storage.specs.RemoteFetchSpec;
import org.apache.kafka.tiered.storage.specs.TopicSpec;

import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("ClassDataAbstractionCoupling")
public final class TieredStorageTestBuilder {

    private final int defaultProducedBatchSize = 1;
    private final long defaultEarliestLocalOffsetExpectedInLogDirectory = 0;

    private final Map<TopicPartition, List<DeletableSpec>> deletables = new HashMap<>();
    private final List<TieredStorageTestAction> actions = new ArrayList<>();
    private Map<TopicPartition, ProducableSpec> producables = new HashMap<>();
    private Map<TopicPartition, List<OffloadableSpec>> offloadables = new HashMap<>();
    private Map<TopicPartition, ConsumableSpec> consumables = new HashMap<>();
    private Map<TopicPartition, FetchableSpec> fetchables = new HashMap<>();

    public TieredStorageTestBuilder() {
    }

    public TieredStorageTestBuilder createTopic(String topic,
                                                int partitionCount,
                                                int replicationFactor,
                                                int maxBatchCountPerSegment,
                                                Map<Integer, List<Integer>> replicaAssignment,
                                                boolean enableRemoteLogStorage) {
        assertTrue(maxBatchCountPerSegment >= 1, "Segments size for topic " + topic + " needs to be >= 1");
        assertTrue(partitionCount >= 1, "Partition count for topic " + topic + " needs to be >= 1");
        assertTrue(replicationFactor >= 1, "Replication factor for topic " + topic + " needs to be >= 1");
        Map<String, String> properties = new HashMap<>();
        properties.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, String.valueOf(enableRemoteLogStorage));
        TopicSpec topicSpec = new TopicSpec(topic, partitionCount, replicationFactor, maxBatchCountPerSegment,
                replicaAssignment, properties);
        actions.add(new CreateTopicAction(topicSpec));
        return this;
    }

    public TieredStorageTestBuilder createPartitions(String topic,
                                                     int partitionCount,
                                                     Map<Integer, List<Integer>> replicaAssignment) {
        assertTrue(partitionCount >= 1, "Partition count for topic " + topic + " needs to be >= 1");
        ExpandPartitionCountSpec spec = new ExpandPartitionCountSpec(topic, partitionCount, replicaAssignment);
        actions.add(new CreatePartitionsAction(spec));
        return this;
    }

    public TieredStorageTestBuilder updateTopicConfig(String topic,
                                                      Map<String, String> configsToBeAdded,
                                                      List<String> configsToBeDeleted) {
        assertTrue(!configsToBeAdded.isEmpty() || !configsToBeDeleted.isEmpty(),
                "Topic " + topic + " configs shouldn't be empty");
        actions.add(new UpdateTopicConfigAction(topic, configsToBeAdded, configsToBeDeleted));
        return this;
    }

    public TieredStorageTestBuilder deleteTopic(List<String> topics) {
        topics.forEach(topic -> actions.add(buildDeleteTopicAction(topic, true)));
        return this;
    }

    public TieredStorageTestBuilder produce(String topic,
                                            int partition,
                                            KeyValueSpec... keyValues) {
        assertTrue(partition >= 0, "Partition must be >= 0");
        ProducableSpec spec = getOrCreateProducable(topic, partition);
        for (KeyValueSpec kv : keyValues) {
            spec.getRecords().add(new ProducerRecord<>(topic, partition, kv.getKey(), kv.getValue()));
        }
        createProduceAction();
        return this;
    }

    public TieredStorageTestBuilder produceWithTimestamp(String topic,
                                                         int partition,
                                                         KeyValueSpec... keyValues) {
        assertTrue(partition >= 0, "Partition must be >= 0");
        ProducableSpec spec = getOrCreateProducable(topic, partition);
        for (KeyValueSpec kv : keyValues) {
            spec.getRecords()
                    .add(new ProducerRecord<>(topic, partition, kv.getTimestamp(), kv.getKey(), kv.getValue()));
        }
        createProduceAction();
        return this;
    }

    public TieredStorageTestBuilder expectEarliestLocalOffsetInLogDirectory(String topic,
                                                                            int partition,
                                                                            long earliestLocalOffset) {
        assertTrue(earliestLocalOffset >= 0, "Record offset must be >= 0");
        getOrCreateProducable(topic, partition).setEarliestLocalLogOffset(earliestLocalOffset);
        return this;
    }

    public TieredStorageTestBuilder expectSegmentToBeOffloaded(int fromBroker,
                                                               String topic,
                                                               int partition,
                                                               int baseOffset,
                                                               KeyValueSpec... keyValues) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<ProducerRecord<String, String>> records = new ArrayList<>();
        for (KeyValueSpec kv: keyValues) {
            records.add(new ProducerRecord<>(topic, partition, kv.getTimestamp(), kv.getKey(), kv.getValue()));
        }
        offloadables.computeIfAbsent(topicPartition, k -> new ArrayList<>())
                .add(new OffloadableSpec(fromBroker, baseOffset, records));
        return this;
    }

    public TieredStorageTestBuilder consume(String topic,
                                            int partition,
                                            long fetchOffset,
                                            int expectedTotalRecord,
                                            int expectedRecordsFromSecondTier) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        assertTrue(partition >= 0, "Partition must be >= 0");
        assertTrue(fetchOffset >= 0, "Fetch offset must be >=0");
        assertTrue(expectedTotalRecord >= 1, "Must read at least one record");
        assertTrue(expectedRecordsFromSecondTier >= 0, "Expected read cannot be < 0");
        assertTrue(expectedRecordsFromSecondTier <= expectedTotalRecord, "Cannot fetch more records than consumed");
        assertFalse(consumables.containsKey(topicPartition), "Consume already in progress for " + topicPartition);
        consumables.put(
                topicPartition, new ConsumableSpec(fetchOffset, expectedTotalRecord, expectedRecordsFromSecondTier));
        createConsumeAction();
        return this;
    }

    public TieredStorageTestBuilder expectLeader(String topic,
                                                 int partition,
                                                 int brokerId,
                                                 boolean electLeader) {
        actions.add(new ExpectLeaderAction(new TopicPartition(topic, partition), brokerId, electLeader));
        return this;
    }

    public TieredStorageTestBuilder expectFetchFromTieredStorage(int fromBroker,
                                                                 String topic,
                                                                 int partition,
                                                                 int segmentFetchRequestCount) {
        return expectFetchFromTieredStorage(fromBroker, topic, partition, new RemoteFetchCount(segmentFetchRequestCount));
    }

    public TieredStorageTestBuilder expectFetchFromTieredStorage(int fromBroker,
                                                                 String topic,
                                                                 int partition,
                                                                 RemoteFetchCount remoteFetchRequestCount) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        assertTrue(partition >= 0, "Partition must be >= 0");
        assertTrue(remoteFetchRequestCount.getSegmentFetchCountAndOp().count() >= 0, "Expected fetch count from tiered storage must be >= 0");
        assertFalse(fetchables.containsKey(topicPartition), "Consume already in progress for " + topicPartition);
        fetchables.put(topicPartition, new FetchableSpec(fromBroker, remoteFetchRequestCount));
        return this;
    }

    public TieredStorageTestBuilder expectDeletionInRemoteStorage(int fromBroker,
                                                                  String topic,
                                                                  int partition,
                                                                  LocalTieredStorageEvent.EventType eventType,
                                                                  int eventCount) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        deletables.computeIfAbsent(topicPartition, k -> new ArrayList<>())
                .add(new DeletableSpec(fromBroker, eventType, eventCount));
        return this;
    }

    public TieredStorageTestBuilder waitForRemoteLogSegmentDeletion(String topic) {
        actions.add(buildDeleteTopicAction(topic, false));
        return this;
    }

    public TieredStorageTestBuilder expectLeaderEpochCheckpoint(int brokerId,
                                                                String topic,
                                                                int partition,
                                                                int beginEpoch,
                                                                long startOffset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        actions.add(new ExpectLeaderEpochCheckpointAction(brokerId, topicPartition, beginEpoch, startOffset));
        return this;
    }

    public TieredStorageTestBuilder expectListOffsets(String topic,
                                                      int partition,
                                                      OffsetSpec offsetSpec,
                                                      EpochEntry epochEntry) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        actions.add(new ExpectListOffsetsAction(topicPartition, offsetSpec, epochEntry));
        return this;
    }

    public TieredStorageTestBuilder bounce(int brokerId) {
        actions.add(new BounceBrokerAction(brokerId));
        return this;
    }

    public TieredStorageTestBuilder stop(int brokerId) {
        actions.add(new StopBrokerAction(brokerId));
        return this;
    }

    public TieredStorageTestBuilder start(int brokerId) {
        actions.add(new StartBrokerAction(brokerId));
        return this;
    }

    public TieredStorageTestBuilder eraseBrokerStorage(int brokerId,
                                                       FilenameFilter filenameFilter,
                                                       boolean isStopped) {
        actions.add(new EraseBrokerStorageAction(brokerId, filenameFilter, isStopped));
        return this;
    }

    public TieredStorageTestBuilder expectEmptyRemoteStorage(String topic,
                                                             int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        actions.add(new ExpectEmptyRemoteStorageAction(topicPartition));
        return this;
    }

    public TieredStorageTestBuilder shrinkReplica(String topic,
                                                  int partition,
                                                  List<Integer> replicaIds) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        actions.add(new ShrinkReplicaAction(topicPartition, replicaIds));
        return this;
    }

    public TieredStorageTestBuilder reassignReplica(String topic,
                                                    int partition,
                                                    List<Integer> replicaIds) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        actions.add(new ReassignReplicaAction(topicPartition, replicaIds));
        return this;
    }

    public TieredStorageTestBuilder alterLogDir(String topic,
                                                int partition,
                                                int replicaIds) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        actions.add(new AlterLogDirAction(topicPartition, replicaIds));
        return this;
    }

    public TieredStorageTestBuilder expectUserTopicMappedToMetadataPartitions(String topic,
                                                                              List<Integer> metadataPartitions) {
        actions.add(new ExpectUserTopicMappedToMetadataPartitionsAction(topic, metadataPartitions));
        return this;
    }

    public TieredStorageTestBuilder deleteRecords(String topic,
                                                  int partition,
                                                  long beforeOffset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        actions.add(new DeleteRecordsAction(topicPartition, beforeOffset, buildDeleteSegmentSpecList(topic)));
        return this;
    }

    public List<TieredStorageTestAction> complete() {
        return actions;
    }

    private void createProduceAction() {
        if (!producables.isEmpty()) {
            producables.forEach((topicPartition, producableSpec) -> {
                List<ProducerRecord<String, String>> recordsToProduce = new ArrayList<>(producableSpec.getRecords());
                List<OffloadedSegmentSpec> offloadedSegmentSpecs =
                        offloadables.computeIfAbsent(topicPartition, k -> new ArrayList<>())
                        .stream()
                        .map(spec ->
                                new OffloadedSegmentSpec(spec.sourceBrokerId(), topicPartition, spec.baseOffset(),
                                        spec.records()))
                        .toList();
                ProduceAction action = new ProduceAction(topicPartition, offloadedSegmentSpecs, recordsToProduce,
                        producableSpec.getBatchSize(), producableSpec.getEarliestLocalLogOffset());
                actions.add(action);
            });
            producables = new HashMap<>();
            offloadables = new HashMap<>();
        }
    }

    private void createConsumeAction() {
        if (!consumables.isEmpty()) {
            consumables.forEach((topicPartition, consumableSpec) -> {
                FetchableSpec fetchableSpec = fetchables.computeIfAbsent(topicPartition, k -> new FetchableSpec(0, new RemoteFetchCount(0)));
                RemoteFetchSpec remoteFetchSpec = new RemoteFetchSpec(fetchableSpec.sourceBrokerId(), topicPartition,
                        fetchableSpec.fetchCount());
                ConsumeAction action = new ConsumeAction(topicPartition, consumableSpec.fetchOffset(),
                        consumableSpec.expectedTotalCount(), consumableSpec.expectedFromSecondTierCount(),
                        remoteFetchSpec);
                actions.add(action);
            });
            consumables = new HashMap<>();
            fetchables = new HashMap<>();
        }
    }

    private ProducableSpec getOrCreateProducable(String topic,
                                                 int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        return producables.computeIfAbsent(topicPartition,
                k -> new ProducableSpec(new ArrayList<>(), defaultProducedBatchSize,
                        defaultEarliestLocalOffsetExpectedInLogDirectory));
    }

    private DeleteTopicAction buildDeleteTopicAction(String topic,
                                                     boolean shouldDelete) {
        return new DeleteTopicAction(topic, buildDeleteSegmentSpecList(topic), shouldDelete);
    }

    private List<RemoteDeleteSegmentSpec> buildDeleteSegmentSpecList(String topic) {
        List<RemoteDeleteSegmentSpec> deleteSegmentSpecList = deletables.entrySet()
                .stream()
                .filter(e -> e.getKey().topic().equals(topic))
                .flatMap(e -> {
                    TopicPartition partition = e.getKey();
                    List<DeletableSpec> deletableSpecs = e.getValue();
                    return deletableSpecs.stream()
                            .map(spec -> new RemoteDeleteSegmentSpec(spec.sourceBrokerId(), partition,
                                    spec.eventType(), spec.eventCount()));
                })
                .toList();
        deleteSegmentSpecList.forEach(spec -> deletables.remove(spec.topicPartition()));
        return deleteSegmentSpecList;
    }
}

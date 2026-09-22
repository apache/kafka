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
package org.apache.kafka.metadata;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataCacheTest {

    protected static final long BROKER_EPOCH = 0L;

    public static MetadataCache createCache() {
        return MetadataCacheFixtures.createCache();
    }

    public static void updateCache(MetadataCache cache, List<ApiMessage> records) {
        MetadataCacheFixtures.updateCache(cache, records);
    }

    @Test
    public void getTopicMetadataNonExistingTopics() {
        MetadataCache cache = createCache();
        String topic = "topic";
        List<MetadataResponseData.MetadataResponseTopic> topicMetadata = cache.getTopicMetadata(
            Set.of(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), false, false);
        assertTrue(topicMetadata.isEmpty());
    }

    @Test
    public void getTopicMetadata() {
        MetadataCache cache = createCache();
        String topic0 = "topic-0";
        String topic1 = "topic-1";

        Map<String, Uuid> topicIds = Map.of(
            topic0, Uuid.randomUuid(),
            topic1, Uuid.randomUuid()
        );

        List<PartitionRecord> partitionStates = List.of(
            new PartitionRecord()
                .setTopicId(topicIds.get(topic0))
                .setPartitionId(0)
                .setLeader(0)
                .setLeaderEpoch(0)
                .setIsr(List.of(0, 1, 3))
                .setReplicas(List.of(0, 1, 3)),
            new PartitionRecord()
                .setTopicId(topicIds.get(topic0))
                .setPartitionId(1)
                .setLeader(1)
                .setLeaderEpoch(1)
                .setIsr(List.of(1, 0))
                .setReplicas(List.of(1, 2, 0, 4)),
            new PartitionRecord()
                .setTopicId(topicIds.get(topic1))
                .setPartitionId(0)
                .setLeader(2)
                .setLeaderEpoch(2)
                .setIsr(List.of(2, 1))
                .setReplicas(List.of(2, 1, 3))
        );

        List<ApiMessage> records = new ArrayList<>();
        for (int brokerId = 0; brokerId <= 4; brokerId++) {
            String host = "foo-" + brokerId;
            BrokerEndpointCollection endpoints = new BrokerEndpointCollection(List.of(
                new BrokerEndpoint()
                    .setHost(host)
                    .setPort(9092)
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                    .setName(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value()),
                new BrokerEndpoint()
                    .setHost(host)
                    .setPort(9093)
                    .setSecurityProtocol(SecurityProtocol.SSL.id)
                    .setName(ListenerName.forSecurityProtocol(SecurityProtocol.SSL).value())
            ));
            records.add(new RegisterBrokerRecord()
                .setBrokerId(brokerId)
                .setEndPoints(endpoints)
                .setRack("rack1"));
        }
        records.add(new TopicRecord().setName(topic0).setTopicId(topicIds.get(topic0)));
        records.add(new TopicRecord().setName(topic1).setTopicId(topicIds.get(topic1)));
        records.addAll(partitionStates);
        updateCache(cache, records);

        for (SecurityProtocol securityProtocol : new SecurityProtocol[]{SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL}) {
            ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);

            for (String topic : new String[]{topic0, topic1}) {
                List<MetadataResponseData.MetadataResponseTopic> topicMetadataList =
                    cache.getTopicMetadata(Set.of(topic), listenerName, false, false);
                assertEquals(1, topicMetadataList.size());

                MetadataResponseData.MetadataResponseTopic topicMetadata = topicMetadataList.get(0);
                assertEquals(Errors.NONE.code(), topicMetadata.errorCode());
                assertEquals(topic, topicMetadata.name());
                assertEquals(topicIds.get(topic), topicMetadata.topicId());

                List<PartitionRecord> topicPartitionStates = new ArrayList<>();
                for (PartitionRecord ps : partitionStates) {
                    if (ps.topicId().equals(topicIds.get(topic))) {
                        topicPartitionStates.add(ps);
                    }
                }

                List<MetadataResponseData.MetadataResponsePartition> partitionMetadatas =
                    new ArrayList<>(topicMetadata.partitions());
                partitionMetadatas.sort(Comparator.comparingInt(MetadataResponseData.MetadataResponsePartition::partitionIndex));
                assertEquals(topicPartitionStates.size(), partitionMetadatas.size(),
                    "Unexpected partition count for topic " + topic);

                for (int i = 0; i < partitionMetadatas.size(); i++) {
                    MetadataResponseData.MetadataResponsePartition partitionMetadata = partitionMetadatas.get(i);
                    int partitionId = i;
                    assertEquals(Errors.NONE.code(), partitionMetadata.errorCode());
                    assertEquals(partitionId, partitionMetadata.partitionIndex());
                    PartitionRecord partitionState = topicPartitionStates.stream()
                        .filter(ps -> ps.partitionId() == partitionId)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Unable to find partition state for partition " + partitionId));
                    assertEquals(partitionState.leader(), partitionMetadata.leaderId());
                    assertEquals(partitionState.leaderEpoch(), partitionMetadata.leaderEpoch());
                    assertEquals(partitionState.isr(), partitionMetadata.isrNodes());
                    assertEquals(partitionState.replicas(), partitionMetadata.replicaNodes());
                }
            }
        }
    }

    @Test
    public void getTopicMetadataPartitionLeaderNotAvailable() {
        MetadataCache cache = createCache();
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        List<RegisterBrokerRecord> brokers = List.of(
            new RegisterBrokerRecord()
                .setBrokerId(0)
                .setFenced(false)
                .setEndPoints(new BrokerEndpointCollection(List.of(
                    new BrokerEndpoint()
                        .setHost("foo")
                        .setPort(9092)
                        .setSecurityProtocol(securityProtocol.id)
                        .setName(listenerName.value())
                )))
        );

        // leader is not available. expect LEADER_NOT_AVAILABLE for any metadata version.
        verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, listenerName,
            1, Errors.LEADER_NOT_AVAILABLE, false);
        verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, listenerName,
            1, Errors.LEADER_NOT_AVAILABLE, true);
    }

    @Test
    public void getTopicMetadataPartitionListenerNotAvailableOnLeader() {
        MetadataCache cache = createCache();
        // when listener name is not present in the metadata cache for a broker, getTopicMetadata should
        // return LEADER_NOT_AVAILABLE or LISTENER_NOT_FOUND errors for old and new versions respectively.
        ListenerName plaintextListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
        ListenerName sslListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL);
        BrokerEndpointCollection broker0Endpoints = new BrokerEndpointCollection(List.of(
            new BrokerEndpoint()
                .setHost("host0")
                .setPort(9092)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                .setName(plaintextListenerName.value()),
            new BrokerEndpoint()
                .setHost("host0")
                .setPort(9093)
                .setSecurityProtocol(SecurityProtocol.SSL.id)
                .setName(sslListenerName.value())
        ));

        BrokerEndpointCollection broker1Endpoints = new BrokerEndpointCollection(List.of(
            new BrokerEndpoint()
                .setHost("host1")
                .setPort(9092)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                .setName(plaintextListenerName.value())
        ));

        List<RegisterBrokerRecord> brokers = List.of(
            new RegisterBrokerRecord()
                .setBrokerId(0)
                .setFenced(false)
                .setEndPoints(broker0Endpoints),
            new RegisterBrokerRecord()
                .setBrokerId(1)
                .setFenced(false)
                .setEndPoints(broker1Endpoints)
        );

        // leader available in cache but listener name not present. expect LISTENER_NOT_FOUND error for new metadata version
        verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, sslListenerName,
            1, Errors.LISTENER_NOT_FOUND, true);
        // leader available in cache but listener name not present. expect LEADER_NOT_AVAILABLE error for old metadata version
        verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, brokers, sslListenerName,
            1, Errors.LEADER_NOT_AVAILABLE, false);
    }

    private void verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(
        MetadataCache cache,
        List<RegisterBrokerRecord> brokers,
        ListenerName listenerName,
        int leader,
        Errors expectedError,
        boolean errorUnavailableListeners
    ) {
        String topic = "topic";
        Uuid topicId = Uuid.randomUuid();
        List<ApiMessage> records = new ArrayList<>(brokers);
        records.add(new TopicRecord().setName(topic).setTopicId(topicId));

        int leaderEpoch = 1;
        int partitionEpoch = 3;
        records.add(new PartitionRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setPartitionEpoch(partitionEpoch)
            .setLeader(leader)
            .setLeaderEpoch(leaderEpoch)
            .setIsr(List.of(0))
            .setReplicas(List.of(0)));
        updateCache(cache, records);

        List<MetadataResponseData.MetadataResponseTopic> topicMetadataList =
            cache.getTopicMetadata(Set.of(topic), listenerName, false, errorUnavailableListeners);
        assertEquals(1, topicMetadataList.size());

        MetadataResponseData.MetadataResponseTopic topicMetadata = topicMetadataList.get(0);
        assertEquals(Errors.NONE.code(), topicMetadata.errorCode());

        List<MetadataResponseData.MetadataResponsePartition> partitionMetadatas = topicMetadata.partitions();
        assertEquals(1, partitionMetadatas.size());

        MetadataResponseData.MetadataResponsePartition partitionMetadata = partitionMetadatas.get(0);
        assertEquals(0, partitionMetadata.partitionIndex());
        assertEquals(expectedError.code(), partitionMetadata.errorCode());
        assertFalse(partitionMetadata.isrNodes().isEmpty());
        assertEquals(List.of(0), partitionMetadata.replicaNodes());
    }

    @Test
    public void getTopicMetadataReplicaNotAvailable() {
        MetadataCache cache = createCache();
        String topic = "topic";
        Uuid topicId = Uuid.randomUuid();

        int partitionEpoch = 3;
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        BrokerEndpointCollection endPoints = new BrokerEndpointCollection(List.of(
            new BrokerEndpoint()
                .setHost("foo")
                .setPort(9092)
                .setSecurityProtocol(securityProtocol.id)
                .setName(listenerName.value())
        ));

        // replica 1 is not available
        int leader = 0;
        int leaderEpoch = 0;
        List<Integer> replicas = List.of(0, 1);
        List<Integer> isr = List.of(0);

        List<ApiMessage> records = List.of(
            new RegisterBrokerRecord()
                .setBrokerId(0)
                .setFenced(false)
                .setEndPoints(endPoints),
            new TopicRecord().setName(topic).setTopicId(topicId),
            new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(0)
                .setLeader(leader)
                .setLeaderEpoch(leaderEpoch)
                .setIsr(isr)
                .setPartitionEpoch(partitionEpoch)
                .setReplicas(replicas)
        );
        updateCache(cache, records);

        // Validate errorUnavailableEndpoints = false
        List<MetadataResponseData.MetadataResponseTopic> topicMetadataList =
            cache.getTopicMetadata(Set.of(topic), listenerName, false, false);
        assertEquals(1, topicMetadataList.size());

        MetadataResponseData.MetadataResponseTopic topicMetadata = topicMetadataList.get(0);
        assertEquals(Errors.NONE.code(), topicMetadata.errorCode());

        List<MetadataResponseData.MetadataResponsePartition> partitionMetadatas = topicMetadata.partitions();
        assertEquals(1, partitionMetadatas.size());

        MetadataResponseData.MetadataResponsePartition partitionMetadata = partitionMetadatas.get(0);
        assertEquals(0, partitionMetadata.partitionIndex());
        assertEquals(Errors.NONE.code(), partitionMetadata.errorCode());
        assertEquals(List.of(0, 1), partitionMetadata.replicaNodes());
        assertEquals(List.of(0), partitionMetadata.isrNodes());

        // Validate errorUnavailableEndpoints = true
        List<MetadataResponseData.MetadataResponseTopic> topicMetadatasWithError =
            cache.getTopicMetadata(Set.of(topic), listenerName, true, false);
        assertEquals(1, topicMetadatasWithError.size());

        MetadataResponseData.MetadataResponseTopic topicMetadataWithError = topicMetadatasWithError.get(0);
        assertEquals(Errors.NONE.code(), topicMetadataWithError.errorCode());

        List<MetadataResponseData.MetadataResponsePartition> partitionMetadatasWithError = topicMetadataWithError.partitions();
        assertEquals(1, partitionMetadatasWithError.size());

        MetadataResponseData.MetadataResponsePartition partitionMetadataWithError = partitionMetadatasWithError.get(0);
        assertEquals(0, partitionMetadataWithError.partitionIndex());
        assertEquals(Errors.REPLICA_NOT_AVAILABLE.code(), partitionMetadataWithError.errorCode());
        assertEquals(List.of(0), partitionMetadataWithError.replicaNodes());
        assertEquals(List.of(0), partitionMetadataWithError.isrNodes());
    }

    @Test
    public void getTopicMetadataIsrNotAvailable() {
        MetadataCache cache = createCache();
        String topic = "topic";
        Uuid topicId = Uuid.randomUuid();

        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);

        BrokerEndpointCollection endpoints = new BrokerEndpointCollection(List.of(
            new BrokerEndpoint()
                .setHost("foo")
                .setPort(9092)
                .setSecurityProtocol(securityProtocol.id)
                .setName(listenerName.value())
        ));

        // isr member 1 is not a registered broker
        int leader = 0;
        int leaderEpoch = 0;
        List<Integer> replicas = List.of(0);
        List<Integer> isr = List.of(0, 1);
        List<ApiMessage> records = List.of(
            new RegisterBrokerRecord()
                .setBrokerId(0)
                .setRack("rack1")
                .setFenced(false)
                .setEndPoints(endpoints),
            new TopicRecord().setName(topic).setTopicId(topicId),
            new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(0)
                .setLeader(leader)
                .setLeaderEpoch(leaderEpoch)
                .setIsr(isr)
                .setReplicas(replicas)
        );
        updateCache(cache, records);

        // Validate errorUnavailableEndpoints = false
        List<MetadataResponseData.MetadataResponseTopic> topicMetadataList =
            cache.getTopicMetadata(Set.of(topic), listenerName, false, false);
        assertEquals(1, topicMetadataList.size());

        MetadataResponseData.MetadataResponseTopic topicMetadata = topicMetadataList.get(0);
        assertEquals(Errors.NONE.code(), topicMetadata.errorCode());

        List<MetadataResponseData.MetadataResponsePartition> partitionMetadatas = topicMetadata.partitions();
        assertEquals(1, partitionMetadatas.size());

        MetadataResponseData.MetadataResponsePartition partitionMetadata = partitionMetadatas.get(0);
        assertEquals(0, partitionMetadata.partitionIndex());
        assertEquals(Errors.NONE.code(), partitionMetadata.errorCode());
        assertEquals(List.of(0), partitionMetadata.replicaNodes());
        assertEquals(List.of(0, 1), partitionMetadata.isrNodes());

        // Validate errorUnavailableEndpoints = true
        List<MetadataResponseData.MetadataResponseTopic> topicMetadatasWithError =
            cache.getTopicMetadata(Set.of(topic), listenerName, true, false);
        assertEquals(1, topicMetadatasWithError.size());

        MetadataResponseData.MetadataResponseTopic topicMetadataWithError = topicMetadatasWithError.get(0);
        assertEquals(Errors.NONE.code(), topicMetadataWithError.errorCode());

        List<MetadataResponseData.MetadataResponsePartition> partitionMetadatasWithError = topicMetadataWithError.partitions();
        assertEquals(1, partitionMetadatasWithError.size());

        MetadataResponseData.MetadataResponsePartition partitionMetadataWithError = partitionMetadatasWithError.get(0);
        assertEquals(0, partitionMetadataWithError.partitionIndex());
        assertEquals(Errors.REPLICA_NOT_AVAILABLE.code(), partitionMetadataWithError.errorCode());
        assertEquals(List.of(0), partitionMetadataWithError.replicaNodes());
        assertEquals(List.of(0), partitionMetadataWithError.isrNodes());
    }

    @Test
    public void getTopicMetadataWithNonSupportedSecurityProtocol() {
        MetadataCache cache = createCache();
        String topic = "topic";
        Uuid topicId = Uuid.randomUuid();
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;

        RegisterBrokerRecord broker = new RegisterBrokerRecord()
            .setBrokerId(0)
            .setRack("")
            .setEndPoints(new BrokerEndpointCollection(List.of(
                new BrokerEndpoint()
                    .setHost("foo")
                    .setPort(9092)
                    .setSecurityProtocol(securityProtocol.id)
                    .setName(ListenerName.forSecurityProtocol(securityProtocol).value())
            )));

        TopicRecord topicRecord = new TopicRecord().setName(topic).setTopicId(topicId);

        int leader = 0;
        int leaderEpoch = 0;
        List<Integer> replicas = List.of(0);
        List<Integer> isr = List.of(0, 1);

        List<ApiMessage> records = List.of(
            broker,
            topicRecord,
            new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(0)
                .setLeader(leader)
                .setLeaderEpoch(leaderEpoch)
                .setIsr(isr)
                .setReplicas(replicas)
        );
        updateCache(cache, records);

        List<MetadataResponseData.MetadataResponseTopic> topicMetadata =
            cache.getTopicMetadata(Set.of(topic), ListenerName.forSecurityProtocol(SecurityProtocol.SSL), false, false);
        assertEquals(1, topicMetadata.size());
        assertEquals(1, topicMetadata.get(0).partitions().size());
        assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, topicMetadata.get(0).partitions().get(0).leaderId());
    }

    @Test
    public void getAliveBrokersShouldNotBeMutatedByUpdateCache() {
        MetadataCache cache = createCache();
        String topic = "topic";
        Uuid topicId = Uuid.randomUuid();
        List<ApiMessage> topicRecords = List.of(
            new TopicRecord().setName(topic).setTopicId(topicId));

        List<Integer> initialBrokerIds = List.of(0, 1, 2);
        updateCacheWithBrokers(cache, initialBrokerIds, topicId, topicRecords);
        // This should not change the alive brokers
        updateCacheWithBrokers(cache, List.of(0, 1, 2, 3), topicId, topicRecords);
        for (int brokerId : initialBrokerIds) {
            assertTrue(cache.hasAliveBroker(brokerId));
        }
    }

    private void updateCacheWithBrokers(MetadataCache cache, List<Integer> brokerIds,
                                         Uuid topicId, List<ApiMessage> topicRecords) {
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        List<ApiMessage> records = new ArrayList<>();
        for (int brokerId : brokerIds) {
            records.add(new RegisterBrokerRecord()
                .setBrokerId(brokerId)
                .setRack("")
                .setFenced(false)
                .setBrokerEpoch(BROKER_EPOCH)
                .setEndPoints(new BrokerEndpointCollection(List.of(
                    new BrokerEndpoint()
                        .setHost("foo")
                        .setPort(9092)
                        .setSecurityProtocol(securityProtocol.id)
                        .setName(ListenerName.forSecurityProtocol(securityProtocol).value())
                ))));
        }
        records.addAll(topicRecords);
        records.add(new PartitionRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setLeader(0)
            .setLeaderEpoch(0)
            .setIsr(List.of(0, 1))
            .setReplicas(List.of(0)));
        updateCache(cache, records);
    }

    @Test
    public void testGetPartitionReplicaEndpoints() {
        MetadataCache cache = createCache();
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);

        // Set up broker data for the metadata cache
        int numBrokers = 10;
        int fencedBrokerId = numBrokers / 3;
        List<RegisterBrokerRecord> brokerRecords = new ArrayList<>();
        for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
            brokerRecords.add(new RegisterBrokerRecord()
                .setBrokerId(brokerId)
                .setFenced(brokerId == fencedBrokerId)
                .setRack("rack" + (brokerId % 3))
                .setEndPoints(new BrokerEndpointCollection(List.of(
                    new BrokerEndpoint()
                        .setHost("foo" + brokerId)
                        .setPort(9092)
                        .setSecurityProtocol(securityProtocol.id)
                        .setName(listenerName.value())
                ))));
        }

        // Set up a single topic (with many partitions) for the metadata cache
        String topic = "many-partitions-topic";
        Uuid topicId = Uuid.randomUuid();

        // Set up a number of partitions such that each different combination of
        // $replicationFactor brokers is made a replica set for exactly one partition
        int replicationFactor = 3;
        List<List<Integer>> replicaSets = getAllReplicaSets(numBrokers, replicationFactor);
        int numPartitions = replicaSets.size();

        List<PartitionRecord> partitionRecords = new ArrayList<>();
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<Integer> replicas = replicaSets.get(partitionId);
            List<Integer> nonFencedReplicas = new ArrayList<>();
            for (Integer id : replicas) {
                if (id != fencedBrokerId) nonFencedReplicas.add(id);
            }
            partitionRecords.add(new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(partitionId)
                .setReplicas(replicas)
                .setLeader(replicas.get(0))
                .setIsr(nonFencedReplicas)
                .setEligibleLeaderReplicas(nonFencedReplicas));
        }

        // Load the prepared data in the metadata cache
        List<ApiMessage> records = new ArrayList<>(brokerRecords);
        records.add(new TopicRecord().setName(topic).setTopicId(topicId));
        records.addAll(partitionRecords);
        updateCache(cache, records);

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            TopicPartition tp = new TopicPartition(topic, partitionId);
            Map<Integer, Node> brokerIdToNodeMap =
                cache.getPartitionReplicaEndpoints(tp, listenerName);
            Set<Integer> replicaSet = brokerIdToNodeMap.keySet();
            Set<Integer> expectedReplicaSet = new HashSet<>(partitionRecords.get(partitionId).replicas());

            // Verify that we have endpoints for exactly the non-fenced brokers of the replica set
            if (expectedReplicaSet.contains(fencedBrokerId)) {
                Set<Integer> replicaSetPlusFenced = new HashSet<>(replicaSet);
                replicaSetPlusFenced.add(fencedBrokerId);
                assertEquals(expectedReplicaSet, replicaSetPlusFenced,
                    "Unexpected partial replica set for partition " + partitionId);
            } else {
                assertEquals(expectedReplicaSet, replicaSet,
                    "Unexpected replica set for partition " + partitionId);
            }

            // Verify that the endpoint data for each non-fenced replica is as expected
            for (int brokerId : replicaSet) {
                Node brokerNode = brokerIdToNodeMap.get(brokerId);
                if (brokerNode == null) {
                    throw new AssertionError("No brokerNode for broker " + brokerId + " and partition " + partitionId);
                }
                RegisterBrokerRecord expectedBroker = brokerRecords.get(brokerId);
                BrokerEndpoint expectedEndpoint = expectedBroker.endPoints().find(listenerName.value());
                assertEquals(expectedEndpoint.host(), brokerNode.host(),
                    "Unexpected host for broker " + brokerId + " and partition " + partitionId);
                assertEquals(expectedEndpoint.port(), brokerNode.port(),
                    "Unexpected port for broker " + brokerId + " and partition " + partitionId);
                assertEquals(expectedBroker.rack(), brokerNode.rack(),
                    "Unexpected rack for broker " + brokerId + " and partition " + partitionId);
            }
        }

        TopicPartition tp = new TopicPartition(topic, numPartitions);
        Map<Integer, Node> brokerIdToNodeMap =
            cache.getPartitionReplicaEndpoints(tp, listenerName);
        assertTrue(brokerIdToNodeMap.isEmpty());
    }

    /**
     * Returns every possible replica set (combination of broker IDs) for the given
     * {@code numBrokers} and {@code replicationFactor}.
     *
     * <p>Broker IDs are in the range {@code [0, numBrokers)}.  The result contains all
     * C(numBrokers, replicationFactor) ordered-ascending combinations, each represented
     * as a {@code List<Integer>}.
     *
     * <p>The algorithm maintains an {@code indices} array of length {@code replicationFactor}
     * whose entries are always strictly increasing broker IDs.  Each iteration records the
     * current combination and then advances to the lexicographically next one using the
     * standard "next k-combination" technique:
     * <ol>
     *   <li>Find the rightmost position {@code i} that can still be incremented (i.e.
     *       {@code indices[i] < numBrokers - replicationFactor + i}).</li>
     *   <li>Increment {@code indices[i]}.</li>
     *   <li>Reset every position to the right of {@code i} to the smallest valid values
     *       ({@code indices[j] = indices[j-1] + 1}).</li>
     *   <li>If no such position exists ({@code i < 0}), all combinations have been
     *       enumerated and the loop ends.</li>
     * </ol>
     *
     * <p>Example – {@code numBrokers=3, replicationFactor=2} produces:
     * {@code [[0,1], [0,2], [1,2]]}.
     */
    private static List<List<Integer>> getAllReplicaSets(int numBrokers, int replicationFactor) {
        List<List<Integer>> result = new ArrayList<>();

        // Start with the lexicographically smallest combination: [0, 1, 2, ..., replicationFactor-1]
        int[] indices = new int[replicationFactor];
        for (int i = 0; i < replicationFactor; i++) indices[i] = i;

        while (true) {
            // Record the current combination
            List<Integer> combo = new ArrayList<>();
            for (int idx : indices) combo.add(idx);
            result.add(combo);

            // Find the rightmost index that has not yet reached its maximum value.
            // The maximum value for position i is (numBrokers - replicationFactor + i),
            // which leaves enough room for all subsequent positions to have distinct IDs.
            int i = replicationFactor - 1;
            while (i >= 0 && indices[i] == numBrokers - replicationFactor + i) i--;

            // All combinations have been generated; stop.
            if (i < 0) break;

            // Increment the found position and reset everything to its right to
            // the smallest contiguous values that keep the array strictly increasing.
            indices[i]++;
            for (int j = i + 1; j < replicationFactor; j++) indices[j] = indices[j - 1] + 1;
        }

        return result;
    }

    @Test
    public void testIsBrokerFenced() {
        KRaftMetadataCache metadataCache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_0);

        MetadataDelta delta = new MetadataDelta.Builder().build();
        delta.replay(new RegisterBrokerRecord()
            .setBrokerId(0)
            .setFenced(false));

        metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY));

        assertFalse(metadataCache.isBrokerFenced(0));

        delta.replay(new BrokerRegistrationChangeRecord()
            .setBrokerId(0)
            .setFenced((byte) 1));

        metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY));

        assertTrue(metadataCache.isBrokerFenced(0));
    }

    @Test
    public void testIsBrokerInControlledShutdown() {
        KRaftMetadataCache metadataCache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_0);

        MetadataDelta delta = new MetadataDelta.Builder().build();
        delta.replay(new RegisterBrokerRecord()
            .setBrokerId(0)
            .setInControlledShutdown(false));

        metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY));

        assertFalse(metadataCache.isBrokerShuttingDown(0));

        delta.replay(new BrokerRegistrationChangeRecord()
            .setBrokerId(0)
            .setInControlledShutdown((byte) 1));

        metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY));

        assertTrue(metadataCache.isBrokerShuttingDown(0));
    }

    @Test
    public void testGetLiveBrokerEpoch() {
        KRaftMetadataCache metadataCache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_0);

        MetadataDelta delta = new MetadataDelta.Builder().build();
        delta.replay(new RegisterBrokerRecord()
            .setBrokerId(0)
            .setBrokerEpoch(100)
            .setFenced(false));

        delta.replay(new RegisterBrokerRecord()
            .setBrokerId(1)
            .setBrokerEpoch(101)
            .setFenced(true));

        metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY));

        assertEquals(100L, metadataCache.getAliveBrokerEpoch(0).orElse(-1L));
        assertEquals(-1L, metadataCache.getAliveBrokerEpoch(1).orElse(-1L));
    }

    @Test
    public void testDescribeTopicResponse() {
        KRaftMetadataCache metadataCache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_0);

        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        String topic0 = "test0";
        String topic1 = "test1";

        Map<String, Uuid> topicIds = Map.of(
            topic0, Uuid.randomUuid(),
            topic1, Uuid.randomUuid()
        );

        // partitionMap key: "topicName:partitionId"
        Map<String, PartitionRecord> partitionMap = Map.of(
            topic0 + ":0", new PartitionRecord()
                .setTopicId(topicIds.get(topic0))
                .setPartitionId(0)
                .setReplicas(List.of(0, 1, 2))
                .setLeader(0)
                .setIsr(List.of(0))
                .setEligibleLeaderReplicas(List.of(1))
                .setLastKnownElr(List.of(2))
                .setLeaderEpoch(0)
                .setPartitionEpoch(1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
            topic0 + ":2", new PartitionRecord()
                .setTopicId(topicIds.get(topic0))
                .setPartitionId(2)
                .setReplicas(List.of(0, 2, 3))
                .setLeader(3)
                .setIsr(List.of(3))
                .setEligibleLeaderReplicas(List.of(2))
                .setLastKnownElr(List.of(0))
                .setLeaderEpoch(1)
                .setPartitionEpoch(2)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
            topic0 + ":1", new PartitionRecord()
                .setTopicId(topicIds.get(topic0))
                .setPartitionId(1)
                .setReplicas(List.of(0, 1, 3))
                .setLeader(0)
                .setIsr(List.of(0))
                .setEligibleLeaderReplicas(List.of(1))
                .setLastKnownElr(List.of(3))
                .setLeaderEpoch(0)
                .setPartitionEpoch(2)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
            topic1 + ":0", new PartitionRecord()
                .setTopicId(topicIds.get(topic1))
                .setPartitionId(0)
                .setReplicas(List.of(0, 1, 2))
                .setLeader(2)
                .setIsr(List.of(2))
                .setEligibleLeaderReplicas(List.of(1))
                .setLastKnownElr(List.of(0))
                .setLeaderEpoch(10)
                .setPartitionEpoch(11)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
        );

        List<ApiMessage> records = new ArrayList<>();
        records.add(new RegisterBrokerRecord().setBrokerEpoch(BROKER_EPOCH).setFenced(false).setBrokerId(0)
            .setEndPoints(new BrokerEndpointCollection(List.of(
                new BrokerEndpoint().setHost("foo0").setPort(9092)
                    .setSecurityProtocol(securityProtocol.id).setName(listenerName.value())
            ))));
        records.add(new RegisterBrokerRecord().setBrokerEpoch(BROKER_EPOCH).setFenced(false).setBrokerId(1)
            .setEndPoints(new BrokerEndpointCollection(List.of(
                new BrokerEndpoint().setHost("foo1").setPort(9093)
                    .setSecurityProtocol(securityProtocol.id).setName(listenerName.value())
            ))));
        records.add(new RegisterBrokerRecord().setBrokerEpoch(BROKER_EPOCH).setFenced(false).setBrokerId(2)
            .setEndPoints(new BrokerEndpointCollection(List.of(
                new BrokerEndpoint().setHost("foo2").setPort(9094)
                    .setSecurityProtocol(securityProtocol.id).setName(listenerName.value())
            ))));
        records.add(new RegisterBrokerRecord().setBrokerEpoch(BROKER_EPOCH).setFenced(false).setBrokerId(3)
            .setEndPoints(new BrokerEndpointCollection(List.of(
                new BrokerEndpoint().setHost("foo3").setPort(9095)
                    .setSecurityProtocol(securityProtocol.id).setName(listenerName.value())
            ))));
        records.add(new TopicRecord().setName(topic0).setTopicId(topicIds.get(topic0)));
        records.add(new TopicRecord().setName(topic1).setTopicId(topicIds.get(topic1)));
        records.addAll(partitionMap.values());
        updateCache(metadataCache, records);

        // Basic test
        List<DescribeTopicPartitionsResponseTopic> result = metadataCache
            .describeTopicResponse(List.of(topic0, topic1).iterator(), listenerName, t -> 0, 10, false)
            .topics().stream().toList();
        assertEquals(2, result.size());
        DescribeTopicPartitionsResponseTopic resultTopic = result.get(0);
        assertEquals(topic0, resultTopic.name());
        assertEquals(0, resultTopic.errorCode());
        assertEquals(topicIds.get(topic0), resultTopic.topicId());
        assertEquals(3, resultTopic.partitions().size());
        checkTopicMetadata(topic0, Set.of(0, 1, 2), resultTopic.partitions(), partitionMap);

        resultTopic = result.get(1);
        assertEquals(topic1, resultTopic.name());
        assertEquals(0, resultTopic.errorCode());
        assertEquals(topicIds.get(topic1), resultTopic.topicId());
        assertEquals(1, resultTopic.partitions().size());
        checkTopicMetadata(topic1, Set.of(0), resultTopic.partitions(), partitionMap);

        // Quota reached
        DescribeTopicPartitionsResponseData response = metadataCache
            .describeTopicResponse(List.of(topic0, topic1).iterator(), listenerName, t -> 0, 2, false);
        result = response.topics().stream().toList();
        assertEquals(1, result.size());
        resultTopic = result.get(0);
        assertEquals(topic0, resultTopic.name());
        assertEquals(0, resultTopic.errorCode());
        assertEquals(topicIds.get(topic0), resultTopic.topicId());
        assertEquals(2, resultTopic.partitions().size());
        checkTopicMetadata(topic0, Set.of(0, 1), resultTopic.partitions(), partitionMap);
        assertEquals(topic0, response.nextCursor().topicName());
        assertEquals(2, response.nextCursor().partitionIndex());

        // With start index
        result = metadataCache
            .describeTopicResponse(List.of(topic0).iterator(), listenerName,
                t -> t.equals(topic0) ? 1 : 0, 10, false)
            .topics().stream().toList();
        assertEquals(1, result.size());
        resultTopic = result.get(0);
        assertEquals(topic0, resultTopic.name());
        assertEquals(0, resultTopic.errorCode());
        assertEquals(topicIds.get(topic0), resultTopic.topicId());
        assertEquals(2, resultTopic.partitions().size());
        checkTopicMetadata(topic0, Set.of(1, 2), resultTopic.partitions(), partitionMap);

        // With start index and quota reached
        response = metadataCache.describeTopicResponse(List.of(topic0, topic1).iterator(), listenerName,
            t -> t.equals(topic0) ? 2 : 0, 1, false);
        result = response.topics().stream().toList();
        assertEquals(1, result.size());
        resultTopic = result.get(0);
        assertEquals(topic0, resultTopic.name());
        assertEquals(0, resultTopic.errorCode());
        assertEquals(topicIds.get(topic0), resultTopic.topicId());
        assertEquals(1, resultTopic.partitions().size());
        checkTopicMetadata(topic0, Set.of(2), resultTopic.partitions(), partitionMap);
        assertEquals(topic1, response.nextCursor().topicName());
        assertEquals(0, response.nextCursor().partitionIndex());

        // When the first topic does not exist
        result = metadataCache.describeTopicResponse(List.of("Non-exist", topic0).iterator(), listenerName,
            t -> t.equals("Non-exist") ? 1 : 0, 1, false).topics().stream().toList();
        assertEquals(2, result.size());
        resultTopic = result.get(0);
        assertEquals("Non-exist", resultTopic.name());
        assertEquals(3, resultTopic.errorCode());

        resultTopic = result.get(1);
        assertEquals(topic0, resultTopic.name());
        assertEquals(0, resultTopic.errorCode());
        assertEquals(topicIds.get(topic0), resultTopic.topicId());
        assertEquals(1, resultTopic.partitions().size());
        checkTopicMetadata(topic0, Set.of(0), resultTopic.partitions(), partitionMap);
    }

    private void checkTopicMetadata(
        String topic,
        Set<Integer> partitionIds,
        List<DescribeTopicPartitionsResponsePartition> partitions,
        Map<String, PartitionRecord> partitionMap
    ) {
        for (DescribeTopicPartitionsResponsePartition partition : partitions) {
            int partitionId = partition.partitionIndex();
            assertTrue(partitionIds.contains(partitionId));
            PartitionRecord expectedPartition = partitionMap.get(topic + ":" + partitionId);
            assertEquals(0, partition.errorCode());
            assertEquals(expectedPartition.leaderEpoch(), partition.leaderEpoch());
            assertEquals(expectedPartition.partitionId(), partition.partitionIndex());
            assertEquals(expectedPartition.eligibleLeaderReplicas(), partition.eligibleLeaderReplicas());
            assertEquals(expectedPartition.isr(), partition.isrNodes());
            assertEquals(expectedPartition.lastKnownElr(), partition.lastKnownElr());
            assertEquals(expectedPartition.leader(), partition.leaderId());
        }
    }

    @Test
    public void testGetLeaderAndIsr() {
        MetadataCache cache = createCache();
        String topic = "topic";
        Uuid topicId = Uuid.randomUuid();
        int partitionIndex = 0;
        int leader = 0;
        int leaderEpoch = 0;
        List<Integer> isr = List.of(2, 3, 0);
        List<Integer> replicas = List.of(2, 3, 0, 1, 4);

        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);

        List<ApiMessage> records = List.of(
            new RegisterBrokerRecord()
                .setBrokerId(0)
                .setBrokerEpoch(BROKER_EPOCH)
                .setRack("rack1")
                .setEndPoints(new BrokerEndpointCollection(List.of(
                    new BrokerEndpoint()
                        .setHost("foo")
                        .setPort(9092)
                        .setSecurityProtocol(securityProtocol.id)
                        .setName(listenerName.value())
                ))),
            new TopicRecord().setName(topic).setTopicId(topicId),
            new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(partitionIndex)
                .setLeader(leader)
                .setLeaderEpoch(leaderEpoch)
                .setIsr(isr)
                .setReplicas(replicas)
        );
        updateCache(cache, records);

        Optional<LeaderAndIsr> leaderAndIsr = cache.getLeaderAndIsr(topic, partitionIndex);
        assertEquals(Optional.of(leader), leaderAndIsr.map(LeaderAndIsr::leader));
        assertEquals(Optional.of(leaderEpoch), leaderAndIsr.map(LeaderAndIsr::leaderEpoch));
        assertEquals(Optional.of(Set.of(2, 3, 0)), leaderAndIsr.map(LeaderAndIsr::isr));
        assertEquals(Optional.of(-1), leaderAndIsr.map(LeaderAndIsr::partitionEpoch));
        assertEquals(Optional.of(LeaderRecoveryState.RECOVERED), leaderAndIsr.map(LeaderAndIsr::leaderRecoveryState));
    }

    @Test
    public void testGetOfflineReplicasConsidersDirAssignment() {
        Map<Integer, List<Integer>> result = offlinePartitions(
            List.of(
                new Broker(0, List.of(Uuid.fromString("broker1logdirjEo71BG0w"))),
                new Broker(1, List.of(Uuid.fromString("broker2logdirRmQQgLxgw")))
            ),
            List.of(
                new Partition(0, List.of(0, 1),
                    List.of(Uuid.fromString("broker1logdirjEo71BG0w"), DirectoryId.LOST)),
                new Partition(1, List.of(0, 1),
                    List.of(Uuid.fromString("unknownlogdirjEo71BG0w"), DirectoryId.UNASSIGNED)),
                new Partition(2, List.of(0, 1),
                    List.of(DirectoryId.MIGRATING, Uuid.fromString("broker2logdirRmQQgLxgw")))
            )
        );

        Map<Integer, List<Integer>> expected = Map.of(
            0, List.of(1),
            1, List.of(0),
            2, List.of()
        );
        assertEquals(expected, result);
    }

    private record Broker(int id, List<Uuid> dirs) {
    }

    @Test
    public void testToClusterIncludesFencedReplicasAsOffline() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        RegisterBrokerRecord broker0RegisterRecord = new RegisterBrokerRecord()
            .setBrokerId(0)
            .setFenced(false)
            .setEndPoints(new BrokerEndpointCollection(List.of(
                new BrokerEndpoint()
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                    .setPort((short) 9092)
                    .setName("PLAINTEXT")
                    .setHost("broker-0")
            )));
        // Register broker 1 as fenced
        RegisterBrokerRecord broker1RegisterRecord = new RegisterBrokerRecord()
            .setBrokerId(1)
            .setFenced(true)
            .setEndPoints(new BrokerEndpointCollection(List.of(
                new BrokerEndpoint()
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                    .setPort((short) 9092)
                    .setName("PLAINTEXT")
                    .setHost("broker-1")
            )));
        delta.replay(broker0RegisterRecord);
        delta.replay(broker1RegisterRecord);

        String topicName = "foo";
        Uuid topicId = Uuid.randomUuid();
        int partitionId = 0;
        int broker0 = broker0RegisterRecord.brokerId();
        int broker1 = broker1RegisterRecord.brokerId();
        delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
        // Broker 1 is not in the ISR, but it remains in the replica set.
        // This used to trigger a NullPointerException when fenced brokers were
        // omitted from the broker map used to build replica metadata.
        delta.replay(new PartitionRecord()
            .setTopicId(topicId)
            .setPartitionId(partitionId)
            .setReplicas(List.of(broker0, broker1))
            .setLeader(broker0)
            .setIsr(List.of(broker0)));

        Cluster cluster = assertDoesNotThrow(() ->
            MetadataCache.toCluster("cluster-id", delta.apply(MetadataProvenance.EMPTY)));
        PartitionInfo partition = cluster.partition(new TopicPartition(topicName, partitionId));

        assertEquals(List.of(broker0, broker1), Arrays.stream(partition.replicas()).map(Node::id).toList());
        assertEquals(List.of(broker1), Arrays.stream(partition.offlineReplicas()).map(Node::id).toList());
        assertTrue(cluster.nodeById(broker1).isFenced());
    }

    private record Partition(int id, List<Integer> replicas, List<Uuid> dirs) {
    }

    private static Map<Integer, List<Integer>> offlinePartitions(
        List<Broker> brokers,
        List<Partition> partitions
    ) {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        for (Broker broker : brokers) {
            delta.replay(new RegisterBrokerRecord()
                .setFenced(false)
                .setBrokerId(broker.id)
                .setLogDirs(broker.dirs)
                .setEndPoints(new BrokerEndpointCollection(List.of(
                    new BrokerEndpoint()
                        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                        .setPort((short) 9093)
                        .setName("PLAINTEXT")
                        .setHost("broker-" + broker.id)
                ))));
        }
        Uuid topicId = Uuid.fromString("95OVr1IPRYGrcNCLlpImCA");
        delta.replay(new TopicRecord().setTopicId(topicId).setName("foo"));
        for (Partition partition : partitions) {
            delta.replay(new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(partition.id)
                .setReplicas(partition.replicas)
                .setDirectories(partition.dirs)
                .setLeader(partition.replicas.get(0))
                .setIsr(partition.replicas));
        }
        KRaftMetadataCache cache = new KRaftMetadataCache(1, () -> KRaftVersion.KRAFT_VERSION_0);
        cache.setImage(delta.apply(MetadataProvenance.EMPTY));
        List<MetadataResponseData.MetadataResponseTopic> topicMetadata =
            cache.getTopicMetadata(Set.of("foo"), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), false, false);
        return topicMetadata.get(0).partitions().stream()
            .collect(Collectors.toMap(
                MetadataResponseData.MetadataResponsePartition::partitionIndex,
                MetadataResponseData.MetadataResponsePartition::offlineReplicas
            ));
    }
}

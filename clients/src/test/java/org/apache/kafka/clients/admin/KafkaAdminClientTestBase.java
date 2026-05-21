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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResultCollection;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListConfigResourcesResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListConfigResourcesResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.UnregisterBrokerResponse;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared scaffolding for KafkaAdminClient unit tests. The test methods themselves live in
 * {@link KafkaAdminClientTest} and the per-domain subclasses (topic, consumer-group, streams-group,
 * share-group, config, transaction). Helpers and constants are placed here so they're available
 * to every subclass without duplication.
 */
public abstract class KafkaAdminClientTestBase {
    protected static final Logger log = LoggerFactory.getLogger(KafkaAdminClientTestBase.class);
    protected static final String GROUP_ID = "group-0";
    public static final Uuid REPLICA_DIRECTORY_ID = Uuid.randomUuid();

    protected static Map<String, Object> newStrMap(String... vals) {
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121");
        map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        if (vals.length % 2 != 0) {
            throw new IllegalStateException();
        }
        for (int i = 0; i < vals.length; i += 2) {
            map.put(vals[i], vals[i + 1]);
        }
        return map;
    }

    protected static AdminClientConfig newConfMap(String... vals) {
        return new AdminClientConfig(newStrMap(vals));
    }

    protected static Cluster mockCluster(int numNodes, int controllerIndex) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; i++)
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        return new Cluster("mockClusterId", nodes.values(),
            Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), nodes.get(controllerIndex));
    }

    protected static Cluster mockBootstrapCluster() {
        return Cluster.bootstrap(ClientUtils.parseAndValidateAddresses(
                singletonList("localhost:8121"), ClientDnsLookup.USE_ALL_DNS_IPS));
    }

    protected static AdminClientUnitTestEnv mockClientEnv(String... configVals) {
        return new AdminClientUnitTestEnv(mockCluster(3, 0), configVals);
    }

    protected static AdminClientUnitTestEnv mockClientEnv(Time time, String... configVals) {
        return new AdminClientUnitTestEnv(time, mockCluster(3, 0), configVals);
    }

    protected static OffsetDeleteResponse prepareOffsetDeleteResponse(Errors error) {
        return new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setErrorCode(error.code())
                .setTopics(new OffsetDeleteResponseTopicCollection())
        );
    }

    protected static OffsetDeleteResponse prepareOffsetDeleteResponse(String topic, int partition, Errors error) {
        return new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(new OffsetDeleteResponseTopicCollection(Stream.of(
                    new OffsetDeleteResponseTopic()
                        .setName(topic)
                        .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                            new OffsetDeleteResponsePartition()
                                .setPartitionIndex(partition)
                                .setErrorCode(error.code())
                        )))
                ).collect(Collectors.toList())))
        );
    }

    protected static OffsetCommitResponse prepareOffsetCommitResponse(TopicPartition tp, Errors error) {
        Map<TopicPartition, Errors> responseData = new HashMap<>();
        responseData.put(tp, error);
        return new OffsetCommitResponse(0, responseData);
    }

    protected static CreateTopicsResponse prepareCreateTopicsResponse(String topicName, Errors error) {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreatableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code()));
        return new CreateTopicsResponse(data);
    }

    public static CreateTopicsResponse prepareCreateTopicsResponse(int throttleTimeMs, CreatableTopicResult... topics) {
        CreateTopicsResponseData data = new CreateTopicsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setTopics(new CreatableTopicResultCollection(Arrays.asList(topics)));
        return new CreateTopicsResponse(data);
    }

    public static CreatableTopicResult creatableTopicResult(String name, Errors error) {
        return new CreatableTopicResult()
            .setName(name)
            .setErrorCode(error.code());
    }

    public static DeleteTopicsResponse prepareDeleteTopicsResponse(int throttleTimeMs, DeletableTopicResult... topics) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResponses(new DeletableTopicResultCollection(Arrays.asList(topics)));
        return new DeleteTopicsResponse(data);
    }

    public static DeletableTopicResult deletableTopicResult(String topicName, Errors error) {
        return new DeletableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code());
    }

    public static DeletableTopicResult deletableTopicResultWithId(Uuid topicId, Errors error) {
        return new DeletableTopicResult()
                .setTopicId(topicId)
                .setErrorCode(error.code());
    }

    public static CreatePartitionsResponse prepareCreatePartitionsResponse(int throttleTimeMs, CreatePartitionsTopicResult... topics) {
        CreatePartitionsResponseData data = new CreatePartitionsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResults(asList(topics));
        return new CreatePartitionsResponse(data);
    }

    public static CreatePartitionsTopicResult createPartitionsTopicResult(String name, Errors error) {
        return createPartitionsTopicResult(name, error, null);
    }

    public static CreatePartitionsTopicResult createPartitionsTopicResult(String name, Errors error, String errorMessage) {
        return new CreatePartitionsTopicResult()
            .setName(name)
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    protected static DeleteTopicsResponse prepareDeleteTopicsResponse(String topicName, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    protected static DeleteTopicsResponse prepareDeleteTopicsResponseWithTopicId(Uuid id, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
                .setTopicId(id)
                .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    protected static FindCoordinatorResponse prepareFindCoordinatorResponse(Errors error, Node node) {
        return prepareFindCoordinatorResponse(error, GROUP_ID, node);
    }

    protected static FindCoordinatorResponse prepareFindCoordinatorResponse(Errors error, String key, Node node) {
        return FindCoordinatorResponse.prepareResponse(error, key, node);
    }

    protected static FindCoordinatorResponse prepareOldFindCoordinatorResponse(Errors error, Node node) {
        return FindCoordinatorResponse.prepareOldResponse(error, node);
    }

    protected static FindCoordinatorResponse prepareBatchedFindCoordinatorResponse(Errors error, Node node, Collection<String> groups) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        List<FindCoordinatorResponseData.Coordinator> coordinators = groups.stream()
                .map(group -> new FindCoordinatorResponseData.Coordinator()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
                        .setKey(group)
                        .setHost(node.host())
                        .setPort(node.port())
                        .setNodeId(node.id()))
                .collect(Collectors.toList());
        data.setCoordinators(coordinators);
        return new FindCoordinatorResponse(data);
    }

    protected static MetadataResponse prepareMetadataResponse(Cluster cluster, Errors error) {
        return prepareMetadataResponse(cluster, error, error);
    }

    protected static MetadataResponse prepareMetadataResponse(Cluster cluster, Errors topicError, Errors partitionError) {
        List<MetadataResponseTopic> metadata = new ArrayList<>();
        for (String topic : cluster.topics()) {
            List<MetadataResponsePartition> pms = new ArrayList<>();
            for (PartitionInfo pInfo : cluster.availablePartitionsForTopic(topic)) {
                MetadataResponsePartition pm  = new MetadataResponsePartition()
                    .setErrorCode(partitionError.code())
                    .setPartitionIndex(pInfo.partition())
                    .setLeaderId(pInfo.leader().id())
                    .setLeaderEpoch(234)
                    .setReplicaNodes(Arrays.stream(pInfo.replicas()).map(Node::id).collect(Collectors.toList()))
                    .setIsrNodes(Arrays.stream(pInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toList()))
                    .setOfflineReplicas(Arrays.stream(pInfo.offlineReplicas()).map(Node::id).collect(Collectors.toList()));
                pms.add(pm);
            }
            MetadataResponseTopic tm = new MetadataResponseTopic()
                .setErrorCode(topicError.code())
                .setName(topic)
                .setIsInternal(false)
                .setPartitions(pms);
            metadata.add(tm);
        }
        return MetadataResponse.prepareResponse(true,
                0,
                cluster.nodes(),
                cluster.clusterResource().clusterId(),
                cluster.controller().id(),
                metadata,
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
    }

    protected static DescribeGroupsResponseData prepareDescribeGroupsResponseData(String groupId,
                                                                                List<String> groupInstances,
                                                                                List<TopicPartition> topicPartitions) {
        final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
        List<DescribedGroupMember> describedGroupMembers = groupInstances.stream().map(groupInstance -> DescribeGroupsResponse.groupMember(JoinGroupRequest.UNKNOWN_MEMBER_ID,
                groupInstance, "clientId0", "clientHost", new byte[memberAssignment.remaining()], null)).collect(Collectors.toList());
        DescribeGroupsResponseData data = new DescribeGroupsResponseData();
        data.groups().add(DescribeGroupsResponse.groupMetadata(
                groupId,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                describedGroupMembers,
                Collections.emptySet()));
        return data;
    }

    protected static FeatureMetadata defaultFeatureMetadata() {
        return new FeatureMetadata(
            Map.of("test_feature_1", new FinalizedVersionRange((short) 2, (short) 2)),
            Optional.of(1L),
            Map.of("test_feature_1", new SupportedVersionRange((short) 1, (short) 5)));
    }

    protected static Features<org.apache.kafka.common.feature.SupportedVersionRange> convertSupportedFeaturesMap(Map<String, SupportedVersionRange> features) {
        final Map<String, org.apache.kafka.common.feature.SupportedVersionRange> featuresMap = new HashMap<>();
        for (final Map.Entry<String, SupportedVersionRange> entry : features.entrySet()) {
            final SupportedVersionRange versionRange = entry.getValue();
            featuresMap.put(
                entry.getKey(),
                new org.apache.kafka.common.feature.SupportedVersionRange(versionRange.minVersion(),
                                                                          versionRange.maxVersion()));
        }

        return Features.supportedFeatures(featuresMap);
    }

    protected static ApiVersionsResponse prepareApiVersionsResponseForDescribeFeatures(Errors error) {
        if (error == Errors.NONE) {
            return new ApiVersionsResponse.Builder().
                setApiVersions(ApiVersionsResponse.filterApis(
                    ApiMessageType.ListenerType.BROKER, false, false)).
                setSupportedFeatures(
                    convertSupportedFeaturesMap(defaultFeatureMetadata().supportedFeatures())).
                setFinalizedFeatures(
                    Collections.singletonMap("test_feature_1", (short) 2)).
                setFinalizedFeaturesEpoch(
                    defaultFeatureMetadata().finalizedFeaturesEpoch().get()).
                build();
        }
        return new ApiVersionsResponse(
            new ApiVersionsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode(error.code()));
    }

    protected static QuorumInfo defaultQuorumInfo(boolean emptyOptionals) {
        return new QuorumInfo(1, 1, 1L,
                singletonList(new QuorumInfo.ReplicaState(1,
                        emptyOptionals ? Uuid.ZERO_UUID : REPLICA_DIRECTORY_ID,
                        100,
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000),
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000))),
                singletonList(new QuorumInfo.ReplicaState(1,
                        emptyOptionals ? Uuid.ZERO_UUID : REPLICA_DIRECTORY_ID,
                        100,
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000),
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000))),
                singletonMap(1, new QuorumInfo.Node(1, Collections.emptyList())));
    }

    protected static DescribeQuorumResponse prepareDescribeQuorumResponse(
            Errors topLevelError,
            Errors partitionLevelError,
            Boolean topicCountError,
            Boolean topicNameError,
            Boolean partitionCountError,
            Boolean partitionIndexError,
            Boolean emptyOptionals) {
        String topicName = topicNameError ? "RANDOM" : Topic.CLUSTER_METADATA_TOPIC_NAME;
        int partitionIndex = partitionIndexError ? 1 : Topic.CLUSTER_METADATA_TOPIC_PARTITION.partition();
        List<DescribeQuorumResponseData.TopicData> topics = new ArrayList<>();
        List<DescribeQuorumResponseData.PartitionData> partitions = new ArrayList<>();
        for (int i = 0; i < (partitionCountError ? 2 : 1); i++) {
            DescribeQuorumResponseData.ReplicaState replica = new DescribeQuorumResponseData.ReplicaState()
                    .setReplicaId(1)
                    .setReplicaDirectoryId(emptyOptionals ? Uuid.ZERO_UUID : REPLICA_DIRECTORY_ID)
                    .setLogEndOffset(100);
            replica.setLastFetchTimestamp(emptyOptionals ? -1 : 1000);
            replica.setLastCaughtUpTimestamp(emptyOptionals ? -1 : 1000);
            partitions.add(new DescribeQuorumResponseData.PartitionData().setPartitionIndex(partitionIndex)
                    .setLeaderId(1)
                    .setLeaderEpoch(1)
                    .setHighWatermark(1)
                    .setCurrentVoters(singletonList(replica))
                    .setObservers(singletonList(replica))
                    .setErrorCode(partitionLevelError.code())
                    .setErrorMessage(partitionLevelError.message()));
        }
        for (int i = 0; i < (topicCountError ? 2 : 1); i++) {
            topics.add(new DescribeQuorumResponseData.TopicData().setTopicName(topicName).setPartitions(partitions));
        }
        return new DescribeQuorumResponse(new DescribeQuorumResponseData()
            .setTopics(topics)
            .setErrorCode(topLevelError.code())
            .setErrorMessage(topLevelError.message())
            .setNodes(new DescribeQuorumResponseData.NodeCollection(Collections.singleton(new DescribeQuorumResponseData.Node().setNodeId(1)))));
    }

    protected void verifyUnreachableBootstrapServer(MetadataRecoveryStrategy metadataRecoveryStrategy) throws Exception {
        // This tests the scenario in which the bootstrap server is unreachable for a short while,
        // which prevents AdminClient from being able to send the initial metadata request

        Cluster cluster = Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 8121)));
        Map<Node, Long> unreachableNodes = Collections.singletonMap(cluster.nodes().get(0), 200L);
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                AdminClientUnitTestEnv.clientConfigs(AdminClientConfig.METADATA_RECOVERY_STRATEGY_CONFIG, metadataRecoveryStrategy.name), unreachableNodes)) {
            Cluster discoveredCluster = mockCluster(3, 0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(body -> body instanceof MetadataRequest,
                    RequestTestUtils.metadataResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP) {
                env.kafkaClient().prepareResponse(body -> body instanceof MetadataRequest,
                        RequestTestUtils.metadataResponse(discoveredCluster.nodes(),
                                discoveredCluster.clusterResource().clusterId(), 1, Collections.emptyList()));
            }
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    protected MockClient.RequestMatcher expectCreateTopicsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof CreateTopicsRequest) {
                CreateTopicsRequest request = (CreateTopicsRequest) body;
                for (String topic : topics) {
                    if (request.data().topics().find(topic) == null)
                        return false;
                }
                return topics.length == request.data().topics().size();
            }
            return false;
        };
    }

    protected MockClient.RequestMatcher expectDeleteTopicsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof DeleteTopicsRequest) {
                DeleteTopicsRequest request = (DeleteTopicsRequest) body;
                return request.topicNames().equals(asList(topics));
            }
            return false;
        };
    }

    protected MockClient.RequestMatcher expectDeleteTopicsRequestWithTopicIds(final Uuid... topicIds) {
        return body -> {
            if (body instanceof DeleteTopicsRequest) {
                DeleteTopicsRequest request = (DeleteTopicsRequest) body;
                return request.topicIds().equals(asList(topicIds));
            }
            return false;
        };
    }

    protected void addPartitionToDescribeTopicPartitionsResponse(
        DescribeTopicPartitionsResponseData data,
        String topicName,
        Uuid topicId,
        List<Integer> partitions) {
        List<DescribeTopicPartitionsResponsePartition> addingPartitions = new ArrayList<>();
        partitions.forEach(partition ->
            addingPartitions.add(new DescribeTopicPartitionsResponsePartition()
                .setIsrNodes(singletonList(0))
                .setErrorCode((short) 0)
                .setLeaderEpoch(0)
                .setLeaderId(0)
                .setEligibleLeaderReplicas(singletonList(1))
                .setLastKnownElr(singletonList(2))
                .setPartitionIndex(partition)
                .setReplicaNodes(asList(0, 1, 2)))
        );
        data.topics().add(new DescribeTopicPartitionsResponseTopic()
                .setErrorCode((short) 0)
                .setTopicId(topicId)
                .setName(topicName)
                .setIsInternal(false)
                .setPartitions(addingPartitions));
    }

    protected void callAdminClientApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> env.adminClient().createTopics(
            singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
            new CreateTopicsOptions().timeoutMs(10000)).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        Map<String, NewPartitions> counts = new HashMap<>();
        counts.put("my_topic", NewPartitions.increaseTo(3));
        counts.put("other_topic", NewPartitions.increaseTo(3, asList(singletonList(2), singletonList(3))));
        e = assertThrows(ExecutionException.class, () -> env.adminClient().createPartitions(counts).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().createAcls(asList(ACL1, ACL2)).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().describeAcls(FILTER1).values().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().deleteAcls(asList(FILTER1, FILTER2)).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().describeConfigs(
            singleton(new ConfigResource(ConfigResource.Type.BROKER, "0"))).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));
    }

    protected void callClientQuotasApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) {
        ExecutionException e = assertThrows(ExecutionException.class,
            () -> env.adminClient().describeClientQuotas(ClientQuotaFilter.all()).entities().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "user"));
        ClientQuotaAlteration alteration = new ClientQuotaAlteration(entity, singletonList(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)));
        e = assertThrows(ExecutionException.class,
            () -> env.adminClient().alterClientQuotas(singletonList(alteration)).all().get());

        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));
    }

    protected static final AclBinding ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    protected static final AclBinding ACL2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic4", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.DENY));
    protected static final AclBindingFilter FILTER1 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY));
    protected static final AclBindingFilter FILTER2 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));
    protected static final AclBindingFilter UNKNOWN_FILTER = new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));

    protected static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir, TopicPartition tp, long partitionSize, long offsetLag) {
        return prepareDescribeLogDirsResponse(error, logDir,
                prepareDescribeLogDirsTopics(partitionSize, offsetLag, tp.topic(), tp.partition(), false));
    }

    protected static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir, TopicPartition tp, long partitionSize, long offsetLag, long totalBytes, long usableBytes) {
        return prepareDescribeLogDirsResponse(error, logDir,
                prepareDescribeLogDirsTopics(partitionSize, offsetLag, tp.topic(), tp.partition(), false), totalBytes, usableBytes, false);
    }

    protected static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir, TopicPartition tp, long partitionSize, long offsetLag, long totalBytes, long usableBytes, boolean isCordoned) {
        return prepareDescribeLogDirsResponse(error, logDir,
                prepareDescribeLogDirsTopics(partitionSize, offsetLag, tp.topic(), tp.partition(), false), totalBytes, usableBytes, isCordoned);
    }

    protected static List<DescribeLogDirsTopic> prepareDescribeLogDirsTopics(
            long partitionSize, long offsetLag, String topic, int partition, boolean isFuture) {
        return singletonList(new DescribeLogDirsTopic()
                .setName(topic)
                .setPartitions(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                        .setPartitionIndex(partition)
                        .setPartitionSize(partitionSize)
                        .setIsFutureKey(isFuture)
                        .setOffsetLag(offsetLag))));
    }

    protected static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir,
                                                                   List<DescribeLogDirsTopic> topics) {
        return new DescribeLogDirsResponse(
                new DescribeLogDirsResponseData().setResults(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(error.code())
                        .setLogDir(logDir)
                        .setTopics(topics)
                )));
    }

    protected static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir,
                                                                          List<DescribeLogDirsTopic> topics,
                                                                          long totalBytes, long usableBytes,
                                                                          boolean isCordoned) {
        return new DescribeLogDirsResponse(
                new DescribeLogDirsResponseData().setResults(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(error.code())
                        .setLogDir(logDir)
                        .setTopics(topics)
                        .setTotalBytes(totalBytes)
                        .setUsableBytes(usableBytes)
                        .setIsCordoned(isCordoned)
                )));
    }

    protected static DescribeLogDirsResponse prepareEmptyDescribeLogDirsResponse(Optional<Errors> error) {
        DescribeLogDirsResponseData data = new DescribeLogDirsResponseData();
        error.ifPresent(e -> data.setErrorCode(e.code()));
        return new DescribeLogDirsResponse(data);
    }

    protected static void assertDescriptionContains(Map<String, LogDirDescription> descriptionsMap, String logDir,
                                           TopicPartition tp, long partitionSize, long offsetLag) {
        assertDescriptionContains(descriptionsMap, logDir, tp, partitionSize, offsetLag, OptionalLong.empty(), OptionalLong.empty());
    }

    protected static void assertDescriptionContains(Map<String, LogDirDescription> descriptionsMap, String logDir,
                                                  TopicPartition tp, long partitionSize, long offsetLag, OptionalLong totalBytes, OptionalLong usableBytes) {
        assertNotNull(descriptionsMap);
        assertEquals(singleton(logDir), descriptionsMap.keySet());
        assertNull(descriptionsMap.get(logDir).error());
        Map<TopicPartition, ReplicaInfo> descriptionsReplicaInfos = descriptionsMap.get(logDir).replicaInfos();
        assertEquals(singleton(tp), descriptionsReplicaInfos.keySet());
        assertEquals(partitionSize, descriptionsReplicaInfos.get(tp).size());
        assertEquals(offsetLag, descriptionsReplicaInfos.get(tp).offsetLag());
        assertFalse(descriptionsReplicaInfos.get(tp).isFuture());
        assertEquals(totalBytes, descriptionsMap.get(logDir).totalBytes());
        assertEquals(usableBytes, descriptionsMap.get(logDir).usableBytes());
        assertFalse(descriptionsMap.get(logDir).isCordoned());
    }

    protected static DescribeLogDirsResponseData.DescribeLogDirsResult prepareDescribeLogDirsResult(TopicPartitionReplica tpr, String logDir, int partitionSize, int offsetLag, boolean isFuture) {
        return new DescribeLogDirsResponseData.DescribeLogDirsResult()
                .setErrorCode(Errors.NONE.code())
                .setLogDir(logDir)
                .setTopics(prepareDescribeLogDirsTopics(partitionSize, offsetLag, tpr.topic(), tpr.partition(), isFuture));
    }

    protected MockClient.RequestMatcher expectCreatePartitionsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof CreatePartitionsRequest) {
                CreatePartitionsRequest request = (CreatePartitionsRequest) body;
                for (String topic : topics) {
                    if (request.data().topics().find(topic) == null)
                        return false;
                }
                return topics.length == request.data().topics().size();
            }
            return false;
        };
    }

    protected static DescribeClusterResponse prepareDescribeClusterResponse(
        int throttleTimeMs,
        Collection<Node> brokers,
        String clusterId,
        int controllerId,
        int clusterAuthorizedOperations,
        boolean sentToController
    ) {
        DescribeClusterResponseData data = new DescribeClusterResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(throttleTimeMs)
            .setControllerId(controllerId)
            .setClusterId(clusterId)
            .setClusterAuthorizedOperations(clusterAuthorizedOperations);

        if (sentToController) {
            data.setEndpointType(EndpointType.CONTROLLER.id());
        }

        brokers.forEach(broker ->
            data.brokers().add(new DescribeClusterBroker()
                .setHost(broker.host())
                .setPort(broker.port())
                .setBrokerId(broker.id())
                .setRack(broker.rack())));

        return new DescribeClusterResponse(data);
    }

    protected MockClient.RequestMatcher expectListGroupsRequestWithFilters(
        Set<String> expectedStates,
        Set<String> expectedTypes
    ) {
        return body -> {
            if (body instanceof ListGroupsRequest) {
                ListGroupsRequest request = (ListGroupsRequest) body;
                return Objects.equals(new HashSet<>(request.data().statesFilter()), expectedStates)
                    && Objects.equals(new HashSet<>(request.data().typesFilter()), expectedTypes);
            }
            return false;
        };
    }

    protected void verifyListConsumerGroupOffsetsOptions() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final List<TopicPartition> partitions = Collections.singletonList(new TopicPartition("A", 0));
            final ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions()
                    .requireStable(true)
                    .timeoutMs(300);

            final ListConsumerGroupOffsetsSpec groupSpec = new ListConsumerGroupOffsetsSpec()
                    .topicPartitions(partitions);
            env.adminClient().listConsumerGroupOffsets(Collections.singletonMap(GROUP_ID, groupSpec), options);

            final MockClient mockClient = env.kafkaClient();
            waitForRequest(mockClient, ApiKeys.OFFSET_FETCH);

            ClientRequest clientRequest = mockClient.requests().peek();
            assertNotNull(clientRequest);
            assertEquals(300, clientRequest.requestTimeoutMs());
            OffsetFetchRequestData data = ((OffsetFetchRequest.Builder) clientRequest.requestBuilder()).build().data();
            assertTrue(data.requireStable());
            assertEquals(Collections.singletonList(GROUP_ID),
                    data.groups().stream().map(OffsetFetchRequestGroup::groupId).collect(Collectors.toList()));
            assertEquals(Collections.singletonList("A"),
                    data.groups().get(0).topics().stream().map(OffsetFetchRequestTopics::name).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(0),
                    data.groups().get(0).topics().get(0).partitionIndexes());
        }
    }

    protected Map<String, ListConsumerGroupOffsetsSpec> batchedListConsumerGroupOffsetsSpec() {
        Set<TopicPartition> groupAPartitions = Collections.singleton(new TopicPartition("A", 1));
        Set<TopicPartition> groupBPartitions =  Collections.singleton(new TopicPartition("B", 2));

        ListConsumerGroupOffsetsSpec groupASpec = new ListConsumerGroupOffsetsSpec().topicPartitions(groupAPartitions);
        ListConsumerGroupOffsetsSpec groupBSpec = new ListConsumerGroupOffsetsSpec().topicPartitions(groupBPartitions);
        return Map.of("groupA", groupASpec, "groupB", groupBSpec);
    }

    protected Map<String, ListStreamsGroupOffsetsSpec> batchedListStreamsGroupOffsetsSpec() {
        Set<TopicPartition> groupAPartitions = Collections.singleton(new TopicPartition("A", 1));
        Set<TopicPartition> groupBPartitions =  Collections.singleton(new TopicPartition("B", 2));

        ListStreamsGroupOffsetsSpec groupASpec = new ListStreamsGroupOffsetsSpec().topicPartitions(groupAPartitions);
        ListStreamsGroupOffsetsSpec groupBSpec = new ListStreamsGroupOffsetsSpec().topicPartitions(groupBPartitions);
        return Map.of("groupA", groupASpec, "groupB", groupBSpec);
    }

    protected void waitForRequest(MockClient mockClient, ApiKeys apiKeys) throws Exception {
        TestUtils.waitForCondition(() -> {
            ClientRequest clientRequest = mockClient.requests().peek();
            return clientRequest != null && clientRequest.apiKey() == apiKeys;
        }, "Failed awaiting " + apiKeys + " request");
    }

    protected void sendFindCoordinatorResponse(MockClient mockClient, Node coordinator) throws Exception {
        waitForRequest(mockClient, ApiKeys.FIND_COORDINATOR);

        ClientRequest clientRequest = mockClient.requests().peek();
        FindCoordinatorRequestData data = ((FindCoordinatorRequest.Builder) clientRequest.requestBuilder()).data();
        mockClient.respond(prepareFindCoordinatorResponse(Errors.NONE, data.key(), coordinator));
    }

    protected void sendOffsetFetchResponse(MockClient mockClient, Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, boolean batched, Errors error) throws Exception {
        waitForRequest(mockClient, ApiKeys.OFFSET_FETCH);

        ClientRequest clientRequest = mockClient.requests().peek();
        OffsetFetchRequestData data = ((OffsetFetchRequest.Builder) clientRequest.requestBuilder()).build().data();

        if (!batched) {
            assertEquals(1, data.groups().size());
        }

        OffsetFetchResponseData response = new OffsetFetchResponseData()
            .setGroups(data.groups().stream().map(group ->
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId(group.groupId())
                    .setErrorCode(error.code())
                    .setTopics(groupSpecs.get(group.groupId()).topicPartitions().stream()
                        .collect(Collectors.groupingBy(TopicPartition::topic)).entrySet().stream().map(entry ->
                            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                .setName(entry.getKey())
                                .setPartitions(entry.getValue().stream().map(partition ->
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(partition.partition())
                                        .setCommittedOffset(10)
                                ).collect(Collectors.toList()))
                        ).collect(Collectors.toList()))
            ).collect(Collectors.toList()));

        mockClient.respond(new OffsetFetchResponse(response, ApiKeys.OFFSET_FETCH.latestVersion()));
    }

    protected void sendStreamsOffsetFetchResponse(MockClient mockClient, Map<String, ListStreamsGroupOffsetsSpec> groupSpecs, boolean batched, Errors error) throws Exception {
        waitForRequest(mockClient, ApiKeys.OFFSET_FETCH);

        ClientRequest clientRequest = mockClient.requests().peek();
        OffsetFetchRequestData data = ((OffsetFetchRequest.Builder) clientRequest.requestBuilder()).build().data();

        if (!batched) {
            assertEquals(1, data.groups().size());
        }

        OffsetFetchResponseData response = new OffsetFetchResponseData()
            .setGroups(data.groups().stream().map(group ->
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId(group.groupId())
                    .setErrorCode(error.code())
                    .setTopics(groupSpecs.get(group.groupId()).topicPartitions().stream()
                        .collect(Collectors.groupingBy(TopicPartition::topic)).entrySet().stream().map(entry ->
                            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                .setName(entry.getKey())
                                .setPartitions(entry.getValue().stream().map(partition ->
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(partition.partition())
                                        .setCommittedOffset(10)
                                ).collect(Collectors.toList()))
                        ).collect(Collectors.toList()))
            ).collect(Collectors.toList()));

        mockClient.respond(new OffsetFetchResponse(response, ApiKeys.OFFSET_FETCH.latestVersion()));
    }

    protected void verifyListOffsetsForMultipleGroups(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
                                                    ListConsumerGroupOffsetsResult result) throws Exception {
        assertEquals(groupSpecs.size(), result.all().get(10, TimeUnit.SECONDS).size());
        for (Map.Entry<String, ListConsumerGroupOffsetsSpec> entry : groupSpecs.entrySet()) {
            assertEquals(entry.getValue().topicPartitions(),
                    result.partitionsToOffsetAndMetadata(entry.getKey()).get().keySet());
        }
    }

    protected void verifyListStreamsOffsetsForMultipleGroups(Map<String, ListStreamsGroupOffsetsSpec> groupSpecs,
                                                           ListStreamsGroupOffsetsResult result) throws Exception {
        assertEquals(groupSpecs.size(), result.all().get(10, TimeUnit.SECONDS).size());
        for (Map.Entry<String, ListStreamsGroupOffsetsSpec> entry : groupSpecs.entrySet()) {
            assertEquals(entry.getValue().topicPartitions(),
                result.partitionsToOffsetAndMetadata(entry.getKey()).get().keySet());
        }
    }

    protected void testRemoveMembersFromGroup(String reason, String expectedReason) throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster)) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(body -> {
                if (!(body instanceof LeaveGroupRequest)) {
                    return false;
                }
                LeaveGroupRequestData leaveGroupRequest = ((LeaveGroupRequest) body).data();

                return leaveGroupRequest.members().stream().allMatch(
                    member -> member.reason().equals(expectedReason)
                );
            }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                asList(
                    new MemberResponse().setGroupInstanceId("instance-1"),
                    new MemberResponse().setGroupInstanceId("instance-2")
                ))
            ));

            MemberToRemove memberToRemove1 = new MemberToRemove("instance-1");
            MemberToRemove memberToRemove2 = new MemberToRemove("instance-2");

            RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions(asList(
                memberToRemove1,
                memberToRemove2
            ));
            options.reason(reason);

            final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                options
            );

            assertNull(result.all().get());
            assertNull(result.memberResult(memberToRemove1).get());
            assertNull(result.memberResult(memberToRemove2).get());
        }
    }

    protected Map<String, FeatureUpdate> makeTestFeatureUpdates() {
        return Map.of(
            "test_feature_1", new FeatureUpdate((short) 2,  FeatureUpdate.UpgradeType.UPGRADE),
            "test_feature_2", new FeatureUpdate((short) 3,  FeatureUpdate.UpgradeType.SAFE_DOWNGRADE));
    }

    protected void testUpdateFeatures(Map<String, FeatureUpdate> featureUpdates,
                                    ApiError topLevelError,
                                    Set<String> updates) throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                body -> body instanceof UpdateFeaturesRequest,
                UpdateFeaturesResponse.createWithErrors(topLevelError, updates, 0));
            final Map<String, KafkaFuture<Void>> futures = env.adminClient().updateFeatures(
                featureUpdates,
                new UpdateFeaturesOptions().timeoutMs(10000)).values();
            for (final Map.Entry<String, KafkaFuture<Void>> entry : futures.entrySet()) {
                final KafkaFuture<Void> future = entry.getValue();
                if (topLevelError.error() == Errors.NONE) {
                    // Since the top level error was NONE, each future should be successful.
                    future.get();
                } else {
                    final ExecutionException e = assertThrows(ExecutionException.class, future::get);
                    assertEquals(e.getCause().getClass(), topLevelError.exception().getClass());
                    assertEquals(e.getCause().getMessage(), topLevelError.exception().getMessage());
                }
            }
        }
    }

    protected static Stream<Arguments> listOffsetsMetadataNonRetriableErrors() {
        return Stream.of(
                Arguments.of(
                        Errors.TOPIC_AUTHORIZATION_FAILED,
                        Errors.TOPIC_AUTHORIZATION_FAILED,
                        TopicAuthorizationException.class
                ),
                Arguments.of(
                        // We fail fast when the entire topic is unknown...
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.NONE,
                        UnknownTopicOrPartitionException.class
                ),
                Arguments.of(
                        // ... even if a partition in the topic is also somehow reported as unknown...
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        UnknownTopicOrPartitionException.class
                ),
                Arguments.of(
                        // ... or a partition in the topic has a different, otherwise-retriable error
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.LEADER_NOT_AVAILABLE,
                        UnknownTopicOrPartitionException.class
                )
        );
    }

    protected void testApiTimeout(int requestTimeoutMs,
                                int defaultApiTimeoutMs,
                                OptionalInt overrideApiTimeoutMs) throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        Node node0 = new Node(0, "localhost", 8121);
        nodes.put(0, node0);
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                singletonList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        final int retryBackoffMs = 100;
        final int effectiveTimeoutMs = overrideApiTimeoutMs.orElse(defaultApiTimeoutMs);
        assertEquals(2 * requestTimeoutMs, effectiveTimeoutMs,
            "This test expects the effective timeout to be twice the request timeout");

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeoutMs))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ListTopicsOptions options = new ListTopicsOptions();
            overrideApiTimeoutMs.ifPresent(options::timeoutMs);

            final ListTopicsResult result = env.adminClient().listTopics(options);

            // Wait until the first attempt has been sent, then advance the time
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            // Wait for the request to be timed out before backing off
            TestUtils.waitForCondition(() -> !env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for inFlightRequests to be timed out");

            // Since api timeout bound is not hit, AdminClient should retry
            TestUtils.waitForCondition(() -> {
                boolean hasInflightRequests = env.kafkaClient().hasInFlightRequests();
                if (!hasInflightRequests)
                    time.sleep(retryBackoffMs);
                return hasInflightRequests;
            }, "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            TestUtils.assertFutureThrows(TimeoutException.class, result.future);
        }
    }

    protected ClientQuotaEntity newClientQuotaEntity(String... args) {
        assertEquals(0, args.length % 2);

        Map<String, String> entityMap = new HashMap<>(args.length / 2);
        for (int index = 0; index < args.length; index += 2) {
            entityMap.put(args[index], args[index + 1]);
        }
        return new ClientQuotaEntity(entityMap);
    }

    protected void createAlterLogDirsResponse(AdminClientUnitTestEnv env, Node node, Errors error, int... partitions) {
        env.kafkaClient().prepareResponseFrom(
            prepareAlterLogDirsResponse(error, "topic", partitions), node);
    }

    protected AlterReplicaLogDirsResponse prepareAlterLogDirsResponse(Errors error, String topic, int... partitions) {
        return new AlterReplicaLogDirsResponse(
            new AlterReplicaLogDirsResponseData().setResults(singletonList(
                new AlterReplicaLogDirTopicResult()
                    .setTopicName(topic)
                    .setPartitions(Arrays.stream(partitions).boxed().map(partitionId ->
                        new AlterReplicaLogDirPartitionResult()
                            .setPartitionIndex(partitionId)
                            .setErrorCode(error.code())).collect(Collectors.toList())))));
    }

    protected WriteTxnMarkersResponse writeTxnMarkersResponse(
        AbortTransactionSpec abortSpec,
        Errors error
    ) {
        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                .setPartitionIndex(abortSpec.topicPartition().partition())
                .setErrorCode(error.code());

        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                .setName(abortSpec.topicPartition().topic());
        topicResponse.partitions().add(partitionResponse);

        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                .setProducerId(abortSpec.producerId());
        markerResponse.topics().add(topicResponse);

        WriteTxnMarkersResponseData response = new WriteTxnMarkersResponseData();
        response.markers().add(markerResponse);

        return new WriteTxnMarkersResponse(response);
    }

    protected DescribeProducersResponse buildDescribeProducersResponse(
        TopicPartition topicPartition,
        List<ProducerState> producerStates
    ) {
        DescribeProducersResponseData response = new DescribeProducersResponseData();

        DescribeProducersResponseData.TopicResponse topicResponse =
            new DescribeProducersResponseData.TopicResponse()
                .setName(topicPartition.topic());
        response.topics().add(topicResponse);

        DescribeProducersResponseData.PartitionResponse partitionResponse =
            new DescribeProducersResponseData.PartitionResponse()
                .setPartitionIndex(topicPartition.partition())
                .setErrorCode(Errors.NONE.code());
        topicResponse.partitions().add(partitionResponse);

        partitionResponse.setActiveProducers(producerStates.stream().map(producerState ->
            new DescribeProducersResponseData.ProducerState()
                .setProducerId(producerState.producerId())
                .setProducerEpoch(producerState.producerEpoch())
                .setCoordinatorEpoch(producerState.coordinatorEpoch().orElse(-1))
                .setLastSequence(producerState.lastSequence())
                .setLastTimestamp(producerState.lastTimestamp())
                .setCurrentTxnStartOffset(producerState.currentTransactionStartOffset().orElse(-1L))
        ).collect(Collectors.toList()));

        return new DescribeProducersResponse(response);
    }

    protected void expectMetadataRequest(
        AdminClientUnitTestEnv env,
        TopicPartition topicPartition,
        Node leader
    ) {
        MetadataResponseData.MetadataResponseTopicCollection responseTopics =
            new MetadataResponseData.MetadataResponseTopicCollection();

        MetadataResponseTopic responseTopic = new MetadataResponseTopic()
            .setName(topicPartition.topic())
            .setErrorCode(Errors.NONE.code());
        responseTopics.add(responseTopic);

        MetadataResponsePartition responsePartition = new MetadataResponsePartition()
            .setErrorCode(Errors.NONE.code())
            .setPartitionIndex(topicPartition.partition())
            .setLeaderId(leader.id())
            .setReplicaNodes(singletonList(leader.id()))
            .setIsrNodes(singletonList(leader.id()));
        responseTopic.partitions().add(responsePartition);

        env.kafkaClient().prepareResponse(
            request -> {
                if (!(request instanceof MetadataRequest)) {
                    return false;
                }
                MetadataRequest metadataRequest = (MetadataRequest) request;
                return metadataRequest.topics().equals(singletonList(topicPartition.topic()));
            },
            new MetadataResponse(new MetadataResponseData().setTopics(responseTopics),
                MetadataResponseData.HIGHEST_SUPPORTED_VERSION)
        );
    }

    protected UnregisterBrokerResponse prepareUnregisterBrokerResponse(Errors error, int throttleTimeMs) {
        return new UnregisterBrokerResponse(new UnregisterBrokerResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(error.message())
                .setThrottleTimeMs(throttleTimeMs));
    }

    protected DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir) {
        return new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
            .setResults(Collections.singletonList(
                new DescribeLogDirsResponseData.DescribeLogDirsResult()
                    .setErrorCode(error.code())
                    .setLogDir(logDir))));
    }

    protected static OffsetFetchResponse offsetFetchResponse(Errors error) {
        return new OffsetFetchResponse(
            new OffsetFetchResponseData()
                .setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(GROUP_ID)
                        .setErrorCode(error.code())
                )),
            ApiKeys.OFFSET_FETCH.latestVersion()
        );
    }

    protected static MemberDescription convertToMemberDescriptions(DescribedGroupMember member,
                                                                 MemberAssignment assignment) {
        return new MemberDescription(member.memberId(),
                                     Optional.ofNullable(member.groupInstanceId()),
                                     Optional.empty(),
                                     member.clientId(),
                                     member.clientHost(),
                                     assignment,
                                     Optional.empty(),
                                     Optional.empty(),
                                     Optional.empty());
    }

    protected static ShareMemberDescription convertToShareMemberDescriptions(ShareGroupDescribeResponseData.Member member,
                                                                           ShareMemberAssignment assignment) {
        return new ShareMemberDescription(member.memberId(),
                                          Optional.ofNullable(member.rackId()),
                                          member.clientId(),
                                          member.clientHost(),
                                          assignment,
                                          member.memberEpoch());
    }

    protected static ListConfigResourcesResponse prepareListClientMetricsResourcesResponse(Errors error) {
        return new ListConfigResourcesResponse(new ListConfigResourcesResponseData()
                .setErrorCode(error.code()));
    }

    @SafeVarargs
    protected static <T> void assertCollectionIs(Collection<T> collection, T... elements) {
        for (T element : elements) {
            assertTrue(collection.contains(element), "Did not find " + element);
        }
        assertEquals(elements.length, collection.size(), "There are unexpected extra elements in the collection.");
    }

    protected static StreamsGroupDescribeResponseData makeFullStreamsGroupDescribeResponse() {
        StreamsGroupDescribeResponseData data;
        StreamsGroupDescribeResponseData.TaskIds activeTasks1 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(0, 1, 2));
        StreamsGroupDescribeResponseData.TaskIds standbyTasks1 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(3, 4, 5));
        StreamsGroupDescribeResponseData.TaskIds warmupTasks1 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(6, 7, 8));
        StreamsGroupDescribeResponseData.TaskIds activeTasks2 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(3, 4, 5));
        StreamsGroupDescribeResponseData.TaskIds standbyTasks2 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(6, 7, 8));
        StreamsGroupDescribeResponseData.TaskIds warmupTasks2 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(0, 1, 2));
        StreamsGroupDescribeResponseData.Assignment memberAssignment = new StreamsGroupDescribeResponseData.Assignment()
            .setActiveTasks(singletonList(activeTasks1))
            .setStandbyTasks(singletonList(standbyTasks1))
            .setWarmupTasks(singletonList(warmupTasks1));
        StreamsGroupDescribeResponseData.Assignment targetAssignment = new StreamsGroupDescribeResponseData.Assignment()
            .setActiveTasks(singletonList(activeTasks2))
            .setStandbyTasks(singletonList(standbyTasks2))
            .setWarmupTasks(singletonList(warmupTasks2));
        StreamsGroupDescribeResponseData.Member memberOne = new StreamsGroupDescribeResponseData.Member()
            .setMemberId("0")
            .setMemberEpoch(1)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setClientId("clientId0")
            .setClientHost("clientHost")
            .setTopologyEpoch(0)
            .setProcessId("processId")
            .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint()
                .setHost("localhost")
                .setPort(8080)
            )
            .setClientTags(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue()
                .setKey("key")
                .setValue("value")
            ))
            .setTaskOffsets(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskOffset()
                .setSubtopologyId("my_subtopology")
                .setPartition(0)
                .setOffset(0)
            ))
            .setTaskEndOffsets(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskOffset()
                .setSubtopologyId("my_subtopology")
                .setPartition(0)
                .setOffset(1)
            ))
            .setAssignment(memberAssignment)
            .setTargetAssignment(targetAssignment)
            .setIsClassic(true);

        StreamsGroupDescribeResponseData.Member memberTwo = new StreamsGroupDescribeResponseData.Member()
            .setMemberId("1")
            .setMemberEpoch(2)
            .setInstanceId(null)
            .setRackId(null)
            .setClientId("clientId1")
            .setClientHost("clientHost")
            .setTopologyEpoch(1)
            .setProcessId("processId2")
            .setUserEndpoint(null)
            .setClientTags(Collections.emptyList())
            .setTaskOffsets(Collections.emptyList())
            .setTaskEndOffsets(Collections.emptyList())
            .setAssignment(new StreamsGroupDescribeResponseData.Assignment())
            .setTargetAssignment(new StreamsGroupDescribeResponseData.Assignment())
            .setIsClassic(false);

        StreamsGroupDescribeResponseData.Subtopology subtopologyDescription = new StreamsGroupDescribeResponseData.Subtopology()
            .setSubtopologyId("my_subtopology")
            .setSourceTopics(Collections.singletonList("my_source_topic"))
            .setRepartitionSinkTopics(Collections.singletonList("my_repartition_sink_topic"))
            .setStateChangelogTopics(Collections.singletonList(
                new StreamsGroupDescribeResponseData.TopicInfo()
                    .setName("my_changelog_topic")
                    .setPartitions(0)
                    .setReplicationFactor((short) 3)
                    .setTopicConfigs(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue()
                        .setKey("key1")
                        .setValue("value1")
                    ))
            ))
            .setRepartitionSourceTopics(Collections.singletonList(
                new StreamsGroupDescribeResponseData.TopicInfo()
                    .setName("my_repartition_topic")
                    .setPartitions(99)
                    .setReplicationFactor((short) 0)
                    .setTopicConfigs(Collections.emptyList())
            ));

        data = new StreamsGroupDescribeResponseData();
        data.groups().add(new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(GROUP_ID)
            .setGroupState(GroupState.STABLE.toString())
            .setMembers(asList(memberOne, memberTwo))
            .setTopology(new StreamsGroupDescribeResponseData.Topology()
                .setEpoch(1)
                .setSubtopologies(Collections.singletonList(subtopologyDescription))
            )
            .setGroupEpoch(2)
            .setAssignmentEpoch(1));
        return data;
    }
}

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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared scaffolding for KafkaAdminClient unit tests. The test methods themselves live in
 * {@link KafkaAdminClientTest} and the per-domain subclasses (topic, consumer-group, streams-group,
 * share-group, config, transaction). Helpers and constants are placed here so they're available
 * to every subclass without duplication.
 */
@Timeout(120)
public abstract class KafkaAdminClientTestBase {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected static final String GROUP_ID = "group-0";

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

    protected static Cluster mockCluster(int numNodes, int controllerIndex) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; i++)
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        return new Cluster("mockClusterId", nodes.values(),
            Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), nodes.get(controllerIndex));
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

    protected void sendFindCoordinatorResponse(MockClient mockClient, Node coordinator) throws Exception {
        waitForRequest(mockClient, ApiKeys.FIND_COORDINATOR);

        ClientRequest clientRequest = mockClient.requests().peek();
        FindCoordinatorRequestData data = ((FindCoordinatorRequest.Builder) clientRequest.requestBuilder()).data();
        mockClient.respond(prepareFindCoordinatorResponse(Errors.NONE, data.key(), coordinator));
    }

    protected void waitForRequest(MockClient mockClient, ApiKeys apiKeys) throws Exception {
        TestUtils.waitForCondition(() -> {
            ClientRequest clientRequest = mockClient.requests().peek();
            return clientRequest != null && clientRequest.apiKey() == apiKeys;
        }, "Failed awaiting " + apiKeys + " request");
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
}

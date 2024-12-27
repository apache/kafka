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

package kafka.server.handlers;

import kafka.network.RequestChannel;
import kafka.server.AuthHelper;
import kafka.server.KafkaConfig;
import kafka.server.metadata.KRaftMetadataCache;
import kafka.utils.TestUtils;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.network.metrics.RequestChannelMetrics;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.KRaftConfigs;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DescribeTopicPartitionsRequestHandlerTest {
    private final RequestChannelMetrics requestChannelMetrics = mock(RequestChannelMetrics.class);
    private final KafkaPrincipalSerde kafkaPrincipalSerde = new KafkaPrincipalSerde() {
        @Override
        public byte[] serialize(KafkaPrincipal principal) throws SerializationException {
            return Utils.utf8(principal.toString());
        }

        @Override
        public KafkaPrincipal deserialize(byte[] bytes) throws SerializationException {
            return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
        }
    };

    ListenerName plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
    UpdateMetadataBroker broker = new UpdateMetadataBroker()
        .setId(0)
        .setRack("rack")
        .setEndpoints(Arrays.asList(
            new UpdateMetadataRequestData.UpdateMetadataEndpoint()
                .setHost("broker0")
                .setPort(9092)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                .setListener(plaintextListener.value())
        ));

    @Test
    void testDescribeTopicPartitionsRequest() {
        // 1. Set up authorizer
        Authorizer authorizer = mock(Authorizer.class);
        String unauthorizedTopic = "unauthorized-topic";
        String authorizedTopic = "authorized-topic";
        String authorizedNonExistTopic = "authorized-non-exist";

        Action expectedActions1 = new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, unauthorizedTopic, PatternType.LITERAL), 1, true, true);
        Action expectedActions2 = new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL), 1, true, true);
        Action expectedActions3 = new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedNonExistTopic, PatternType.LITERAL), 1, true, true);

        when(authorizer.authorize(any(RequestContext.class), argThat(t ->
            t.contains(expectedActions1) || t.contains(expectedActions2) || t.contains(expectedActions3))))
            .thenAnswer(invocation -> {
                List<Action> actions = invocation.getArgument(1);
                return actions.stream().map(action -> {
                    if (action.resourcePattern().name().startsWith("authorized"))
                        return AuthorizationResult.ALLOWED;
                    else
                        return AuthorizationResult.DENIED;
                }).collect(Collectors.toList());
            });

        // 2. Set up MetadataCache
        Uuid authorizedTopicId = Uuid.randomUuid();
        Uuid unauthorizedTopicId = Uuid.randomUuid();

        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put(authorizedTopic, authorizedTopicId);
        topicIds.put(unauthorizedTopic, unauthorizedTopicId);

        BrokerEndpointCollection collection = new BrokerEndpointCollection();
        collection.add(new BrokerEndpoint()
            .setName(broker.endpoints().get(0).listener())
            .setHost(broker.endpoints().get(0).host())
            .setPort(broker.endpoints().get(0).port())
            .setSecurityProtocol(broker.endpoints().get(0).securityProtocol())
        );
        List<ApiMessage> records = Arrays.asList(
            new RegisterBrokerRecord()
                .setBrokerId(broker.id())
                .setBrokerEpoch(0)
                .setIncarnationId(Uuid.randomUuid())
                .setEndPoints(collection)
                .setRack(broker.rack())
                .setFenced(false),
            new TopicRecord().setName(authorizedTopic).setTopicId(topicIds.get(authorizedTopic)),
            new TopicRecord().setName(unauthorizedTopic).setTopicId(topicIds.get(unauthorizedTopic)),
            new PartitionRecord()
                .setTopicId(authorizedTopicId)
                .setPartitionId(1)
                .setReplicas(Arrays.asList(0, 1, 2))
                .setLeader(0)
                .setIsr(Arrays.asList(0))
                .setEligibleLeaderReplicas(Arrays.asList(1))
                .setLastKnownElr(Arrays.asList(2))
                .setLeaderEpoch(0)
                .setPartitionEpoch(1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
            new PartitionRecord()
                .setTopicId(authorizedTopicId)
                .setPartitionId(0)
                .setReplicas(Arrays.asList(0, 1, 2))
                .setLeader(0)
                .setIsr(Arrays.asList(0))
                .setEligibleLeaderReplicas(Arrays.asList(1))
                .setLastKnownElr(Arrays.asList(2))
                .setLeaderEpoch(0)
                .setPartitionEpoch(1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
            new PartitionRecord()
                .setTopicId(unauthorizedTopicId)
                .setPartitionId(0)
                .setReplicas(Arrays.asList(0, 1, 3))
                .setLeader(0)
                .setIsr(Arrays.asList(0))
                .setEligibleLeaderReplicas(Arrays.asList(1))
                .setLastKnownElr(Arrays.asList(3))
                .setLeaderEpoch(0)
                .setPartitionEpoch(2)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
        );
        KRaftMetadataCache metadataCache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1);
        updateKraftMetadataCache(metadataCache, records);
        DescribeTopicPartitionsRequestHandler handler =
            new DescribeTopicPartitionsRequestHandler(metadataCache, new AuthHelper(scala.Option.apply(authorizer)), createKafkaDefaultConfig());

        // 3.1 Basic test
        DescribeTopicPartitionsRequest describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(
            new DescribeTopicPartitionsRequestData()
                .setTopics(Arrays.asList(
                    new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic),
                    new DescribeTopicPartitionsRequestData.TopicRequest().setName(unauthorizedTopic)
                ))
        );
        RequestChannel.Request request;
        try {
            request = buildRequest(describeTopicPartitionsRequest, plaintextListener);
        } catch (Exception e) {
            fail(e.getMessage());
            return;
        }
        DescribeTopicPartitionsResponseData response = handler.handleDescribeTopicPartitionsRequest(request);
        List<DescribeTopicPartitionsResponseTopic> topics = response.topics().valuesList();
        assertEquals(2, topics.size());
        DescribeTopicPartitionsResponseTopic topicToCheck = topics.get(0);
        assertEquals(authorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic, topicToCheck.name());
        assertEquals(2, topicToCheck.partitions().size());

        topicToCheck = topics.get(1);
        assertNotEquals(unauthorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), topicToCheck.errorCode());
        assertEquals(unauthorizedTopic, topicToCheck.name());

        // 3.2 With cursor
        describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(new DescribeTopicPartitionsRequestData()
            .setTopics(Arrays.asList(
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic),
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(unauthorizedTopic)
                ))
            .setCursor(new DescribeTopicPartitionsRequestData.Cursor().setTopicName(authorizedTopic).setPartitionIndex(1))
        );

        try {
            request = buildRequest(describeTopicPartitionsRequest, plaintextListener);
        } catch (Exception e) {
            fail(e.getMessage());
            return;
        }
        response = handler.handleDescribeTopicPartitionsRequest(request);
        topics = response.topics().valuesList();
        assertEquals(2, topics.size());
        topicToCheck = topics.get(0);
        assertEquals(authorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic, topicToCheck.name());
        assertEquals(1, topicToCheck.partitions().size());

        topicToCheck = topics.get(1);
        assertNotEquals(unauthorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), topicToCheck.errorCode());
        assertEquals(unauthorizedTopic, topicToCheck.name());

        // 3.3 Fetch all topics
        describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(new DescribeTopicPartitionsRequestData());
        try {
            request = buildRequest(describeTopicPartitionsRequest, plaintextListener);
        } catch (Exception e) {
            fail(e.getMessage());
            return;
        }
        response = handler.handleDescribeTopicPartitionsRequest(request);
        topics = response.topics().valuesList();
        assertEquals(1, topics.size());
        topicToCheck = topics.get(0);
        assertEquals(authorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic, topicToCheck.name());
        assertEquals(2, topicToCheck.partitions().size());

        // 3.4 Fetch all topics with cursor
        describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(
            new DescribeTopicPartitionsRequestData().setCursor(
                new DescribeTopicPartitionsRequestData.Cursor().setTopicName(authorizedTopic).setPartitionIndex(1)));
        try {
            request = buildRequest(describeTopicPartitionsRequest, plaintextListener);
        } catch (Exception e) {
            fail(e.getMessage());
            return;
        }
        response = handler.handleDescribeTopicPartitionsRequest(request);
        topics = response.topics().valuesList();
        assertEquals(1, topics.size());
        topicToCheck = topics.get(0);
        assertEquals(authorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic, topicToCheck.name());
        assertEquals(1, topicToCheck.partitions().size());

        // 3.5 Fetch all topics with limit
        describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(
                new DescribeTopicPartitionsRequestData().setResponsePartitionLimit(1)
        );
        try {
            request = buildRequest(describeTopicPartitionsRequest, plaintextListener);
        } catch (Exception e) {
            fail(e.getMessage());
            return;
        }
        response = handler.handleDescribeTopicPartitionsRequest(request);
        topics = response.topics().valuesList();
        assertEquals(1, topics.size());
        topicToCheck = topics.get(0);
        assertEquals(authorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic, topicToCheck.name());
        assertEquals(1, topicToCheck.partitions().size());
        assertEquals(authorizedTopic, response.nextCursor().topicName());
        assertEquals(1, response.nextCursor().partitionIndex());
    }

    @Test
    void testDescribeTopicPartitionsRequestWithEdgeCases() {
        // 1. Set up authorizer
        Authorizer authorizer = mock(Authorizer.class);
        String authorizedTopic = "authorized-topic1";
        String authorizedTopic2 = "authorized-topic2";

        Action expectedActions1 = new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL), 1, true, true);
        Action expectedActions2 = new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic2, PatternType.LITERAL), 1, true, true);

        when(authorizer.authorize(any(RequestContext.class), argThat(t ->
            t.contains(expectedActions1) || t.contains(expectedActions2))))
            .thenAnswer(invocation -> {
                List<Action> actions = invocation.getArgument(1);
                return actions.stream().map(action -> {
                    if (action.resourcePattern().name().startsWith("authorized"))
                        return AuthorizationResult.ALLOWED;
                    else
                        return AuthorizationResult.DENIED;
                }).collect(Collectors.toList());
            });

        // 2. Set up MetadataCache
        Uuid authorizedTopicId = Uuid.randomUuid();
        Uuid authorizedTopicId2 = Uuid.randomUuid();

        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put(authorizedTopic, authorizedTopicId);
        topicIds.put(authorizedTopic2, authorizedTopicId2);

        BrokerEndpointCollection collection = new BrokerEndpointCollection();
        collection.add(new BrokerEndpoint()
                .setName(broker.endpoints().get(0).listener())
                .setHost(broker.endpoints().get(0).host())
                .setPort(broker.endpoints().get(0).port())
                .setSecurityProtocol(broker.endpoints().get(0).securityProtocol())
        );
        List<ApiMessage> records = Arrays.asList(
            new RegisterBrokerRecord()
                .setBrokerId(broker.id())
                .setBrokerEpoch(0)
                .setIncarnationId(Uuid.randomUuid())
                .setEndPoints(collection)
                .setRack(broker.rack())
                .setFenced(false),
            new TopicRecord().setName(authorizedTopic).setTopicId(topicIds.get(authorizedTopic)),
            new TopicRecord().setName(authorizedTopic2).setTopicId(topicIds.get(authorizedTopic2)),
            new PartitionRecord()
                .setTopicId(authorizedTopicId)
                .setPartitionId(0)
                .setReplicas(Arrays.asList(0, 1, 2))
                .setLeader(0)
                .setIsr(Arrays.asList(0))
                .setEligibleLeaderReplicas(Arrays.asList(1))
                .setLastKnownElr(Arrays.asList(2))
                .setLeaderEpoch(0)
                .setPartitionEpoch(1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
            new PartitionRecord()
                .setTopicId(authorizedTopicId)
                .setPartitionId(1)
                .setReplicas(Arrays.asList(0, 1, 2))
                .setLeader(0)
                .setIsr(Arrays.asList(0))
                .setEligibleLeaderReplicas(Arrays.asList(1))
                .setLastKnownElr(Arrays.asList(2))
                .setLeaderEpoch(0)
                .setPartitionEpoch(1)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
            new PartitionRecord()
                .setTopicId(authorizedTopicId2)
                .setPartitionId(0)
                .setReplicas(Arrays.asList(0, 1, 3))
                .setLeader(0)
                .setIsr(Arrays.asList(0))
                .setEligibleLeaderReplicas(Arrays.asList(1))
                .setLastKnownElr(Arrays.asList(3))
                .setLeaderEpoch(0)
                .setPartitionEpoch(2)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
        );
        KRaftMetadataCache metadataCache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1);
        updateKraftMetadataCache(metadataCache, records);
        DescribeTopicPartitionsRequestHandler handler =
            new DescribeTopicPartitionsRequestHandler(metadataCache, new AuthHelper(scala.Option.apply(authorizer)), createKafkaDefaultConfig());

        // 3.1 With cursor point to the first one
        DescribeTopicPartitionsRequest describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(new DescribeTopicPartitionsRequestData()
            .setTopics(Arrays.asList(
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic),
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic2)
                ))
            .setCursor(new DescribeTopicPartitionsRequestData.Cursor().setTopicName(authorizedTopic).setPartitionIndex(1))
        );

        RequestChannel.Request request;
        try {
            request = buildRequest(describeTopicPartitionsRequest, plaintextListener);
        } catch (Exception e) {
            fail(e.getMessage());
            return;
        }
        DescribeTopicPartitionsResponseData response = handler.handleDescribeTopicPartitionsRequest(request);
        List<DescribeTopicPartitionsResponseTopic> topics = response.topics().valuesList();
        assertEquals(2, topics.size());
        DescribeTopicPartitionsResponseTopic topicToCheck = topics.get(0);
        assertEquals(authorizedTopicId, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic, topicToCheck.name());
        assertEquals(1, topicToCheck.partitions().size());

        topicToCheck = topics.get(1);
        assertEquals(authorizedTopicId2, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic2, topicToCheck.name());
        assertEquals(1, topicToCheck.partitions().size());

        // 3.2 With cursor point to the second one. The first topic should be ignored.
        describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(new DescribeTopicPartitionsRequestData()
            .setTopics(Arrays.asList(
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic),
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic2)
                ))
            .setCursor(new DescribeTopicPartitionsRequestData.Cursor().setTopicName(authorizedTopic2).setPartitionIndex(0))
        );

        try {
            request = buildRequest(describeTopicPartitionsRequest, plaintextListener);
        } catch (Exception e) {
            fail(e.getMessage());
            return;
        }
        response = handler.handleDescribeTopicPartitionsRequest(request);
        topics = response.topics().valuesList();
        assertEquals(1, topics.size());
        topicToCheck = topics.get(0);
        assertEquals(authorizedTopicId2, topicToCheck.topicId());
        assertEquals(Errors.NONE.code(), topicToCheck.errorCode());
        assertEquals(authorizedTopic2, topicToCheck.name());
        assertEquals(1, topicToCheck.partitions().size());

        // 3.3 With cursor point to a non existing topic. Exception should be thrown if not querying all the topics.
        describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(new DescribeTopicPartitionsRequestData()
            .setTopics(Arrays.asList(
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic),
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic2)
                ))
            .setCursor(new DescribeTopicPartitionsRequestData.Cursor().setTopicName("Non-existing").setPartitionIndex(0))
        );

        try {
            handler.handleDescribeTopicPartitionsRequest(buildRequest(describeTopicPartitionsRequest, plaintextListener));
        } catch (Exception e) {
            assertInstanceOf(InvalidRequestException.class, e, e.getMessage());
        }

        // 3.4 With cursor point to a negative partition id. Exception should be thrown if not querying all the topics.
        describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest(new DescribeTopicPartitionsRequestData()
            .setTopics(Arrays.asList(
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic),
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(authorizedTopic2)
            ))
            .setCursor(new DescribeTopicPartitionsRequestData.Cursor().setTopicName(authorizedTopic).setPartitionIndex(-1))
        );

        try {
            handler.handleDescribeTopicPartitionsRequest(buildRequest(describeTopicPartitionsRequest, plaintextListener));
        } catch (Exception e) {
            assertInstanceOf(InvalidRequestException.class, e, e.getMessage());
        }
    }

    void updateKraftMetadataCache(KRaftMetadataCache kRaftMetadataCache, List<ApiMessage> records) {
        MetadataImage image = kRaftMetadataCache.currentImage();
        MetadataImage partialImage = new MetadataImage(
            new MetadataProvenance(100L, 10, 1000L, true),
            image.features(),
            ClusterImage.EMPTY,
            image.topics(),
            image.configs(),
            image.clientQuotas(),
            image.producerIds(),
            image.acls(),
            image.scram(),
            image.delegationTokens()
        );
        MetadataDelta delta = new MetadataDelta.Builder().setImage(partialImage).build();
        records.stream().forEach(record -> delta.replay(record));
        kRaftMetadataCache.setImage(delta.apply(new MetadataProvenance(100L, 10, 1000L, true)));
    }

    private RequestChannel.Request buildRequest(AbstractRequest request,
                                                ListenerName listenerName
    ) throws UnknownHostException {
        ByteBuffer buffer = request.serializeWithHeader(
            new RequestHeader(request.apiKey(), request.version(), "test-client", 0));

        // read the header from the buffer first so that the body can be read next from the Request constructor
        RequestHeader header = RequestHeader.parse(buffer);
        // DelegationTokens require the context authenticated to be non SecurityProtocol.PLAINTEXT
        // and have a non KafkaPrincipal.ANONYMOUS principal. This test is done before the check
        // for forwarding because after forwarding the context will have a different context.
        // We validate the context authenticated failure case in other integration tests.
        RequestContext context = new RequestContext(header, "1", InetAddress.getLocalHost(), Optional.empty(), new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"),
                listenerName, SecurityProtocol.SSL, ClientInformation.EMPTY, false,
                Optional.of(kafkaPrincipalSerde));
        return new RequestChannel.Request(1, context, 0, MemoryPool.NONE, buffer,
                requestChannelMetrics, scala.Option.apply(null));
    }

    KafkaConfig createKafkaDefaultConfig() {
        int brokerId = 1;
        Properties properties = TestUtils.createBrokerConfig(
            brokerId,
            "",
            true,
            true,
            TestUtils.RandomPort(),
            scala.Option.apply(null),
            scala.Option.apply(null),
            scala.Option.apply(null),
            true,
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            scala.Option.apply(null),
            1,
            false,
            1,
            (short) 1,
            false);
        properties.put(KRaftConfigs.NODE_ID_CONFIG, Integer.toString(brokerId));
        properties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        int voterId = brokerId + 1;
        properties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, voterId + "@localhost:9093");
        properties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL");
        TestUtils.setIbpVersion(properties, MetadataVersion.latestProduction());
        return new KafkaConfig(properties);
    }
}
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

package kafka.test;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ResultOrError;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FeatureMapAndEpoch;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.DELETE;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.protocol.Errors.INVALID_REQUEST;


public class MockController implements Controller {
    private final static NotControllerException NOT_CONTROLLER_EXCEPTION =
        new NotControllerException("This is not the correct controller for this cluster.");

    private final AtomicLong nextTopicId = new AtomicLong(1);

    @Override
    public CompletableFuture<List<AclCreateResult>> createAcls(List<AclBinding> aclBindings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<List<AclDeleteResult>> deleteAcls(List<AclBindingFilter> aclBindingFilters) {
        throw new UnsupportedOperationException();
    }

    public static class Builder {
        private final Map<String, MockTopic> initialTopics = new HashMap<>();

        public Builder newInitialTopic(String name, Uuid id) {
            initialTopics.put(name, new MockTopic(name, id));
            return this;
        }

        public MockController build() {
            return new MockController(initialTopics.values());
        }
    }

    private volatile boolean active = true;

    private MockController(Collection<MockTopic> initialTopics) {
        for (MockTopic topic : initialTopics) {
            topics.put(topic.id, topic);
            topicNameToId.put(topic.name, topic.id);
        }
    }

    @Override
    public CompletableFuture<AlterPartitionResponseData> alterPartition(AlterPartitionRequestData request) {
        throw new UnsupportedOperationException();
    }

    @Override
    synchronized public CompletableFuture<CreateTopicsResponseData>
            createTopics(CreateTopicsRequestData request) {
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        for (CreatableTopic topic : request.topics()) {
            if (topicNameToId.containsKey(topic.name())) {
                response.topics().add(new CreatableTopicResult().
                    setName(topic.name()).
                    setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()));
            } else {
                long topicId = nextTopicId.getAndIncrement();
                Uuid topicUuid = new Uuid(0, topicId);
                topicNameToId.put(topic.name(), topicUuid);
                topics.put(topicUuid, new MockTopic(topic.name(), topicUuid));
                response.topics().add(new CreatableTopicResult().
                    setName(topic.name()).
                    setErrorCode(Errors.NONE.code()).
                    setTopicId(topicUuid));
                // For a better mock, we might want to return configs, replication factor,
                // etc.  Right now, the tests that use MockController don't need these
                // things.
            }
        }
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<Void> unregisterBroker(int brokerId) {
        throw new UnsupportedOperationException();
    }

    static class MockTopic {
        private final String name;
        private final Uuid id;

        MockTopic(String name, Uuid id) {
            this.name = name;
            this.id = id;
        }
    }

    private final Map<String, Uuid> topicNameToId = new HashMap<>();

    private final Map<Uuid, MockTopic> topics = new HashMap<>();

    private final Map<ConfigResource, Map<String, String>> configs = new HashMap<>();

    @Override
    synchronized public CompletableFuture<Map<String, ResultOrError<Uuid>>>
            findTopicIds(long deadlineNs, Collection<String> topicNames) {
        Map<String, ResultOrError<Uuid>> results = new HashMap<>();
        for (String topicName : topicNames) {
            if (!topicNameToId.containsKey(topicName)) {
                results.put(topicName, new ResultOrError<>(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION)));
            } else {
                results.put(topicName, new ResultOrError<>(topicNameToId.get(topicName)));
            }
        }
        return CompletableFuture.completedFuture(results);
    }

    @Override
    synchronized public CompletableFuture<Map<String, Uuid>> findAllTopicIds(long deadlineNs) {
        Map<String, Uuid> results = new HashMap<>();
        for (Entry<Uuid, MockTopic> entry : topics.entrySet()) {
            results.put(entry.getValue().name, entry.getKey());
        }
        return CompletableFuture.completedFuture(results);
    }

    @Override
    synchronized public CompletableFuture<Map<Uuid, ResultOrError<String>>>
            findTopicNames(long deadlineNs, Collection<Uuid> topicIds) {
        Map<Uuid, ResultOrError<String>> results = new HashMap<>();
        for (Uuid topicId : topicIds) {
            MockTopic topic = topics.get(topicId);
            if (topic == null) {
                results.put(topicId, new ResultOrError<>(new ApiError(Errors.UNKNOWN_TOPIC_ID)));
            } else {
                results.put(topicId, new ResultOrError<>(topic.name));
            }
        }
        return CompletableFuture.completedFuture(results);
    }

    @Override
    synchronized public CompletableFuture<Map<Uuid, ApiError>>
            deleteTopics(long deadlineNs, Collection<Uuid> topicIds) {
        if (!active) {
            CompletableFuture<Map<Uuid, ApiError>> future = new CompletableFuture<>();
            future.completeExceptionally(NOT_CONTROLLER_EXCEPTION);
            return future;
        }
        Map<Uuid, ApiError> results = new HashMap<>();
        for (Uuid topicId : topicIds) {
            MockTopic topic = topics.remove(topicId);
            if (topic == null) {
                results.put(topicId, new ApiError(Errors.UNKNOWN_TOPIC_ID));
            } else {
                topicNameToId.remove(topic.name);
                results.put(topicId, ApiError.NONE);
            }
        }
        return CompletableFuture.completedFuture(results);
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ResultOrError<Map<String, String>>>> describeConfigs(Map<ConfigResource, Collection<String>> resources) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ElectLeadersResponseData> electLeaders(ElectLeadersRequestData request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<FeatureMapAndEpoch> finalizedFeatures() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
            Map<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> configChanges,
            boolean validateOnly) {
        Map<ConfigResource, ApiError> results = new HashMap<>();
        for (Entry<ConfigResource, Map<String, Entry<AlterConfigOp.OpType, String>>> entry :
                configChanges.entrySet()) {
            ConfigResource resource = entry.getKey();
            results.put(resource, incrementalAlterResource(resource, entry.getValue(), validateOnly));
        }
        CompletableFuture<Map<ConfigResource, ApiError>> future = new CompletableFuture<>();
        future.complete(results);
        return future;
    }

    private ApiError incrementalAlterResource(ConfigResource resource,
            Map<String, Entry<AlterConfigOp.OpType, String>> ops, boolean validateOnly) {
        for (Entry<String, Entry<AlterConfigOp.OpType, String>> entry : ops.entrySet()) {
            AlterConfigOp.OpType opType = entry.getValue().getKey();
            if (opType != SET && opType != DELETE) {
                return new ApiError(INVALID_REQUEST, "This mock does not " +
                    "support the " + opType + " config operation.");
            }
        }
        if (!validateOnly) {
            for (Entry<String, Entry<AlterConfigOp.OpType, String>> entry : ops.entrySet()) {
                String key = entry.getKey();
                AlterConfigOp.OpType op = entry.getValue().getKey();
                String value = entry.getValue().getValue();
                switch (op) {
                    case SET:
                        configs.computeIfAbsent(resource, __ -> new HashMap<>()).put(key, value);
                        break;
                    case DELETE:
                        configs.getOrDefault(resource, Collections.emptyMap()).remove(key);
                        break;
                }
            }
        }
        return ApiError.NONE;
    }

    @Override
    public CompletableFuture<AlterPartitionReassignmentsResponseData>
            alterPartitionReassignments(AlterPartitionReassignmentsRequestData request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListPartitionReassignmentsResponseData>
            listPartitionReassignments(ListPartitionReassignmentsRequestData request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> legacyAlterConfigs(
            Map<ConfigResource, Map<String, String>> newConfigs, boolean validateOnly) {
        Map<ConfigResource, ApiError> results = new HashMap<>();
        if (!validateOnly) {
            for (Entry<ConfigResource, Map<String, String>> entry : newConfigs.entrySet()) {
                ConfigResource resource = entry.getKey();
                Map<String, String> map = configs.computeIfAbsent(resource, __ -> new HashMap<>());
                map.clear();
                map.putAll(entry.getValue());
            }
        }
        CompletableFuture<Map<ConfigResource, ApiError>> future = new CompletableFuture<>();
        future.complete(results);
        return future;
    }

    @Override
    public CompletableFuture<BrokerHeartbeatReply>
            processBrokerHeartbeat(BrokerHeartbeatRequestData request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<BrokerRegistrationReply>
            registerBroker(BrokerRegistrationRequestData request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> waitForReadyBrokers(int minBrokers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<ClientQuotaEntity, ApiError>>
            alterClientQuotas(Collection<ClientQuotaAlteration> quotaAlterations, boolean validateOnly) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AllocateProducerIdsResponseData> allocateProducerIds(AllocateProducerIdsRequestData request) {
        throw new UnsupportedOperationException();
    }

    @Override
    synchronized public CompletableFuture<List<CreatePartitionsTopicResult>>
            createPartitions(long deadlineNs, List<CreatePartitionsTopic> topicList) {
        if (!active) {
            CompletableFuture<List<CreatePartitionsTopicResult>> future = new CompletableFuture<>();
            future.completeExceptionally(NOT_CONTROLLER_EXCEPTION);
            return future;
        }
        List<CreatePartitionsTopicResult> results = new ArrayList<>();
        for (CreatePartitionsTopic topic : topicList) {
            if (topicNameToId.containsKey(topic.name())) {
                results.add(new CreatePartitionsTopicResult().setName(topic.name()).
                    setErrorCode(Errors.NONE.code()).
                    setErrorMessage(null));
            } else {
                results.add(new CreatePartitionsTopicResult().setName(topic.name()).
                    setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()).
                    setErrorMessage("No such topic as " + topic.name()));
            }
        }
        return CompletableFuture.completedFuture(results);
    }

    @Override
    public CompletableFuture<Long> beginWritingSnapshot() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beginShutdown() {
        this.active = false;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public int curClaimEpoch() {
        return active ? 1 : -1;
    }

    @Override
    public void close() {
        beginShutdown();
    }
}

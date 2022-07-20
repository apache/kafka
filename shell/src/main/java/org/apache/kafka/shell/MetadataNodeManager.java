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

package org.apache.kafka.shell;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.shell.MetadataNode.DirectoryNode;
import org.apache.kafka.shell.MetadataNode.FileNode;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


/**
 * Maintains the in-memory metadata for the metadata tool.
 */
public final class MetadataNodeManager implements AutoCloseable {
    private static final int NO_LEADER_CHANGE = -2;

    private static final Logger log = LoggerFactory.getLogger(MetadataNodeManager.class);

    public static class Data {
        private final DirectoryNode root = new DirectoryNode();
        private String workingDirectory = "/";

        public DirectoryNode root() {
            return root;
        }

        public String workingDirectory() {
            return workingDirectory;
        }

        public void setWorkingDirectory(String workingDirectory) {
            this.workingDirectory = workingDirectory;
        }
    }

    class LogListener implements RaftClient.Listener<ApiMessageAndVersion> {
        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            try {
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    log.debug("handleCommits " + batch.records() + " at offset " + batch.lastOffset());
                    DirectoryNode dir = data.root.mkdirs("metadataQuorum");
                    dir.create("offset").setContents(String.valueOf(batch.lastOffset()));
                    for (ApiMessageAndVersion messageAndVersion : batch.records()) {
                        handleMessage(messageAndVersion.message());
                    }
                    dir.mkdirs("log").create(String.format("%07d", batch.baseOffset())).
                            setContents(StoredRecordBatch.fromRaftBatch(batch));
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
            try {
                DirectoryNode dir = data.root.mkdirs("metadataQuorum");
                dir.rmrf("snapshot");
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    log.trace("handling new snapshot {} batch {}", reader.snapshotId(), batch);
                    for (ApiMessageAndVersion messageAndVersion : batch) {
                        handleMessage(messageAndVersion.message());
                    }
                    dir.mkdirs("snapshot").create("" + batch.baseOffset()).setContents(StoredRecordBatch.fromRaftBatch(batch));
                }
            } finally {
                log.trace("closing snapshot reader for snapshot {}", reader.snapshotId());
                reader.close();
            }
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch leader) {
            appendEvent("handleNewLeader", () -> {
                log.debug("handleNewLeader " + leader);
                DirectoryNode dir = data.root.mkdirs("metadataQuorum");
                dir.create("leader").setContents(leader.toString());
            }, null);
        }

        @Override
        public void beginShutdown() {
            log.debug("Metadata log listener sent beginShutdown");
        }
    }

    private final Data data = new Data();
    private final LogListener logListener = new LogListener();
    private final ObjectMapper objectMapper;
    private final KafkaEventQueue queue;

    public MetadataNodeManager() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new Jdk8Module());
        this.queue = new KafkaEventQueue(Time.SYSTEM,
            new LogContext("[node-manager-event-queue] "), "");
    }

    public void setup(Object source) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        appendEvent("createShellNodes", () -> {
            DirectoryNode directory = data.root().mkdirs("shell");
            directory.create("release").setContents(AppInfoParser.getVersion());
            directory.create("commitId").setContents(AppInfoParser.getCommitId());
            directory.create("source").setContents(source);
            future.complete(null);
        }, future);
        future.get();
    }

    public LogListener logListener() {
        return logListener;
    }

    // VisibleForTesting
    Data getData() {
        return data;
    }

    @Override
    public void close() throws Exception {
        queue.close();
    }

    public void visit(Consumer<Data> consumer) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        appendEvent("visit", () -> {
            consumer.accept(data);
            future.complete(null);
        }, future);
        future.get();
    }

    private void appendEvent(String name, Runnable runnable, CompletableFuture<?> future) {
        queue.append(new EventQueue.Event() {
            @Override
            public void run() throws Exception {
                runnable.run();
            }

            @Override
            public void handleException(Throwable e) {
                log.error("Unexpected error while handling event " + name, e);
                if (future != null) {
                    future.completeExceptionally(e);
                }
            }
        });
    }

    // VisibleForTesting
    void handleMessage(ApiMessage message) {
        try {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            handleCommitImpl(type, message);
        } catch (Exception e) {
            log.error("Error processing record of type " + message.apiKey(), e);
        }
    }

    private void handleCommitImpl(MetadataRecordType type, ApiMessage message)
            throws Exception {
        switch (type) {
            case REGISTER_BROKER_RECORD: {
                DirectoryNode brokersNode = data.root.mkdirs("brokers");
                RegisterBrokerRecord record = (RegisterBrokerRecord) message;
                DirectoryNode brokerNode = brokersNode.
                    mkdirs(Integer.toString(record.brokerId()));
                FileNode registrationNode = brokerNode.create("registration");
                registrationNode.setContents(BrokerRegistration.fromRecord(record));
                brokerNode.create("isFenced").setContents(record.fenced());
                break;
            }
            case UNREGISTER_BROKER_RECORD: {
                UnregisterBrokerRecord record = (UnregisterBrokerRecord) message;
                data.root.rmrf("brokers", Integer.toString(record.brokerId()));
                break;
            }
            case TOPIC_RECORD: {
                TopicRecord record = (TopicRecord) message;
                DirectoryNode topicsDirectory = data.root.mkdirs("topics");
                DirectoryNode topicDirectory = topicsDirectory.mkdirs(record.name());
                topicDirectory.create("id").setContents(record.topicId().toString());
                topicDirectory.create("name").setContents(record.name().toString());
                DirectoryNode topicIdsDirectory = data.root.mkdirs("topicIds");
                topicIdsDirectory.addChild(record.topicId().toString(), topicDirectory);
                break;
            }
            case PARTITION_RECORD: {
                PartitionRecord record = (PartitionRecord) message;
                PartitionRegistration registration = new PartitionRegistration(record);
                DirectoryNode topicDirectory =
                    data.root.mkdirs("topicIds").mkdirs(record.topicId().toString());
                DirectoryNode partitionDirectory =
                    topicDirectory.mkdirs(Integer.toString(record.partitionId()));
                partitionDirectory.create("registration").setContents(registration);
                break;
            }
            case CONFIG_RECORD: {
                ConfigRecord record = (ConfigRecord) message;
                String typeString = "";
                switch (ConfigResource.Type.forId(record.resourceType())) {
                    case BROKER:
                        typeString = "broker";
                        break;
                    case TOPIC:
                        typeString = "topic";
                        break;
                    default:
                        throw new RuntimeException("Error processing CONFIG_RECORD: " +
                            "Can't handle ConfigResource.Type " + record.resourceType());
                }
                DirectoryNode configDirectory = data.root.mkdirs("configs").
                    mkdirs(typeString).mkdirs(record.resourceName());
                if (record.value() == null) {
                    configDirectory.rmrf(record.name());
                } else {
                    configDirectory.create(record.name()).setContents(record.value());
                }
                break;
            }
            case PARTITION_CHANGE_RECORD: {
                PartitionChangeRecord record = (PartitionChangeRecord) message;
                FileNode file = data.root.file("topicIds", record.topicId().toString(),
                    Integer.toString(record.partitionId()), "registration");
                PartitionRegistration registration = (PartitionRegistration) file.contents();
                PartitionRegistration newRegistration = registration.merge(record);
                file.setContents(newRegistration);
                break;
            }
            case ACCESS_CONTROL_ENTRY_RECORD: {
                AccessControlEntryRecord record = (AccessControlEntryRecord) message;
                StandardAcl acl  = StandardAcl.fromRecord(record);
                DirectoryNode aclsById = data.root.mkdirs("acls", "by-id");
                aclsById.create(record.id().toString()).setContents(acl);
                List<String> aclsByTypeDirectory = aclPath(record.resourceType(),
                        record.patternType(),
                        record.resourceName(),
                        record.id());
                DirectoryNode node = data.root;
                for (String directory : aclsByTypeDirectory) {
                    node = node.mkdirs(directory);
                }
                node.create(record.id().toString()).setContents(acl);
                break;
            }
            case FENCE_BROKER_RECORD: {
                FenceBrokerRecord record = (FenceBrokerRecord) message;
                alterBrokerRegistration(record.id(), Optional.of(true), Optional.empty());
                break;
            }
            case UNFENCE_BROKER_RECORD: {
                UnfenceBrokerRecord record = (UnfenceBrokerRecord) message;
                alterBrokerRegistration(record.id(), Optional.of(false), Optional.empty());
                break;
            }
            case REMOVE_TOPIC_RECORD: {
                RemoveTopicRecord record = (RemoveTopicRecord) message;
                DirectoryNode topicsDirectory =
                    data.root.directory("topicIds", record.topicId().toString());
                String name = (String) topicsDirectory.file("name").contents();
                data.root.rmrf("topics", name);
                data.root.rmrf("topicIds", record.topicId().toString());
                break;
            }
            case FEATURE_LEVEL_RECORD: {
                FeatureLevelRecord record = (FeatureLevelRecord) message;
                DirectoryNode featuresDirectory = data.root.mkdirs("features");
                Short level = Short.valueOf(record.featureLevel());
                if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
                    MetadataVersion version = MetadataVersion.fromFeatureLevel(level);
                    featuresDirectory.create(MetadataVersion.FEATURE_NAME).setContents(version);
                } else {
                    featuresDirectory.create(record.name()).setContents(level);
                }
                break;
            }
            case CLIENT_QUOTA_RECORD: {
                ClientQuotaRecord record = (ClientQuotaRecord) message;
                List<String> directories = clientQuotaRecordDirectories(record.entity());
                DirectoryNode node = data.root;
                for (String directory : directories) {
                    node = node.mkdirs(directory);
                }
                if (record.remove())
                    node.rmrf(record.key());
                else
                    node.create(record.key()).setContents(record.value() + "");
                break;
            }
            case PRODUCER_IDS_RECORD: {
                ProducerIdsRecord record = (ProducerIdsRecord) message;
                DirectoryNode producerIds = data.root.mkdirs("producerIds");
                producerIds.create("lastBlockBrokerId").setContents(record.brokerId() + "");
                producerIds.create("lastBlockBrokerEpoch").setContents(record.brokerEpoch() + "");

                producerIds.create("nextBlockStartId").setContents(record.nextProducerId() + "");
                break;
            }
            case BROKER_REGISTRATION_CHANGE_RECORD: {
                BrokerRegistrationChangeRecord record = (BrokerRegistrationChangeRecord) message;
                alterBrokerRegistration(record.brokerId(),
                    BrokerRegistrationFencingChange.fromValue(record.fenced()).
                        flatMap(BrokerRegistrationFencingChange::asBoolean),
                    BrokerRegistrationInControlledShutdownChange.fromValue(record.inControlledShutdown()).
                        flatMap(BrokerRegistrationInControlledShutdownChange::asBoolean));
                break;
            }
            case REMOVE_ACCESS_CONTROL_ENTRY_RECORD: {
                RemoveAccessControlEntryRecord record = (RemoveAccessControlEntryRecord) message;
                DirectoryNode aclsById = data.root.mkdirs("acls", "by-id");
                aclsById.rmrf(record.id().toString());
                StandardAcl acl = (StandardAcl) data.root.directory("acls", "by-id").file(record.id().toString()).contents();
                List<String> aclsByTypeDirectory = aclPath(acl.resourceType(),
                        acl.patternType(),
                        acl.resourceName(),
                        record.id());
                aclsByTypeDirectory.remove(record.id().toString());
                break;
            }
            case NO_OP_RECORD: {
                // Nothing to do
                break;
            }
            default:
                throw new RuntimeException("Unhandled metadata record type");
        }
    }

    private void alterBrokerRegistration(
        int brokerId,
        Optional<Boolean> fencingChange,
        Optional<Boolean> shutdownChange
    ) {
        DirectoryNode brokerDirectory =
                data.root.directory("brokers", String.valueOf(brokerId));
        BrokerRegistration registration = (BrokerRegistration) brokerDirectory.file("registration").contents();
        BrokerRegistration newRegistration = registration.cloneWith(fencingChange, shutdownChange);
        brokerDirectory.file("registration").setContents(newRegistration);
        if (registration.fenced() != newRegistration.fenced()) {
            brokerDirectory.file("isFenced").setContents(newRegistration.fenced());
        }
    }

    static List<String> aclPath(byte resourceType, byte patternType, String resourceName, Uuid id) {
        return aclPath(ResourceType.fromCode(resourceType), PatternType.fromCode(patternType), resourceName, id);
    }

    static List<String> aclPath(ResourceType resourceType, PatternType patternType, String resourceName, Uuid id) {
        List<String> results = new ArrayList<>();
        results.add("acls");
        results.add("by-type");
        if (resourceType.isUnknown()) {
            throw new RuntimeException("Unable to identify a valid ACL resource type for ACL " + id.toString());
        }
        results.add(resourceType.toString().toLowerCase(Locale.ROOT));
        switch (patternType) {
            case LITERAL:
                if (resourceName.equals("*")) {
                    results.add("wildcard");
                } else {
                    results.add("literal");
                    results.add(resourceName);
                }
                break;
            case PREFIXED:
                results.add("prefixed");
                results.add(resourceName);
                break;
            default:
                throw new RuntimeException("Unable to identify a valid ACL pattern type for ACL " + id.toString());
        }
        return results;
    }

    static List<String> clientQuotaRecordDirectories(List<EntityData> entityData) {
        List<String> result = new ArrayList<>();
        result.add("client-quotas");
        TreeMap<String, EntityData> entries = new TreeMap<>();
        entityData.forEach(e -> entries.put(e.entityType(), e));
        for (Map.Entry<String, EntityData> entry : entries.entrySet()) {
            result.add(entry.getKey());
            result.add(entry.getValue().entityName() == null ?
                "<default>" : entry.getValue().entityName());
        }
        return result;
    }
}

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

package org.apache.kafka.tools.metadata;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metalog.MetaLogLeader;
import org.apache.kafka.metalog.MetaLogListener;
import org.apache.kafka.tools.metadata.MetadataNode.DirectoryNode;
import org.apache.kafka.tools.metadata.MetadataNode.FileNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Maintains the in-memory metadata for the metadata tool.
 */
public final class MetadataNodeManager implements AutoCloseable {
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

    class LogListener implements MetaLogListener {
        @Override
        public void handleCommits(long lastOffset, List<ApiMessage> messages) {
            appendEvent("handleCommits", () -> {
                log.error("handleCommits " + messages + " at offset " + lastOffset);
                DirectoryNode dir = data.root.mkdirs("metadataQuorum");
                dir.create("offset").setContents(String.valueOf(lastOffset));
                for (ApiMessage message : messages) {
                    handleCommit(message);
                }
            }, null);
        }

        @Override
        public void handleNewLeader(MetaLogLeader leader) {
            appendEvent("handleNewLeader", () -> {
                log.error("handleNewLeader " + leader);
                DirectoryNode dir = data.root.mkdirs("metadataQuorum");
                dir.create("leader").setContents(leader.toString());
            }, null);
        }

        @Override
        public void handleRenounce(long epoch) {
            // This shouldn't happen because we should never be the leader.
            log.debug("MetaLogListener sent handleRenounce(epoch=" + epoch + ")");
        }

        @Override
        public void beginShutdown() {
            log.debug("MetaLogListener sent beginShutdown");
        }
    }

    private final Data data = new Data();
    private final LogListener logListener = new LogListener();
    private final KafkaEventQueue queue;

    public MetadataNodeManager() {
        this.queue = new KafkaEventQueue(Time.SYSTEM,
            new LogContext("[node-manager-event-queue] "), "");
    }

    public void setup() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        appendEvent("createShellNodes", () -> {
            DirectoryNode directory = data.root().mkdirs("local");
            directory.create("version").setContents(AppInfoParser.getVersion());
            directory.create("commitId").setContents(AppInfoParser.getCommitId());
            future.complete(null);
        }, future);
        future.get();
    }

    public LogListener logListener() {
        return logListener;
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

    private void handleCommit(ApiMessage message) {
        try {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            handleCommitImpl(type, message);
        } catch (Exception e) {
            log.error("Error processing record of type " + message.apiKey(), e);
        }
    }

    private void handleCommitImpl(MetadataRecordType type, ApiMessage message) {
        switch (type) {
            case REGISTER_BROKER_RECORD: {
                DirectoryNode brokersNode = data.root.mkdirs("brokers");
                RegisterBrokerRecord record = (RegisterBrokerRecord) message;
                DirectoryNode brokerNode = brokersNode.
                    mkdirs(Integer.toString(record.brokerId()));
                FileNode registrationNode = brokerNode.create("registration");
                registrationNode.setContents(record.toString());
                brokerNode.create("isFenced").setContents("true");
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
                DirectoryNode topicDirectory =
                    data.root.mkdirs("topicIds").mkdirs(record.topicId().toString());
                DirectoryNode partitionDirectory =
                    topicDirectory.mkdirs(Integer.toString(record.partitionId()));
                partitionDirectory.create("data").setContents(record.toString());
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
//            case ISR_CHANGE_RECORD:
//            case ACCESS_CONTROL_RECORD:
            case FENCE_BROKER_RECORD: {
                FenceBrokerRecord record = (FenceBrokerRecord) message;
                data.root.mkdirs("brokers", Integer.toString(record.id())).
                    create("isFenced").setContents("true");
                break;
            }
            case UNFENCE_BROKER_RECORD: {
                UnfenceBrokerRecord record = (UnfenceBrokerRecord) message;
                data.root.mkdirs("brokers", Integer.toString(record.id())).
                    create("isFenced").setContents("false");
                break;
            }
            case REMOVE_TOPIC_RECORD: {
                RemoveTopicRecord record = (RemoveTopicRecord) message;
                DirectoryNode topicsDirectory =
                    data.root.directory("topicIds", record.topicId().toString());
                String name = topicsDirectory.file("name").contents();
                data.root.rmrf("topics", name);
                data.root.rmrf("topicIds", record.topicId().toString());
                break;
            }
//            case DELEGATION_TOKEN_RECORD:
//            case USER_SCRAM_CREDENTIAL_RECORD:
            case FEATURE_LEVEL_RECORD:
                break;
            default:
                throw new RuntimeException("Unhandled metadata record type");
        }
    }
}

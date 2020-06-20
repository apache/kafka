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
package org.apache.kafka.connect.integration;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.TestSinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A sink connector that is used in Apache Kafka integration tests to verify the behavior of the
 * Connect framework, but that can be used in other integration tests as a simple connector that
 * consumes and counts records. This class provides methods to find task instances
 * which are initiated by the embedded connector, and wait for them to consume a desired number of
 * messages.
 */
public class MonitorableSinkConnector extends TestSinkConnector {

    private static final Logger log = LoggerFactory.getLogger(MonitorableSinkConnector.class);

    private String connectorName;
    private Map<String, String> commonConfigs;
    private ConnectorHandle connectorHandle;

    @Override
    public void start(Map<String, String> props) {
        connectorHandle = RuntimeHandles.get().connectorHandle(props.get("name"));
        connectorName = props.get("name");
        commonConfigs = props;
        log.info("Starting connector {}", props.get("name"));
        connectorHandle.recordConnectorStart();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MonitorableSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>(commonConfigs);
            config.put("connector.name", connectorName);
            config.put("task.id", connectorName + "-" + i);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopped {} connector {}", this.getClass().getSimpleName(), connectorName);
        connectorHandle.recordConnectorStop();
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    public static class MonitorableSinkTask extends SinkTask {

        private String connectorName;
        private String taskId;
        TaskHandle taskHandle;
        Set<TopicPartition> assignments;
        Map<TopicPartition, Long> committedOffsets;
        Map<String, Map<Integer, TopicPartition>> cachedTopicPartitions;

        public MonitorableSinkTask() {
            this.assignments = new HashSet<>();
            this.committedOffsets = new HashMap<>();
            this.cachedTopicPartitions = new HashMap<>();
        }

        @Override
        public String version() {
            return "unknown";
        }

        @Override
        public void start(Map<String, String> props) {
            taskId = props.get("task.id");
            connectorName = props.get("connector.name");
            taskHandle = RuntimeHandles.get().connectorHandle(connectorName).taskHandle(taskId);
            log.debug("Starting task {}", taskId);
            taskHandle.recordTaskStart();
        }

        @Override
        public void open(Collection<TopicPartition> partitions) {
            log.debug("Opening {} partitions", partitions.size());
            assignments.addAll(partitions);
            taskHandle.partitionsAssigned(partitions.size());
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            for (SinkRecord rec : records) {
                taskHandle.record(rec);
                TopicPartition tp = cachedTopicPartitions
                        .computeIfAbsent(rec.topic(), v -> new HashMap<>())
                        .computeIfAbsent(rec.kafkaPartition(), v -> new TopicPartition(rec.topic(), rec.kafkaPartition()));
                committedOffsets.put(tp, committedOffsets.getOrDefault(tp, 0L) + 1);
                log.trace("Task {} obtained record (key='{}' value='{}')", taskId, rec.key(), rec.value());
            }
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            for (TopicPartition tp : assignments) {
                Long recordsSinceLastCommit = committedOffsets.get(tp);
                if (recordsSinceLastCommit == null) {
                    log.warn("preCommit was called with topic-partition {} that is not included "
                            + "in the assignments of this task {}", tp, assignments);
                } else {
                    taskHandle.commit(recordsSinceLastCommit.intValue());
                    log.error("Forwarding to framework request to commit additional {} for {}",
                            recordsSinceLastCommit, tp);
                    taskHandle.commit((int) (long) recordsSinceLastCommit);
                    committedOffsets.put(tp, 0L);
                }
            }
            return offsets;
        }

        @Override
        public void stop() {
            log.info("Stopped {} task {}", this.getClass().getSimpleName(), taskId);
            taskHandle.recordTaskStop();
        }
    }
}

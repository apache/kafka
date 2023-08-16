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
import org.apache.kafka.connect.runtime.SampleSinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A sink connector that is used in Apache Kafka integration tests to verify the behavior of the
 * Connect framework, but that can be used in other integration tests as a simple connector that
 * consumes and counts records. This class provides methods to find task instances
 * which are initiated by the embedded connector, and wait for them to consume a desired number of
 * messages.
 */
public class MonitorableSinkConnector extends SampleSinkConnector {

    private static final Logger log = LoggerFactory.getLogger(MonitorableSinkConnector.class);

    // Boolean valued configuration that determines whether MonitorableSinkConnector::alterOffsets should return true or false
    public static final String ALTER_OFFSETS_RESULT = "alter.offsets.result";

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

    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<TopicPartition, Long> offsets) {
        return Boolean.parseBoolean(connectorConfig.get(ALTER_OFFSETS_RESULT));
    }

    public static class MonitorableSinkTask extends SinkTask {

        private String connectorName;
        private String taskId;
        TaskHandle taskHandle;
        Map<TopicPartition, Integer> committedOffsets;
        Map<String, Map<Integer, TopicPartition>> cachedTopicPartitions;

        public MonitorableSinkTask() {
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
            log.debug("Opening partitions {}", partitions);
            taskHandle.partitionsAssigned(partitions);
        }

        @Override
        public void close(Collection<TopicPartition> partitions) {
            log.debug("Closing partitions {}", partitions);
            taskHandle.partitionsRevoked(partitions);
            partitions.forEach(committedOffsets::remove);
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            for (SinkRecord rec : records) {
                taskHandle.record(rec);
                TopicPartition tp = cachedTopicPartitions
                        .computeIfAbsent(rec.topic(), v -> new HashMap<>())
                        .computeIfAbsent(rec.kafkaPartition(), v -> new TopicPartition(rec.topic(), rec.kafkaPartition()));
                committedOffsets.put(tp, committedOffsets.getOrDefault(tp, 0) + 1);
                log.trace("Task {} obtained record (key='{}' value='{}')", taskId, rec.key(), rec.value());
            }
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            taskHandle.partitionsCommitted(offsets.keySet());
            offsets.forEach((tp, offset) -> {
                int recordsSinceLastCommit = committedOffsets.getOrDefault(tp, 0);
                if (recordsSinceLastCommit != 0) {
                    taskHandle.commit(recordsSinceLastCommit);
                    log.debug("Forwarding to framework request to commit {} records for {}", recordsSinceLastCommit, tp);
                    committedOffsets.put(tp, 0);
                }
            });
            return offsets;
        }

        @Override
        public void stop() {
            log.info("Stopped {} task {}", this.getClass().getSimpleName(), taskId);
            taskHandle.recordTaskStop();
        }
    }
}

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
import java.util.List;
import java.util.Map;

/**
 * A connector to be used in integration tests. This class provides methods to find task instances
 * which are initiated by the embedded connector, and wait for them to consume a desired number of
 * messages.
 */
public class MonitorableSinkConnector extends TestSinkConnector {

    private static final Logger log = LoggerFactory.getLogger(MonitorableSinkConnector.class);

    private String connectorName;

    @Override
    public void start(Map<String, String> props) {
        connectorName = props.get("name");
        log.info("Starting connector {}", props.get("name"));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MonitorableSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("connector.name", connectorName);
            config.put("task.id", connectorName + "-" + i);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    public static class MonitorableSinkTask extends SinkTask {

        private String connectorName;
        private String taskId;
        private TaskHandle taskHandle;

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
        }

        @Override
        public void open(Collection<TopicPartition> partitions) {
            log.debug("Opening {} partitions", partitions.size());
            super.open(partitions);
            taskHandle.partitionsAssigned(partitions.size());
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            for (SinkRecord rec : records) {
                taskHandle.record();
                log.trace("Task {} obtained record (key='{}' value='{}')", taskId, rec.key(), rec.value());
            }
        }

        @Override
        public void stop() {
        }
    }
}

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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.runtime.TestSourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.tools.ThroughputThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A source connector that is used in Apache Kafka integration tests to verify the behavior of
 * the Connect framework, but that can be used in other integration tests as a simple connector
 * that generates records of a fixed structure. The rate of record production can be adjusted
 * through the configs 'throughput' and 'messages.per.poll'
 */
public class MonitorableSourceConnector extends TestSourceConnector {
    private static final Logger log = LoggerFactory.getLogger(MonitorableSourceConnector.class);

    private String connectorName;
    private ConnectorHandle connectorHandle;
    private Map<String, String> commonConfigs;

    @Override
    public void start(Map<String, String> props) {
        connectorHandle = RuntimeHandles.get().connectorHandle(props.get("name"));
        connectorName = connectorHandle.name();
        commonConfigs = props;
        log.info("Started {} connector {}", this.getClass().getSimpleName(), connectorName);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MonitorableSourceTask.class;
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
    }

    @Override
    public ConfigDef config() {
        log.info("Configured {} connector {}", this.getClass().getSimpleName(), connectorName);
        return new ConfigDef();
    }

    public static class MonitorableSourceTask extends SourceTask {
        private String connectorName;
        private String taskId;
        private String topicName;
        private TaskHandle taskHandle;
        private volatile boolean stopped;
        private long startingSeqno;
        private long seqno;
        private long throughput;
        private int batchSize;
        private ThroughputThrottler throttler;

        @Override
        public String version() {
            return "unknown";
        }

        @Override
        public void start(Map<String, String> props) {
            taskId = props.get("task.id");
            connectorName = props.get("connector.name");
            topicName = props.getOrDefault("topic", "sequential-topic");
            throughput = Long.valueOf(props.getOrDefault("throughput", "-1"));
            batchSize = Integer.valueOf(props.getOrDefault("messages.per.poll", "1"));
            taskHandle = RuntimeHandles.get().connectorHandle(connectorName).taskHandle(taskId);
            Map<String, Object> offset = Optional.ofNullable(
                    context.offsetStorageReader().offset(Collections.singletonMap("task.id", taskId)))
                    .orElse(Collections.emptyMap());
            startingSeqno = Optional.ofNullable((Long) offset.get("saved")).orElse(0L);
            log.info("Started {} task {}", this.getClass().getSimpleName(), taskId);
            throttler = new ThroughputThrottler(throughput, System.currentTimeMillis());
        }

        @Override
        public List<SourceRecord> poll() {
            if (!stopped) {
                if (throttler.shouldThrottle(seqno - startingSeqno, System.currentTimeMillis())) {
                    throttler.throttle();
                }
                taskHandle.record(batchSize);
                return LongStream.range(0, batchSize)
                        .mapToObj(i -> new SourceRecord(
                                Collections.singletonMap("task.id", taskId),
                                Collections.singletonMap("saved", ++seqno),
                                topicName,
                                null,
                                Schema.STRING_SCHEMA,
                                "key-" + taskId + "-" + seqno,
                                Schema.STRING_SCHEMA,
                                "value-" + taskId + "-" + seqno))
                        .collect(Collectors.toList());
            }
            return null;
        }

        @Override
        public void commit() {
            log.info("Task {} committing offsets", taskId);
            //TODO: save progress outside the offset topic, potentially in the task handle
        }

        @Override
        public void commitRecord(SourceRecord record) {
            log.trace("Committing record: {}", record);
            taskHandle.commit();
        }

        @Override
        public void stop() {
            log.info("Stopped {} task {}", this.getClass().getSimpleName(), taskId);
            stopped = true;
        }
    }
}

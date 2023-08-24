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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.runtime.SampleSourceConnector;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.server.util.ThroughputThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
public class MonitorableSourceConnector extends SampleSourceConnector {
    private static final Logger log = LoggerFactory.getLogger(MonitorableSourceConnector.class);

    public static final String TOPIC_CONFIG = "topic";
    public static final String MESSAGES_PER_POLL_CONFIG = "messages.per.poll";
    public static final String MAX_MESSAGES_PER_SECOND_CONFIG = "throughput";
    public static final String MAX_MESSAGES_PRODUCED_CONFIG = "max.messages";

    public static final String CUSTOM_EXACTLY_ONCE_SUPPORT_CONFIG = "custom.exactly.once.support";
    public static final String EXACTLY_ONCE_SUPPORTED = "supported";
    public static final String EXACTLY_ONCE_UNSUPPORTED = "unsupported";
    public static final String EXACTLY_ONCE_NULL = "null";
    public static final String EXACTLY_ONCE_FAIL = "fail";

    public static final String CUSTOM_TRANSACTION_BOUNDARIES_CONFIG = "custom.transaction.boundaries";
    public static final String TRANSACTION_BOUNDARIES_SUPPORTED = "supported";
    public static final String TRANSACTION_BOUNDARIES_UNSUPPORTED = "unsupported";
    public static final String TRANSACTION_BOUNDARIES_NULL = "null";
    public static final String TRANSACTION_BOUNDARIES_FAIL = "fail";

    // Boolean valued configuration that determines whether MonitorableSourceConnector::alterOffsets should return true or false
    public static final String ALTER_OFFSETS_RESULT = "alter.offsets.result";

    private String connectorName;
    private ConnectorHandle connectorHandle;
    private Map<String, String> commonConfigs;

    @Override
    public void start(Map<String, String> props) {
        connectorHandle = RuntimeHandles.get().connectorHandle(props.get("name"));
        connectorName = connectorHandle.name();
        commonConfigs = props;
        log.info("Started {} connector {}", this.getClass().getSimpleName(), connectorName);
        connectorHandle.recordConnectorStart();
        if (Boolean.parseBoolean(props.getOrDefault("connector.start.inject.error", "false"))) {
            throw new RuntimeException("Injecting errors during connector start");
        }
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
            config.put("task.id", taskId(connectorName, i));
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopped {} connector {}", this.getClass().getSimpleName(), connectorName);
        connectorHandle.recordConnectorStop();
        if (Boolean.parseBoolean(commonConfigs.getOrDefault("connector.stop.inject.error", "false"))) {
            throw new RuntimeException("Injecting errors during connector stop");
        }
    }

    @Override
    public ConfigDef config() {
        log.info("Configured {} connector {}", this.getClass().getSimpleName(), connectorName);
        return new ConfigDef();
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        String supportLevel = connectorConfig.getOrDefault(CUSTOM_EXACTLY_ONCE_SUPPORT_CONFIG, "null").toLowerCase(Locale.ROOT);
        switch (supportLevel) {
            case EXACTLY_ONCE_SUPPORTED:
                return ExactlyOnceSupport.SUPPORTED;
            case EXACTLY_ONCE_UNSUPPORTED:
                return ExactlyOnceSupport.UNSUPPORTED;
            case EXACTLY_ONCE_FAIL:
                throw new ConnectException("oops");
            default:
            case EXACTLY_ONCE_NULL:
                return null;
        }
    }

    @Override
    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> connectorConfig) {
        String supportLevel = connectorConfig.getOrDefault(CUSTOM_TRANSACTION_BOUNDARIES_CONFIG, TRANSACTION_BOUNDARIES_UNSUPPORTED).toLowerCase(Locale.ROOT);
        switch (supportLevel) {
            case TRANSACTION_BOUNDARIES_SUPPORTED:
                return ConnectorTransactionBoundaries.SUPPORTED;
            case TRANSACTION_BOUNDARIES_FAIL:
                throw new ConnectException("oh no :(");
            case TRANSACTION_BOUNDARIES_NULL:
                return null;
            default:
            case TRANSACTION_BOUNDARIES_UNSUPPORTED:
                return ConnectorTransactionBoundaries.UNSUPPORTED;
        }
    }

    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<Map<String, ?>, Map<String, ?>> offsets) {
        return Boolean.parseBoolean(connectorConfig.get(ALTER_OFFSETS_RESULT));
    }

    public static String taskId(String connectorName, int taskId) {
        return connectorName + "-" + taskId;
    }

    public static class MonitorableSourceTask extends SourceTask {
        private String taskId;
        private String topicName;
        private TaskHandle taskHandle;
        private volatile boolean stopped;
        private long startingSeqno;
        private long seqno;
        private int batchSize;
        private ThroughputThrottler throttler;
        private long maxMessages;

        private long priorTransactionBoundary;
        private long nextTransactionBoundary;

        @Override
        public String version() {
            return "unknown";
        }

        @Override
        public void start(Map<String, String> props) {
            taskId = props.get("task.id");
            String connectorName = props.get("connector.name");
            topicName = props.getOrDefault(TOPIC_CONFIG, "sequential-topic");
            batchSize = Integer.parseInt(props.getOrDefault(MESSAGES_PER_POLL_CONFIG, "1"));
            taskHandle = RuntimeHandles.get().connectorHandle(connectorName).taskHandle(taskId);
            Map<String, Object> offset = Optional.ofNullable(
                    context.offsetStorageReader().offset(sourcePartition(taskId)))
                    .orElse(Collections.emptyMap());
            startingSeqno = Optional.ofNullable((Long) offset.get("saved")).orElse(0L);
            seqno = startingSeqno;
            log.info("Started {} task {} with properties {}", this.getClass().getSimpleName(), taskId, props);
            throttler = new ThroughputThrottler(Long.parseLong(props.getOrDefault(MAX_MESSAGES_PER_SECOND_CONFIG, "-1")), System.currentTimeMillis());
            maxMessages = Long.parseLong(props.getOrDefault(MAX_MESSAGES_PRODUCED_CONFIG, String.valueOf(Long.MAX_VALUE)));
            taskHandle.recordTaskStart();
            priorTransactionBoundary = 0;
            nextTransactionBoundary = 1;
            if (Boolean.parseBoolean(props.getOrDefault("task-" + taskId + ".start.inject.error", "false"))) {
                throw new RuntimeException("Injecting errors during task start");
            }
            calculateNextBoundary();
        }

        @Override
        public List<SourceRecord> poll() {
            if (!stopped) {
                // Don't return any more records since we've already produced the configured maximum number.
                if (seqno >= maxMessages) {
                    return null;
                }
                if (throttler.shouldThrottle(seqno - startingSeqno, System.currentTimeMillis())) {
                    throttler.throttle();
                }
                int currentBatchSize = (int) Math.min(maxMessages - seqno, batchSize);
                taskHandle.record(currentBatchSize);
                log.trace("Returning batch of {} records", currentBatchSize);
                return LongStream.range(0, currentBatchSize)
                        .mapToObj(i -> {
                            seqno++;
                            SourceRecord record = new SourceRecord(
                                    sourcePartition(taskId),
                                    sourceOffset(seqno),
                                    topicName,
                                    null,
                                    Schema.STRING_SCHEMA,
                                    "key-" + taskId + "-" + seqno,
                                    Schema.STRING_SCHEMA,
                                    "value-" + taskId + "-" + seqno,
                                    null,
                                    new ConnectHeaders().addLong("header-" + seqno, seqno));
                            maybeDefineTransactionBoundary(record);
                            return record;
                        })
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
        public void commitRecord(SourceRecord record, RecordMetadata metadata) {
            log.trace("Committing record: {}", record);
            taskHandle.commit();
        }

        @Override
        public void stop() {
            log.info("Stopped {} task {}", this.getClass().getSimpleName(), taskId);
            stopped = true;
            taskHandle.recordTaskStop();
        }

        /**
         * Calculate the next transaction boundary, i.e., the seqno whose corresponding source record should be used to
         * either {@link org.apache.kafka.connect.source.TransactionContext#commitTransaction(SourceRecord) commit}
         * or {@link org.apache.kafka.connect.source.TransactionContext#abortTransaction(SourceRecord) abort} the next transaction.
         * <p>
         * This connector defines transactions whose size correspond to successive elements of the Fibonacci sequence,
         * where transactions with an even number of records are aborted, and those with an odd number of records are committed.
         */
        private void calculateNextBoundary() {
            while (nextTransactionBoundary <= seqno) {
                nextTransactionBoundary += priorTransactionBoundary;
                priorTransactionBoundary = nextTransactionBoundary - priorTransactionBoundary;
            }
        }

        private void maybeDefineTransactionBoundary(SourceRecord record) {
            if (context.transactionContext() == null || seqno != nextTransactionBoundary) {
                return;
            }
            long transactionSize = nextTransactionBoundary - priorTransactionBoundary;

            // If the transaction boundary ends on an even-numbered offset, abort it
            // Otherwise, commit
            boolean abort = nextTransactionBoundary % 2 == 0;
            calculateNextBoundary();
            if (abort) {
                log.info("Aborting transaction of {} records", transactionSize);
                context.transactionContext().abortTransaction(record);
            } else {
                log.info("Committing transaction of {} records", transactionSize);
                context.transactionContext().commitTransaction(record);
            }
        }
    }

    public static Map<String, Object> sourcePartition(String taskId) {
        return Collections.singletonMap("task.id", taskId);
    }

    public static Map<String, Object> sourceOffset(long seqno) {
        return Collections.singletonMap("saved", seqno);
    }
}

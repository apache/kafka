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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Collections.singleton;

/**
 * Write the original consumed record into a dead letter queue. The dead letter queue is a Kafka topic located
 * on the same cluster used by the worker to maintain internal topics. Each connector is typically configured
 * with its own Kafka topic dead letter queue. By default, the topic name is not set, and if the
 * connector config doesn't specify one, this feature is disabled.
 */
public class DeadLetterQueueReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueReporter.class);

    private static final int DLQ_NUM_DESIRED_PARTITIONS = 1;

    public static final String HEADER_PREFIX = "__connect.errors.";
    public static final String ERROR_HEADER_ORIG_TOPIC = HEADER_PREFIX + "topic";
    public static final String ERROR_HEADER_ORIG_PARTITION = HEADER_PREFIX + "partition";
    public static final String ERROR_HEADER_ORIG_OFFSET = HEADER_PREFIX + "offset";
    public static final String ERROR_HEADER_CONNECTOR_NAME = HEADER_PREFIX + "connector.name";
    public static final String ERROR_HEADER_TASK_ID = HEADER_PREFIX + "task.id";
    public static final String ERROR_HEADER_STAGE = HEADER_PREFIX + "stage";
    public static final String ERROR_HEADER_EXECUTING_CLASS = HEADER_PREFIX + "class.name";
    public static final String ERROR_HEADER_EXCEPTION = HEADER_PREFIX + "exception.class.name";
    public static final String ERROR_HEADER_EXCEPTION_MESSAGE = HEADER_PREFIX + "exception.message";
    public static final String ERROR_HEADER_EXCEPTION_STACK_TRACE = HEADER_PREFIX + "exception.stacktrace";

    private final SinkConnectorConfig connConfig;
    private final ConnectorTaskId connectorTaskId;
    private final ErrorHandlingMetrics errorHandlingMetrics;
    private final String dlqTopicName;

    private final KafkaProducer<byte[], byte[]> kafkaProducer;

    public static DeadLetterQueueReporter createAndSetup(Map<String, Object> adminProps,
                                                         ConnectorTaskId id,
                                                         SinkConnectorConfig sinkConfig, Map<String, Object> producerProps,
                                                         ErrorHandlingMetrics errorHandlingMetrics) {
        String topic = sinkConfig.dlqTopicName();

        try (Admin admin = Admin.create(adminProps)) {
            if (!admin.listTopics().names().get().contains(topic)) {
                log.error("Topic {} doesn't exist. Will attempt to create topic.", topic);
                NewTopic schemaTopicRequest = new NewTopic(topic, DLQ_NUM_DESIRED_PARTITIONS, sinkConfig.dlqTopicReplicationFactor());
                admin.createTopics(singleton(schemaTopicRequest)).all().get();
            }
        } catch (InterruptedException e) {
            throw new ConnectException("Could not initialize dead letter queue with topic=" + topic, e);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new ConnectException("Could not initialize dead letter queue with topic=" + topic, e);
            }
        }

        KafkaProducer<byte[], byte[]> dlqProducer = new KafkaProducer<>(producerProps);
        return new DeadLetterQueueReporter(dlqProducer, sinkConfig, id, errorHandlingMetrics);
    }

    /**
     * Initialize the dead letter queue reporter with a {@link KafkaProducer}.
     *
     * @param kafkaProducer a Kafka Producer to produce the original consumed records.
     */
    // Visible for testing
    DeadLetterQueueReporter(KafkaProducer<byte[], byte[]> kafkaProducer, SinkConnectorConfig connConfig,
                            ConnectorTaskId id, ErrorHandlingMetrics errorHandlingMetrics) {
        Objects.requireNonNull(kafkaProducer);
        Objects.requireNonNull(connConfig);
        Objects.requireNonNull(id);
        Objects.requireNonNull(errorHandlingMetrics);

        this.kafkaProducer = kafkaProducer;
        this.connConfig = connConfig;
        this.connectorTaskId = id;
        this.errorHandlingMetrics = errorHandlingMetrics;
        this.dlqTopicName = connConfig.dlqTopicName().trim();
    }

    /**
     * Write the raw records into a Kafka topic and return the producer future.
     *
     * @param context processing context containing the raw record at {@link ProcessingContext#consumerRecord()}.
     * @return the future associated with the writing of this record; never null
     */
    public Future<RecordMetadata> report(ProcessingContext context) {
        if (dlqTopicName.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        errorHandlingMetrics.recordDeadLetterQueueProduceRequest();

        ConsumerRecord<byte[], byte[]> originalMessage = context.consumerRecord();
        if (originalMessage == null) {
            errorHandlingMetrics.recordDeadLetterQueueProduceFailed();
            return CompletableFuture.completedFuture(null);
        }

        ProducerRecord<byte[], byte[]> producerRecord;
        if (originalMessage.timestamp() == RecordBatch.NO_TIMESTAMP) {
            producerRecord = new ProducerRecord<>(dlqTopicName, null,
                    originalMessage.key(), originalMessage.value(), originalMessage.headers());
        } else {
            producerRecord = new ProducerRecord<>(dlqTopicName, null, originalMessage.timestamp(),
                    originalMessage.key(), originalMessage.value(), originalMessage.headers());
        }

        if (connConfig.isDlqContextHeadersEnabled()) {
            populateContextHeaders(producerRecord, context);
        }

        return this.kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Could not produce message to dead letter queue. topic=" + dlqTopicName, exception);
                errorHandlingMetrics.recordDeadLetterQueueProduceFailed();
            }
        });
    }

    // Visible for testing
    void populateContextHeaders(ProducerRecord<byte[], byte[]> producerRecord, ProcessingContext context) {
        Headers headers = producerRecord.headers();
        if (context.consumerRecord() != null) {
            headers.add(ERROR_HEADER_ORIG_TOPIC, toBytes(context.consumerRecord().topic()));
            headers.add(ERROR_HEADER_ORIG_PARTITION, toBytes(context.consumerRecord().partition()));
            headers.add(ERROR_HEADER_ORIG_OFFSET, toBytes(context.consumerRecord().offset()));
        }

        headers.add(ERROR_HEADER_CONNECTOR_NAME, toBytes(connectorTaskId.connector()));
        headers.add(ERROR_HEADER_TASK_ID, toBytes(String.valueOf(connectorTaskId.task())));
        headers.add(ERROR_HEADER_STAGE, toBytes(context.stage().name()));
        headers.add(ERROR_HEADER_EXECUTING_CLASS, toBytes(context.executingClass().getName()));
        if (context.error() != null) {
            headers.add(ERROR_HEADER_EXCEPTION, toBytes(context.error().getClass().getName()));
            headers.add(ERROR_HEADER_EXCEPTION_MESSAGE, toBytes(context.error().getMessage()));
            byte[] trace;
            if ((trace = stacktrace(context.error())) != null) {
                headers.add(ERROR_HEADER_EXCEPTION_STACK_TRACE, trace);
            }
        }
    }

    private byte[] stacktrace(Throwable error) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            PrintStream stream = new PrintStream(bos, true, StandardCharsets.UTF_8.name());
            error.printStackTrace(stream);
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            log.error("Could not serialize stacktrace.", e);
        }
        return null;
    }

    private byte[] toBytes(int value) {
        return toBytes(String.valueOf(value));
    }

    private byte[] toBytes(long value) {
        return toBytes(String.valueOf(value));
    }

    private byte[] toBytes(String value) {
        if (value != null) {
            return value.getBytes(StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}

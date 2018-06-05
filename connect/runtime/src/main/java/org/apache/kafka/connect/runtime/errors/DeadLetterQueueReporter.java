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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singleton;

/**
 * Write the original consumed record into a dead letter queue. The dead letter queue is a Kafka topic located
 * on the same cluster used by the worker to maintain internal topics. Each connector is typically configured
 * with its own Kafka topic dead letter queue. By default, the topic name is not set, and if the
 * connector config doesn't specify one, this feature is disabled.
 */
public class DeadLetterQueueReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueReporter.class);

    private static final short DLQ_MAX_DESIRED_REPLICATION_FACTOR = 3;
    private static final int DLQ_NUM_DESIRED_PARTITIONS = 1;

    public static final String PREFIX = "errors.deadletterqueue";

    public static final String DLQ_TOPIC_NAME = "topic.name";
    public static final String DLQ_TOPIC_NAME_DOC = "The name of the topic where these messages are written to.";
    public static final String DLQ_TOPIC_DEFAULT = "";

    private DeadLetterQueueReporterConfig config;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private ErrorHandlingMetrics errorHandlingMetrics;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DLQ_TOPIC_NAME, ConfigDef.Type.STRING, DLQ_TOPIC_DEFAULT, ConfigDef.Importance.HIGH, DLQ_TOPIC_NAME_DOC);

    public static DeadLetterQueueReporter createAndSetup(WorkerConfig workerConfig,
                                                         ConnectorConfig connConfig, Map<String, Object> producerProps) {
        String topic = connConfig.getString(PREFIX + "." + DLQ_TOPIC_NAME);

        try (AdminClient admin = AdminClient.create(workerConfig.originals())) {
            if (!admin.listTopics().names().get().contains(topic)) {
                log.error("Topic {} doesn't exist. Will attempt to create topic.", topic);
                NewTopic schemaTopicRequest = new NewTopic(topic, DLQ_NUM_DESIRED_PARTITIONS, DLQ_MAX_DESIRED_REPLICATION_FACTOR);
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
        return new DeadLetterQueueReporter(dlqProducer);
    }

    /**
     * Initialize the dead letter queue reporter with a {@link KafkaProducer}.
     *
     * @param kafkaProducer a Kafka Producer to produce the original consumed records.
     */
    // Visible for testing
    DeadLetterQueueReporter(KafkaProducer<byte[], byte[]> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new DeadLetterQueueReporterConfig(configs);
    }

    @Override
    public void metrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    /**
     * Write the raw records into a Kafka topic.
     *
     * @param context processing context containing the raw record at {@link ProcessingContext#consumerRecord()}.
     */
    public void report(ProcessingContext context) {
        if (config.topic().isEmpty()) {
            return;
        }

        errorHandlingMetrics.recordDeadLetterQueueProduceRequest();

        ConsumerRecord<byte[], byte[]> originalMessage = context.consumerRecord();
        if (originalMessage == null) {
            errorHandlingMetrics.recordDeadLetterQueueProduceFailed();
            return;
        }

        ProducerRecord<byte[], byte[]> producerRecord;
        if (originalMessage.timestamp() == RecordBatch.NO_TIMESTAMP) {
            producerRecord = new ProducerRecord<>(config.topic(), null,
                    originalMessage.key(), originalMessage.value(), originalMessage.headers());
        } else {
            producerRecord = new ProducerRecord<>(config.topic(), null, originalMessage.timestamp(),
                    originalMessage.key(), originalMessage.value(), originalMessage.headers());
        }

        this.kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Could not produce message to dead letter queue. topic=" + config.topic(), exception);
                errorHandlingMetrics.recordDeadLetterQueueProduceFailed();
            }
        });
    }

    static class DeadLetterQueueReporterConfig extends AbstractConfig {
        public DeadLetterQueueReporterConfig(Map<?, ?> originals) {
            super(CONFIG_DEF, originals, true);
        }

        /**
         * @return name of the dead letter queue topic.
         */
        public String topic() {
            return getString(DLQ_TOPIC_NAME);
        }
    }
}

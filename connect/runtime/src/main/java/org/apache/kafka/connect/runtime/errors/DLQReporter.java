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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Write the original consumed record into a dead letter queue. The dead letter queue is a Kafka topic located
 * on the same cluster used by the worker to maintain internal topics. Each connector gets their own dead letter
 * queue topic. By default, the topic name is not set, and if the connector config doesn't specify one, this
 * feature is disabled.
 */
public class DLQReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(DLQReporter.class);

    public static final String PREFIX = "errors.deadletterqueue.";

    public static final String DLQ_TOPIC_NAME = "topic.name";
    public static final String DLQ_TOPIC_NAME_DOC = "The name of the topic where these messages are written to.";
    public static final String DLQ_TOPIC_DEFAULT = "";

    private final int numPartitions;

    private DLQReporterConfig config;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private ErrorHandlingMetrics errorHandlingMetrics;

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(DLQ_TOPIC_NAME, ConfigDef.Type.STRING, DLQ_TOPIC_DEFAULT, ConfigDef.Importance.HIGH, DLQ_TOPIC_NAME_DOC);
    }

    /**
     * Initialize the dead letter queue reporter.
     * @param kafkaProducer a Kafka Producer to produce the original consumed records.
     * @param numPartitions the number of partitions in the dead letter queue topic.
     */
    public DLQReporter(KafkaProducer<byte[], byte[]> kafkaProducer, int numPartitions) {
        this.kafkaProducer = kafkaProducer;
        this.numPartitions = numPartitions;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new DLQReporterConfig(configs);
    }

    @Override
    public void setMetrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    /**
     * @param context write the record from {@link ProcessingContext#consumerRecord()} into the dead letter queue.
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

        int partition = ThreadLocalRandom.current().nextInt(numPartitions);

        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(config.topic(),
                partition, originalMessage.key(), originalMessage.value(), originalMessage.headers());

        this.kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Could not produce message to dead letter queue. topic=" + config.topic(), exception);
                errorHandlingMetrics.recordDeadLetterQueueProduceFailed();
            }
        });
    }

    static class DLQReporterConfig extends AbstractConfig {
        public DLQReporterConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals, true);
        }

        /**
         * @return name of the dead letter queue topic.
         */
        public String topic() {
            return getString(DLQ_TOPIC_NAME);
        }
    }
}

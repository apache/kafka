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
package org.apache.kafka.connect.runtime.errors.impl;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class DLQReporter extends ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(DLQReporter.class);

    public static final String DLQ_TOPIC_NAME = "errors.dlq.topic.name";
    public static final String DLQ_TOPIC_NAME_DOC = "The name of the topic where these messages are written to.";

    public static final String DLQ_TOPIC_PARTITIONS = "errors.dlq.topic.partitions";
    public static final String DLQ_TOPIC_PARTITIONS_DOC = "Number of partitions for the DLQ topic.";
    public static final int DLQ_TOPIC_PARTITIONS_DEFAULT = 25;

    public static final String DLQ_TOPIC_REPLICATION_FACTOR = "errors.dlq.topic.replication.factor";
    public static final String DLQ_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the DLQ topic.";
    public static final short DLQ_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    public static final String DLQ_INCLUDE_CONFIGS = "dlq.include.configs";
    public static final String DLQ_INCLUDE_CONFIGS_DOC = "Include the (worker, connector) configs in the log.";
    public static final boolean DLQ_INCLUDE_CONFIGS_DEFAULT = false;

    public static final String DLQ_INCLUDE_MESSAGES = "dlq.include.messages";
    public static final String DLQ_INCLUDE_MESSAGES_DOC = "Include the Connect Record which failed to process in the log.";
    public static final boolean DLQ_INCLUDE_MESSAGES_DEFAULT = false;

    public static final String DLQ_CONVERTER = "dlq.converter";
    public static final String DLQ_CONVERTER_DOC = "Include the Connect Record which failed to process in the log.";
    public static final Class<? extends Converter> DLQ_CONVERTER_DEFAULT = StringConverter.class;

    public static final String DLQ_PRODUCER_PROPERTIES = "dlq.producer";

    private DlqReporterConfig config;
    private KafkaProducer<byte[], byte[]> producer;
    private Converter converter;

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(DLQ_TOPIC_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DLQ_TOPIC_NAME_DOC)
                .define(DLQ_TOPIC_PARTITIONS, ConfigDef.Type.INT, DLQ_TOPIC_PARTITIONS_DEFAULT, atLeast(1), ConfigDef.Importance.HIGH, DLQ_TOPIC_PARTITIONS_DOC)
                .define(DLQ_TOPIC_REPLICATION_FACTOR, ConfigDef.Type.SHORT, DLQ_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), ConfigDef.Importance.HIGH, DLQ_TOPIC_REPLICATION_FACTOR_DOC)
                .define(DLQ_INCLUDE_CONFIGS, ConfigDef.Type.BOOLEAN, DLQ_INCLUDE_CONFIGS_DEFAULT, ConfigDef.Importance.HIGH, DLQ_INCLUDE_CONFIGS_DOC)
                .define(DLQ_INCLUDE_MESSAGES, ConfigDef.Type.BOOLEAN, DLQ_INCLUDE_MESSAGES_DEFAULT, ConfigDef.Importance.HIGH, DLQ_INCLUDE_MESSAGES_DOC)
                .define(DLQ_CONVERTER, ConfigDef.Type.CLASS, DLQ_CONVERTER_DEFAULT, ConfigDef.Importance.HIGH, DLQ_CONVERTER_DOC);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new DlqReporterConfig(configs);
    }

    @Override
    public void initialize() {
        // check if topic exists
        NewTopic topicDescription = TopicAdmin.defineTopic(config.topic())
                .partitions(config.topicPartitions())
                .replicationFactor(config.topicReplicationFactor())
                .build();

        Map<String, Object> adminProps = config.getProducerProps();
        try (TopicAdmin admin = new TopicAdmin(adminProps)) {
            admin.createTopics(topicDescription);
        }

        Map<String, Object> producerProps = config.getProducerProps();
        producer = new KafkaProducer<>(producerProps);

        try {
            if (config.converter().isAssignableFrom(Converter.class)) {
                converter = config.converter().newInstance();
            }
        } catch (InstantiationException e) {
            throw new ConnectException("Could not instantiate converter");
        } catch (IllegalAccessException e) {
            throw new ConnectException("Could not access class");
        }
    }

    @Override
    public void report(final ProcessingContext report) {
        byte[] val;
        try {
            val = converter.fromConnectData(config.topic(), SchemaBuilder.struct().schema(), report);
        } catch (Exception e) {
            log.error("Could not convert report for producing", e);
            return;
        }



        producer.send(new ProducerRecord<byte[], byte[]>(config.topic(), val), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    log.error("Could not write record to DLQ", report);
                }
            }
        });
    }

    private static class DlqReporterConfig extends AbstractConfig {
        public DlqReporterConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals);
        }

        public String topic() {
            return getString(DLQ_TOPIC_NAME);
        }

        public int topicPartitions() {
            return getInt(DLQ_TOPIC_NAME);
        }

        public short topicReplicationFactor() {
            return getShort(DLQ_TOPIC_REPLICATION_FACTOR);
        }

        public boolean includeConfigs() {
            return getBoolean(DLQ_INCLUDE_CONFIGS);
        }

        public boolean includeMessages() {
            return getBoolean(DLQ_INCLUDE_MESSAGES);
        }

        @SuppressWarnings("unchecked")
        public Class<? extends Converter> converter() {
            return (Class<? extends Converter>) getClass(DLQ_CONVERTER);
        }

        public Map<String, Object> getProducerProps() {
            return originalsWithPrefix(DLQ_PRODUCER_PROPERTIES + ".");
        }

    }
}

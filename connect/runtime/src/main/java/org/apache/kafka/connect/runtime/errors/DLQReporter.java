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

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class DLQReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(DLQReporter.class);

    public static final String PREFIX = "errors.deadletterqueue.";

    public static final String DLQ_TOPIC_NAME = "topic.name";
    public static final String DLQ_TOPIC_NAME_DOC = "The name of the topic where these messages are written to.";
    public static final String DLQ_TOPIC_DEFAULT = "";

    private final TopicDescription topicDescription;

    private DLQReporterConfig config;
    private KafkaProducer<byte[], byte[]> kafkaProducer;

    static ConfigDef getConfigDef() {
        return new ConfigDef()
                .define(DLQ_TOPIC_NAME, ConfigDef.Type.STRING, DLQ_TOPIC_DEFAULT, ConfigDef.Importance.HIGH, DLQ_TOPIC_NAME_DOC);
    }

    public DLQReporter(KafkaProducer<byte[], byte[]> kafkaProducer, TopicDescription desc) {
        this.kafkaProducer = kafkaProducer;
        this.topicDescription = desc;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        config = new DLQReporterConfig(configs);
    }

    public void report(ProcessingContext context) {
        ConsumerRecord<byte[], byte[]> originalMessage = context.sinkRecord();
        int partition = ThreadLocalRandom.current().nextInt(topicDescription.partitions().size());
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(config.topic(),
                partition, originalMessage.key(), originalMessage.value());
        this.kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    log.debug("Could not send object to DLQ", exception);
                }
            }
        });
    }

    static class DLQReporterConfig extends AbstractConfig {
        public DLQReporterConfig(Map<?, ?> originals) {
            super(getConfigDef(), originals, true);
        }

        public String topic() {
            return getString(DLQ_TOPIC_NAME);
        }
    }
}

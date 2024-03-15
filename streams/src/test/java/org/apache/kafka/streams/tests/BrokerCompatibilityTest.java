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
package org.apache.kafka.streams.tests;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Grouped;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BrokerCompatibilityTest {

    private static final String SOURCE_TOPIC = "brokerCompatibilitySourceTopic";
    private static final String SINK_TOPIC = "brokerCompatibilitySinkTopic";

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("BrokerCompatibilityTest are expecting two parameters: propFile, processingMode; but only see " + args.length + " parameter");
            Exit.exit(1);
        }

        System.out.println("StreamsTest instance started");

        final String propFileName = args[0];
        final String processingMode = args[1];

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            Exit.exit(1);
        }

        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-system-test-broker-compatibility");
        streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsProperties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingMode);
        final int timeout = 6000;
        streamsProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), timeout);
        streamsProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), timeout);
        streamsProperties.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout + 1);
        final Serde<String> stringSerde = Serdes.String();


        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(SOURCE_TOPIC).groupByKey(Grouped.with(stringSerde, stringSerde))
            .count()
            .toStream()
            .mapValues(Object::toString)
            .to(SINK_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);
        streams.setUncaughtExceptionHandler(e -> {
            Throwable cause = e;
            if (cause instanceof StreamsException) {
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
            }
            System.err.println("FATAL: An unexpected exception " + cause);
            e.printStackTrace(System.err);
            System.err.flush();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        System.out.println("start Kafka Streams");
        streams.start();

        final boolean eosEnabled = processingMode.startsWith("exactly_once");

        System.out.println("send data");
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (eosEnabled) {
            producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "broker-compatibility-producer-tx");
        }

        try {
            try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
                if (eosEnabled) {
                    producer.initTransactions();
                    producer.beginTransaction();
                }
                producer.send(new ProducerRecord<>(SOURCE_TOPIC, "key", "value"));
                if (eosEnabled) {
                    producer.commitTransaction();
                }

                System.out.println("wait for result");
                loopUntilRecordReceived(kafka, eosEnabled);
                System.out.println("close Kafka Streams");
                streams.close();
            }
        } catch (final RuntimeException e) {
            System.err.println("Non-Streams exception occurred: ");
            e.printStackTrace(System.err);
            System.err.flush();
        }
    }

    private static void loopUntilRecordReceived(final String kafka, final boolean eosEnabled) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "broker-compatibility-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        if (eosEnabled) {
            consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString());
        }

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList(SINK_TOPIC));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String, String> record : records) {
                    if (record.key().equals("key") && record.value().equals("1")) {
                        return;
                    }
                }
            }
        }
    }

}

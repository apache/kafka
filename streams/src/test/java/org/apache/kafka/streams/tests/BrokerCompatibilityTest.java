/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.util.Collections;
import java.util.Properties;

public class BrokerCompatibilityTest {

    private static final String SOURCE_TOPIC = "brokerCompatibilitySourceTopic";
    private static final String SINK_TOPIC = "brokerCompatibilitySinkTopic";

    public static void main(String[] args) throws Exception {
        System.out.println("StreamsTest instance started");

        final String kafka = args.length > 0 ? args[0] : "localhost:9092";
        final String stateDirStr = args.length > 1 ? args[1] : TestUtils.tempDirectory().getAbsolutePath();

        final File stateDir = new File(stateDirStr);
        stateDir.mkdir();

        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-system-test-broker-compatibility");
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProperties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);


        final KStreamBuilder builder = new KStreamBuilder();
        builder.stream(SOURCE_TOPIC).to(SINK_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsProperties);
        System.out.println("start Kafka Streams");
        streams.start();


        System.out.println("send data");
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        producer.send(new ProducerRecord<>(SOURCE_TOPIC, "key", "value"));


        System.out.println("wait for result");
        loopUntilRecordReceived(kafka);


        System.out.println("close Kafka Streams");
        streams.close();
    }

    private static void loopUntilRecordReceived(final String kafka) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "broker-compatibility-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(SINK_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                if (record.key().equals("key") && record.value().equals("value")) {
                    consumer.close();
                    return;
                }
            }
        }
    }

}

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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * Utility class used to send messages with integer keys
 * repeating in sequence every 1000 messages.  Multiple topics for publishing
 * can be provided in the config map with key of 'topics' and ';' delimited list of output topics
 */
public class StreamsRepeatingIntegerKeyProducer {

    private static volatile boolean keepProducing = true;

    public static void main(String[] args) {
        System.out.println("StreamsTest instance started");

        final String kafka = args.length > 0 ? args[0] : "localhost:9092";
        final String configString = args.length > 2 ? args[2] : null;

        Map<String, String> configs = SystemTestUtil.parseConfigs(configString);
        System.out.println("Using provided configs " + configs);

        final int numMessages = configs.containsKey("num_messages") ? Integer.parseInt(configs.get("num_messages")) : Integer.MAX_VALUE;

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "StandbyTaskTestsProducer");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        String value = "testingValue";
        Integer key = 0;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                keepProducing = false;
            }
        }));
        final String[] topics = configs.get("topics").split(";");

        int messageCounter = 0;

        try (final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps)) {

            while (keepProducing && messageCounter++ < numMessages) {
                for (String topic : topics) {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key.toString(), value + key);
                    kafkaProducer.send(producerRecord);
                }
                key += 1;
                if (key % 1000 == 0) {
                    try {
                        System.out.println("Sent 1000 messages");
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    key = 0;
                }
            }
        }
        System.out.println("Producer shut down now, sent total [" + (messageCounter - 1) + "] of requested [" + numMessages + "]");
        System.out.flush();
    }

}

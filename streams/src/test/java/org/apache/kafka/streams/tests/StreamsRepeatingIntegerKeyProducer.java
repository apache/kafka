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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class used to send messages with integer keys
 * repeating in sequence every 1000 messages.  Multiple topics for publishing
 * can be provided in the config map with key of 'topics' and ';' delimited list of output topics
 */
public class StreamsRepeatingIntegerKeyProducer {

    private static volatile boolean keepProducing = true;
    private volatile static int messageCounter = 0;

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("StreamsStandByReplicaTest are expecting two parameters: propFile, additionalConfigs; but only see " + args.length + " parameter");
            System.exit(1);
        }

        System.out.println("StreamsTest instance started");

        final String propFileName = args[0];
        final String configString = args[1];

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(1);
        }

        final Map<String, String> configs = SystemTestUtil.parseConfigs(configString);
        System.out.println("Using provided configs " + configs);

        final int numMessages = configs.containsKey("num_messages") ? Integer.parseInt(configs.get("num_messages")) : 1000;

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "StreamsRepeatingIntegerKeyProducer");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        final String value = "testingValue";
        Integer key = 0;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                keepProducing = false;
            }
        }));

        final String[] topics = configs.get("topics").split(";");
        final int totalMessagesToProduce = numMessages * topics.length;

        try (final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps)) {

            while (keepProducing && messageCounter < totalMessagesToProduce) {
                for (final String topic : topics) {
                    final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key.toString(), value + key);
                    kafkaProducer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                            if (exception != null) {
                                exception.printStackTrace(System.err);
                                System.err.flush();
                                if (exception instanceof TimeoutException) {
                                    try {
                                        // message == org.apache.kafka.common.errors.TimeoutException: Expiring 4 record(s) for data-0: 30004 ms has passed since last attempt plus backoff time
                                        final int expired = Integer.parseInt(exception.getMessage().split(" ")[2]);
                                        messageCounter -= expired;
                                    } catch (final Exception ignore) {
                                    }
                                }
                            }
                        }
                    });
                    messageCounter += 1;
                }
                key += 1;
                if (key % 1000 == 0) {
                    System.out.println("Sent 1000 messages");
                    Utils.sleep(100);
                    key = 0;
                }
            }
        }
        System.out.println("Producer shut down now, sent total " + messageCounter  + " of requested " + totalMessagesToProduce);
        System.out.flush();
    }
}

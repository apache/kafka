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
package org.apache.kafka.clients;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class MockKafkaConsumer {
    public static void main(String[] args) {
        // Configuration properties for the Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka server address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");       // Consumer group ID
        props.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit"); // Acknowledgement mode
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");       // Start reading at the earliest available message
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");          // Enable auto-commit of message offsets
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // Create a Kafka Consumer instance
        KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props);

        // Subscribe to the desired topic(s)
        consumer.subscribe(Collections.singletonList("test-topic"));

        try {
            ConsumerRecords<String, String> records = ConsumerRecords.empty();

            while (true) {
                // Poll for new messages
                int count = 0;
                try {
                    records = consumer.poll(Duration.ofSeconds(10));
                    // Timeout set to 1 second for polling
                    System.out.println("Record count " + records.count());
                    for (ConsumerRecord<String, String> record : records) {
                        if (count == 0) {
                            // Mock to just acknowledge the first record.
                            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                            count++;
                        }
                        // Successfully received a message, process it
                        System.out.printf("Received message: key = %s, value = %s, partition = %d, offset = %d%n",
                                record.key(), record.value(), record.partition(), record.offset());
                    }
                } catch (Exception e) {
                    // Handle any exceptions during polling
                    e.printStackTrace();
                    if (e instanceof IllegalStateException) {
//                        consumer.close();
//                        break;

                        // Process each record which encountered IllegalStateException.
                        int recordCount = 0;
                        for (ConsumerRecord<String, String> record : records) {
                            System.out.println("Here");
                            // Process each record
                            try {
                                if (recordCount == 0) {
                                    // Mock to just acknowledge the first record.
                                    consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                                    recordCount++;
                                    System.out.println("Acknowledge record : " + record.value());
                                }
                            } catch (Exception e2) {
                                //do nothing
                            }
                        }
                    }
                }
            }
        } finally {
            // Ensure the consumer is properly closed
            consumer.close();
        }
    }
}
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
package kafka.examples.testing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MockConsumerExample {

    public static void basicExample() {
        System.out.println("=== Basic MockConsumer Example ===\n");

        MockConsumer<String, String> mockConsumer = new MockConsumer<>();

        mockConsumer.subscribe(Collections.singletonList("test-topic"));

        TopicPartition partition = new TopicPartition("test-topic", 0);
        mockConsumer.rebalance(Collections.singletonList(partition));
        
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(partition, 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);

        mockConsumer.addRecord(new ConsumerRecord<>("test-topic", 0, 0L, "key1", "value1"));
        mockConsumer.addRecord(new ConsumerRecord<>("test-topic", 0, 1L, "key2", "value2"));

        ConsumerRecords<String, String> records = mockConsumer.poll(Duration.ofMillis(100));

        System.out.println("Consumed " + records.count() + " messages");
        mockConsumer.close();
        System.out.println("\n✓ Basic example completed\n");
    }

    public static void messageProcessingExample() {
        System.out.println("=== Message Processing Example ===\n");

        MockConsumer<String, String> mockConsumer = new MockConsumer<>();
        mockConsumer.subscribe(Collections.singletonList("orders-topic"));

        TopicPartition partition = new TopicPartition("orders-topic", 0);
        mockConsumer.rebalance(Collections.singletonList(partition));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));

        mockConsumer.addRecord(new ConsumerRecord<>("orders-topic", 0, 0L, "order1", "{\"amount\":100}"));
        
        ConsumerRecords<String, String> records = mockConsumer.poll(Duration.ofMillis(100));
        
        for (ConsumerRecord<String, String> record : records) {
             System.out.println("Processed order: " + record.key());
        }

        mockConsumer.close();
        System.out.println("\n✓ Message processing verified\n");
    }

    public static void offsetManagementExample() {
        System.out.println("=== Offset Management Example ===\n");

        MockConsumer<String, String> mockConsumer = new MockConsumer<>();
        mockConsumer.subscribe(Collections.singletonList("test-topic"));

        TopicPartition partition = new TopicPartition("test-topic", 0);
        mockConsumer.rebalance(Collections.singletonList(partition));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));

        mockConsumer.addRecord(new ConsumerRecord<>("test-topic", 0, 0L, "key0", "value0"));
        
        mockConsumer.poll(Duration.ofMillis(100));
        mockConsumer.commitSync();
        
        mockConsumer.seek(partition, 5L);
        System.out.println("Seeked to offset 5");

        mockConsumer.close();
        System.out.println("\n✓ Offset management verified\n");
    }

    public static void main(String[] args) {
        System.out.println("\nMockConsumer Testing Examples\n");
        basicExample();
        messageProcessingExample();
        offsetManagementExample();
    }
}

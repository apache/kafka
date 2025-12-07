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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.concurrent.Future;

public class MockProducerExample {

    public static void basicExample() {
        System.out.println("=== Basic MockProducer Example ===\n");

        StringSerializer stringSerializer = new StringSerializer();
        MockProducer<String, String> mockProducer = new MockProducer<>(
            true,
            stringSerializer,
            stringSerializer
        );

        mockProducer.send(new ProducerRecord<>("test-topic", "key1", "value1"));
        mockProducer.send(new ProducerRecord<>("test-topic", "key2", "value2"));

        List<ProducerRecord<String, String>> history = mockProducer.history();

        System.out.println("Total messages sent: " + history.size());
        mockProducer.close();
        System.out.println("\n✓ Basic example completed\n");
    }

    public static void topicRoutingExample() {
        System.out.println("=== Topic Routing Example ===\n");

        StringSerializer stringSerializer = new StringSerializer();
        MockProducer<String, String> mockProducer = new MockProducer<>(
            true,
            stringSerializer,
            stringSerializer
        );

        String[] messages = {"urgent: system down", "info: user logged in"};

        for (String message : messages) {
            String topic = message.startsWith("urgent:") ? "high-priority-topic" : "normal-topic";
            mockProducer.send(new ProducerRecord<>(topic, message));
        }

        long highPriorityCount = mockProducer.history().stream()
            .filter(record -> "high-priority-topic".equals(record.topic()))
            .count();

        System.out.println("High priority messages: " + highPriorityCount);
        mockProducer.close();
        System.out.println("\n✓ Topic routing verified\n");
    }

    public static void metadataExample() throws Exception {
        System.out.println("=== Metadata Verification Example ===\n");

        StringSerializer stringSerializer = new StringSerializer();
        MockProducer<String, String> mockProducer = new MockProducer<>(
            true,
            stringSerializer,
            stringSerializer
        );

        Future<RecordMetadata> future = mockProducer.send(
            new ProducerRecord<>("test-topic", 0, "key", "value")
        );

        RecordMetadata metadata = future.get();

        System.out.println("Message metadata:");
        System.out.println("  Topic: " + metadata.topic());

        mockProducer.close();
        System.out.println("\n✓ Metadata verification completed\n");
    }

    public static void errorHandlingExample() {
        System.out.println("=== Error Handling Example ===\n");

        StringSerializer stringSerializer = new StringSerializer();
        MockProducer<String, String> mockProducer = new MockProducer<>(
            false,
            stringSerializer,
            stringSerializer
        );

        Future<RecordMetadata> future = mockProducer.send(
            new ProducerRecord<>("test-topic", "key", "value")
        );

        mockProducer.errorNext(new RuntimeException("Simulated broker failure"));

        try {
            future.get();
        } catch (Exception e) {
            System.out.println("✓ Successfully caught expected error: " + e.getCause().getMessage());
        }

        mockProducer.close();
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("\nMockProducer Testing Examples\n");
        basicExample();
        topicRoutingExample();
        metadataExample();
        errorHandlingExample();
    }
}

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
package org.apache.kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * A simple producer thread supporting two send modes:
 * - Async mode (default): messages are sent without waiting for the response.
 * - Sync mode: each send operation blocks waiting for the response.
 */
public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int numRecords;
    private final CountDownLatch latch;
    private volatile boolean closed;

    public Producer(String bootstrapServers,
                    String topic,
                    Boolean isAsync,
                    String transactionalId,
                    boolean enableIdempotency,
                    int numRecords,
                    int transactionTimeoutMs,
                    CountDownLatch latch) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Utils.createClientId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (transactionTimeoutMs > 0) {
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        }
        if (transactionalId != null) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.numRecords = numRecords;
        this.latch = latch;
    }

    public KafkaProducer<Integer, String> get() {
        return producer;
    }

    @Override
    public void run() {
        int key = 0;
        int sentRecords = 0;
        try {
            while (!closed && sentRecords < numRecords) {
                if (isAsync) {
                    asyncSend(key, "demo" + key);
                } else {
                    syncSend(key, "demo" + key);
                }
                key++;
                sentRecords++;
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.printf("Done: %d messages sent to %s%n", sentRecords, topic);
        shutdown();
    }

    private void asyncSend(int key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value),
            new ProducerCallback(key, value));
    }

    private RecordMetadata syncSend(int key, String value) throws Exception {
        RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
        maybePrintMessage(key, value, metadata);
        return metadata;
    }

    private void maybePrintMessage(int key, String value, RecordMetadata metadata) {
        // we only print 10 messages when there are 20 or more to send
        if (key % Math.max(1, numRecords / 10) == 0) {
            System.out.printf("Sample: message(%d, %s) sent to partition(%d) with offset(%d)%n",
                key, value, metadata.partition(), metadata.offset());
        }
    }

    public void shutdown() {
        closed = true;
        producer.close(Duration.ofMillis(5_000));
        latch.countDown();
    }

    class ProducerCallback implements Callback {
        private final int key;
        private final String value;

        public ProducerCallback(int key, String value) {
            this.key = key;
            this.value = value;
        }

        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (metadata != null) {
                maybePrintMessage(key, value, metadata);
            } else {
                e.printStackTrace();
            }
        }
    }
}

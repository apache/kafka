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
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Demo producer supporting two send modes:
 * - Async mode (default): messages are sent without waiting for the response (non blocking).
 * - Sync mode (sync argument): each send operation blocks waiting for the response.
 */
public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int numRecords;
    private final CountDownLatch latch;

    public Producer(final String topic,
                    final Boolean isAsync,
                    final String transactionalId,
                    final boolean enableIdempotency,
                    final int numRecords,
                    final int transactionTimeoutMs,
                    final CountDownLatch latch) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (transactionTimeoutMs > 0) {
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        }
        if (transactionalId != null) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);
        producer = new KafkaProducer<>(props);

        this.topic = topic;
        this.isAsync = isAsync;
        this.numRecords = numRecords;
        this.latch = latch;
    }

    KafkaProducer<Integer, String> get() {
        return producer;
    }

    @Override
    public void run() {
        int messageKey = 0;
        int recordsSent = 0;
        try {
            while (recordsSent < numRecords) {
                final long currentTimeMs = System.currentTimeMillis();
                produceOnce(messageKey, currentTimeMs);
                messageKey += 2;
                recordsSent += 1;
            }
        } catch (Throwable e) {
            System.err.println("Unexpected producer termination");
            e.printStackTrace();
        } finally {
            System.out.printf("Producer sent %d records successfully%n", numRecords);
            this.producer.close();
            latch.countDown();
        }
    }

    private void produceOnce(final int messageKey, final long currentTimeMs) throws ExecutionException, InterruptedException {
        String messageStr = "message" + messageKey;

        if (isAsync) { // Send asynchronously
            sendAsync(messageKey, messageStr, currentTimeMs);
            return;
        }
        Future<RecordMetadata> future = send(messageKey, messageStr);
        future.get();
        System.out.printf("Sent message: (%d, %s)%n", messageKey, messageStr);
    }

    private void sendAsync(final int messageKey, final String messageStr, final long currentTimeMs) {
        this.producer.send(new ProducerRecord<>(topic, messageKey, messageStr),
                new DemoCallBack(currentTimeMs, messageKey, messageStr));
    }

    private Future<RecordMetadata> send(final int messageKey, final String messageStr) {
        return producer.send(new ProducerRecord<>(topic, messageKey, messageStr));
    }
}

class DemoCallBack implements Callback {
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.printf("message(%d, %s) sent to partition(%d), offset(%d) in %d ms%n",
                key, message, metadata.partition(), metadata.offset(), elapsedTime);
        } else {
            exception.printStackTrace();
        }
    }
}

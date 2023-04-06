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

/**
 * A simple producer thread supporting two send modes:
 * - Async mode (default): records are sent without waiting for the response.
 * - Sync mode: each send operation blocks waiting for the response.
 */
public class Producer extends Thread {
    private final String bootstrapServers;
    private final String topic;
    private final boolean isAsync;
    private final String transactionalId;
    private final boolean enableIdempotency;
    private final int numRecords;
    private final int transactionTimeoutMs;
    private final CountDownLatch latch;
    private volatile boolean closed;

    public Producer(String threadName,
                    String bootstrapServers,
                    String topic,
                    boolean isAsync,
                    String transactionalId,
                    boolean enableIdempotency,
                    int numRecords,
                    int transactionTimeoutMs,
                    CountDownLatch latch) {
        super(threadName);
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.isAsync = isAsync;
        this.transactionalId = transactionalId;
        this.enableIdempotency = enableIdempotency;
        this.numRecords = numRecords;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.latch = latch;
    }

    @Override
    public void run() {
        int key = 0;
        int sentRecords = 0;
        // the producer instance is thread safe
        try (KafkaProducer<Integer, String> producer = createKafkaProducer()) {
            while (!closed && sentRecords < numRecords) {
                if (isAsync) {
                    asyncSend(producer, key, "test");
                } else {
                    syncSend(producer, key, "test");
                }
                key++;
                sentRecords++;
            }
        } catch (Throwable e) {
            Utils.printOut("Fatal error");
            e.printStackTrace();
        }
        Utils.printOut("Sent %d records", sentRecords);
        shutdown();
    }

    public void shutdown() {
        if (!closed) {
            closed = true;
            latch.countDown();
        }
    }

    public KafkaProducer<Integer, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (transactionTimeoutMs > 0) {
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        }
        if (transactionalId != null) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);
        return new KafkaProducer<>(props);
    }

    private void asyncSend(KafkaProducer<Integer, String> producer, int key, String value) {
        // still blocks when buffer.memory is full or metadata are not available
        producer.send(new ProducerRecord<>(topic, key, value),
            new ProducerCallback(key, value));
    }

    private RecordMetadata syncSend(KafkaProducer<Integer, String> producer, int key, String value)
        throws ExecutionException, InterruptedException {
        // add your application retry strategy here
        RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
        Utils.maybePrintRecord(numRecords, key, value, metadata);
        return metadata;
    }

    class ProducerCallback implements Callback {
        private final int key;
        private final String value;

        public ProducerCallback(int key, String value) {
            this.key = key;
            this.value = value;
        }

        public void onCompletion(RecordMetadata metadata, Exception e) {
            // add your stateful application retry strategy here
            if (e != null) {
                Utils.printErr(e.getMessage());
            } else {
                Utils.maybePrintRecord(numRecords, key, value, metadata);
            }
        }
    }
}

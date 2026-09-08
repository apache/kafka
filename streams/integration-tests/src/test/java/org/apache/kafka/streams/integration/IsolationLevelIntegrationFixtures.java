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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared plumbing for the IQv1 and IQv2 isolation-level integration tests.
 *
 * <p>Both tests need the same thing: start a Streams app with {@code enable.transactional.statestores=true}
 * under EOS, drive a single record through a processor that writes to a state store and then sits in a
 * stall loop, and — while the processor is stalled — interrogate the store through both
 * {@code READ_UNCOMMITTED} and {@code READ_COMMITTED} views. The stall keeps the StreamThread out of its
 * consumer loop, which prevents any commit from firing; so staged writes remain in the transaction buffer
 * and {@code READ_COMMITTED} must not see them until the stall is released.
 *
 * <p>The gate itself is two latches plus a boolean:
 * <ul>
 *   <li>{@link StallGate#stalled}: counted down by the processor the moment it enters the stall loop,
 *       so the test knows the write has happened and the commit has not.</li>
 *   <li>{@link StallGate#released}: the processor spins on this flag until the test flips it.</li>
 *   <li>{@link StallGate#exited}: counted down by the processor on the way out, so the test can wait for
 *       the stream thread to return to its consumer loop (and thereby allow a commit to occur).</li>
 * </ul>
 */
final class IsolationLevelIntegrationFixtures {

    private IsolationLevelIntegrationFixtures() {
    }

    /** Keys & values are small integers throughout. The sentinel key is distinct from any test payload. */
    static final int SENTINEL_KEY = -1;
    static final int TEST_KEY = 7;
    static final int TEST_VALUE = 42;
    static final long TEST_TIMESTAMP = 1_700_000_000_000L;

    /** Coordination object shared between the processor and the test body. */
    static final class StallGate {
        final CountDownLatch stalled = new CountDownLatch(1);
        final AtomicBoolean released = new AtomicBoolean(false);
        final CountDownLatch exited = new CountDownLatch(1);

        void awaitStalled() throws InterruptedException {
            if (!stalled.await(60, TimeUnit.SECONDS)) {
                fail("Processor never entered stall");
            }
        }

        void release() {
            released.set(true);
        }

        void awaitExit() throws InterruptedException {
            if (!exited.await(60, TimeUnit.SECONDS)) {
                fail("Processor never left stall");
            }
        }

        /** Run from inside the processor: signal stalled, spin until released, signal exited. */
        void spinHere() {
            stalled.countDown();
            try {
                while (!released.get()) {
                    Thread.sleep(50L);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                exited.countDown();
            }
        }
    }

    /**
     * Base Streams configuration needed to observe isolation-level differences:
     * EOS + transactional statestores + no cache + a small but non-zero commit interval so that once the
     * stall is released, a commit fires promptly. Caller layers on application id, bootstrap servers, state
     * dir, and the isolation-level default under test.
     */
    static Properties baseStreamsConfig() {
        final Properties p = new Properties();
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, "true");
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        return p;
    }

    /** Synchronous single-record producer — returns once the record is acked by the broker. */
    static void sendOne(final String bootstrap,
                        final String topic,
                        final int key,
                        final int value,
                        final long timestamp) throws Exception {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        try (final Producer<Integer, Integer> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, 0, timestamp, key, value)).get(30, TimeUnit.SECONDS);
        }
    }

    /** Polling deadline used by visibility assertions that wait on a commit. */
    static final Duration COMMIT_WAIT = Duration.ofSeconds(30);

    /**
     * Broker-side overrides that let the embedded 1-broker cluster support EOS transactions — the default
     * transaction state-log replication/ISR requirements need 3 brokers, which is more than we need.
     */
    static Properties singleBrokerEosOverrides() {
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("auto.create.topics.enable", "true");
        overrides.put("transaction.state.log.replication.factor", "1");
        overrides.put("transaction.state.log.min.isr", "1");
        final Properties props = new Properties();
        props.putAll(overrides);
        return props;
    }
}

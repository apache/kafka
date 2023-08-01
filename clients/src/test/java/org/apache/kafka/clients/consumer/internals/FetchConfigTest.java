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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class FetchConfigTest {

    /**
     * Verify correctness if both the key and value {@link Deserializer deserializers} provided to the
     * {@link FetchConfig} constructors are {@code nonnull}.
     */
    @Test
    public void testBasicFromConsumerConfig() {
        try (StringDeserializer keyDeserializer = new StringDeserializer(); StringDeserializer valueDeserializer = new StringDeserializer()) {
            newFetchConfigFromConsumerConfig(keyDeserializer, valueDeserializer);
            newFetchConfigFromValues(keyDeserializer, valueDeserializer);
        }
    }

    /**
     * Verify an exception is thrown if the key {@link Deserializer deserializer} provided to the
     * {@link FetchConfig} constructors is {@code null}.
     */
    @Test
    public void testPreventNullKeyDeserializer() {
        try (StringDeserializer valueDeserializer = new StringDeserializer()) {
            assertThrows(NullPointerException.class, () -> newFetchConfigFromConsumerConfig(null, valueDeserializer));
            assertThrows(NullPointerException.class, () -> newFetchConfigFromValues(null, valueDeserializer));
        }
    }

    /**
     * Verify an exception is thrown if the value {@link Deserializer deserializer} provided to the
     * {@link FetchConfig} constructors is {@code null}.
     */
    @Test
    @SuppressWarnings("resources")
    public void testPreventNullValueDeserializer() {
        try (StringDeserializer keyDeserializer = new StringDeserializer()) {
            assertThrows(NullPointerException.class, () -> newFetchConfigFromConsumerConfig(keyDeserializer, null));
            assertThrows(NullPointerException.class, () -> newFetchConfigFromValues(keyDeserializer, null));
        }
    }

    private void newFetchConfigFromConsumerConfig(Deserializer<String> keyDeserializer,
                                                  Deserializer<String> valueDeserializer) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ConsumerConfig config = new ConsumerConfig(p);
        new FetchConfig<>(config, keyDeserializer, valueDeserializer, IsolationLevel.READ_UNCOMMITTED);
    }

    private void newFetchConfigFromValues(Deserializer<String> keyDeserializer,
                                          Deserializer<String> valueDeserializer) {
        new FetchConfig<>(ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
                ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
                ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
                true,
                ConsumerConfig.DEFAULT_CLIENT_RACK,
                keyDeserializer,
                valueDeserializer,
                IsolationLevel.READ_UNCOMMITTED);
    }
}

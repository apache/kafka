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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * {@code ConsumerCreator} implements a quasi-factory pattern to allow the caller to remain unaware of the
 * underlying {@link Consumer} implementation that is created. This provides the means by which {@link KafkaConsumer}
 * can remain the top-level facade for implementations, but allow different implementations to co-exist under
 * the covers.
 *
 * <p/>
 *
 * The current logic for the {@code ConsumerCreator} inspects the incoming configuration and determines if
 * it is using the new KIP-848 consumer protocol or if it should fall back to the existing, legacy group protocol.
 * This is based on the presence and value of the {@code group.protocol} configuration value. If the value is present
 * and equals {@code consumer}, the {@link AsyncKafkaConsumer} will be returned. Otherwise, the
 * {@link LegacyKafkaConsumer} will be returned.
 *
 * <p/>
 *
 * This is not to be called by end users and callers should not attempt to determine the underlying implementation
 * as this will make such code very brittle. Users of this facility should honor the top-level {@link Consumer} API
 * contract as-is.
 */
public class ConsumerCreator {

    /**
     * This is it! This is the core logic. It's extremely rudimentary.
     */
    private static boolean useNewConsumer(Map<?, ?> configs) {
        Object groupProtocol = configs.get("group.protocol");

        // Takes care of both the null and type checks.
        if (!(groupProtocol instanceof String))
            return false;

        return ((String) groupProtocol).equalsIgnoreCase("consumer");
    }

    public <K, V> Consumer<K, V> create(Map<String, Object> configs) {
        return createInternal(() -> {
            if (useNewConsumer(configs)) {
                return new AsyncKafkaConsumer<>(configs);
            } else {
                return new LegacyKafkaConsumer<>(configs);
            }
        });
    }

    public <K, V> Consumer<K, V> create(Properties properties) {
        return createInternal(() -> {
            if (useNewConsumer(properties)) {
                return new AsyncKafkaConsumer<>(properties);
            } else {
                return new LegacyKafkaConsumer<>(properties);
            }
        });
    }

    public <K, V> Consumer<K, V> create(Properties properties,
                                 Deserializer<K> keyDeserializer,
                                 Deserializer<V> valueDeserializer) {
        return createInternal(() -> {
            if (useNewConsumer(properties)) {
                return new AsyncKafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
            } else {
                return new LegacyKafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
            }
        });
    }

    public <K, V> Consumer<K, V> create(Map<String, Object> configs,
                                 Deserializer<K> keyDeserializer,
                                 Deserializer<V> valueDeserializer) {
        return createInternal(() -> {
            if (useNewConsumer(configs)) {
                return new AsyncKafkaConsumer<>(configs, keyDeserializer, valueDeserializer);
            } else {
                return new LegacyKafkaConsumer<>(configs, keyDeserializer, valueDeserializer);
            }
        });
    }

    public <K, V> Consumer<K, V> create(ConsumerConfig config,
                                        Deserializer<K> keyDeserializer,
                                        Deserializer<V> valueDeserializer) {
        return createInternal(() -> {
            if (useNewConsumer(config.values())) {
                return new AsyncKafkaConsumer<>(config, keyDeserializer, valueDeserializer);
            } else {
                return new LegacyKafkaConsumer<>(config, keyDeserializer, valueDeserializer);
            }
        });
    }

    private <K, V> Consumer<K, V> createInternal(Supplier<Consumer<K, V>> consumerSupplier) {
        try {
            return consumerSupplier.get();
        } catch (KafkaException e) {
            throw e;
        } catch (Throwable t) {
            throw new KafkaException("Failed to construct Kafka consumer", t);
        }
    }
}

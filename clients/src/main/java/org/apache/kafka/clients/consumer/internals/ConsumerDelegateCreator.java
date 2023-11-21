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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.List;
import java.util.Locale;

/**
 * {@code ConsumerDelegateCreator} implements a quasi-factory pattern to allow the caller to remain unaware of the
 * underlying {@link Consumer} implementation that is created. This provides the means by which {@link KafkaConsumer}
 * can remain the top-level facade for implementations, but allow different implementations to co-exist under
 * the covers.
 *
 * <p/>
 *
 * The current logic for the {@code ConsumerCreator} inspects the incoming configuration and determines if
 * it is using the new consumer group protocol (KIP-848) or if it should fall back to the existing, legacy group
 * protocol. This is based on the presence and value of the {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG group.protocol}
 * configuration. If the value is present and equal to &quot;{@code consumer}&quot;, the {@link AsyncKafkaConsumer}
 * will be returned. Otherwise, the {@link LegacyKafkaConsumer} will be returned.
 *
 *
 * <p/>
 *
 * <em>Note</em>: this is for internal use only and is not intended for use by end users. Internal users should
 * not attempt to determine the underlying implementation to avoid coding to an unstable interface. Rather, it is
 * the {@link Consumer} API contract that should serve as the caller's interface.
 */
public class ConsumerDelegateCreator {

    public <K, V> ConsumerDelegate<K, V> create(ConsumerConfig config,
                                                Deserializer<K> keyDeserializer,
                                                Deserializer<V> valueDeserializer) {
        try {
            GroupProtocol groupProtocol = GroupProtocol.valueOf(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG).toUpperCase(Locale.ROOT));

            if (groupProtocol == GroupProtocol.CONSUMER)
                return new AsyncKafkaConsumer<>(config, keyDeserializer, valueDeserializer);
            else
                return new LegacyKafkaConsumer<>(config, keyDeserializer, valueDeserializer);
        } catch (KafkaException e) {
            throw e;
        } catch (Throwable t) {
            throw new KafkaException("Failed to construct Kafka consumer", t);
        }
    }

    public <K, V> ConsumerDelegate<K, V> create(LogContext logContext,
                                                Time time,
                                                ConsumerConfig config,
                                                Deserializer<K> keyDeserializer,
                                                Deserializer<V> valueDeserializer,
                                                KafkaClient client,
                                                SubscriptionState subscriptions,
                                                ConsumerMetadata metadata,
                                                List<ConsumerPartitionAssignor> assignors) {
        try {
            GroupProtocol groupProtocol = GroupProtocol.valueOf(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG).toUpperCase(Locale.ROOT));

            if (groupProtocol == GroupProtocol.CONSUMER)
                return new AsyncKafkaConsumer<>(
                    logContext,
                    time,
                    config,
                    keyDeserializer,
                    valueDeserializer,
                    client,
                    subscriptions,
                    metadata,
                    assignors
                );
            else
                return new LegacyKafkaConsumer<>(
                    logContext,
                    time,
                    config,
                    keyDeserializer,
                    valueDeserializer,
                    client,
                    subscriptions,
                    metadata,
                    assignors
                );
        } catch (KafkaException e) {
            throw e;
        } catch (Throwable t) {
            throw new KafkaException("Failed to construct Kafka consumer", t);
        }
    }
}

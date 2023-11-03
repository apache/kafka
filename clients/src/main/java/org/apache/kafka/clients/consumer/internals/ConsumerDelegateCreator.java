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
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * {@code ConsumerDelegateCreator} implements a quasi-factory pattern to allow the caller to remain unaware of the
 * underlying {@link Consumer} implementation that is created. This provides the means by which {@link KafkaConsumer}
 * can remain the top-level facade for implementations, but allow different implementations to co-exist under
 * the covers.
 *
 * <p/>
 *
 * The current logic for the {@code ConsumerCreator} inspects the incoming configuration and determines if
 * it is using the new KIP-848 consumer protocol or if it should fall back to the existing, legacy group protocol.
 * This is based on the presence and value of the {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG group.protocol}
 * configuration. If the value is present and equal to &quot;{@code consumer}&quot;, the {@link AsyncKafkaConsumer}
 * will be returned. Otherwise, the {@link LegacyKafkaConsumer} will be returned.
 *
 * <p/>
 *
 * This is not to be called by end users and callers should not attempt to determine the underlying implementation
 * as this will make such code very brittle. Users of this facility should honor the top-level {@link Consumer} API
 * contract as-is.
 */
public class ConsumerDelegateCreator {

    public <K, V> Consumer<K, V> create(ConsumerConfig config,
                                        Deserializer<K> keyDeserializer,
                                        Deserializer<V> valueDeserializer) {
        try {
            GroupProtocol groupProtocol = GroupProtocol.valueOf(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG));

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
}

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
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * {@code ShareConsumerDelegateCreator} implements a quasi-factory pattern to allow the caller to remain unaware of the
 * underlying {@link ShareConsumer} implementation that is created. This provides the means by which
 * {@link KafkaShareConsumer} can remain the top-level facade for implementations, but allow different implementations
 * to co-exist under the covers.
 *
 * <p>
 * <em>Note</em>: this is for internal use only and is not intended for use by end users. Internal users should
 * not attempt to determine the underlying implementation to avoid coding to an unstable interface. Rather, it is
 * the {@link ShareConsumer} API contract that should serve as the caller's interface.
 */
public class ShareConsumerDelegateCreator {
    public <K, V> ShareConsumer<K, V> create(ConsumerConfig config,
                                             Deserializer<K> keyDeserializer,
                                             Deserializer<V> valueDeserializer) {
        try {
            return new ShareConsumerImpl<>(config, keyDeserializer, valueDeserializer);
        } catch (KafkaException e) {
            throw e;
        } catch (Throwable t) {
            throw new KafkaException("Failed to construct Kafka share consumer", t);
        }
    }
}
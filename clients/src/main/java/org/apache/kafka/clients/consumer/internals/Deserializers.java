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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class Deserializers<K, V> implements AutoCloseable {

    public final Deserializer<K> keyDeserializer;
    public final Deserializer<V> valueDeserializer;

    public Deserializers(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "Key deserializer provided to Deserializers should not be null");
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer, "Value deserializer provided to Deserializers should not be null");
    }

    public Deserializers(ConsumerConfig config) {
        this(config, null, null);
    }

    @SuppressWarnings("unchecked")
    public Deserializers(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

        if (keyDeserializer == null) {
            this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            this.keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
        } else {
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            this.keyDeserializer = keyDeserializer;
        }

        if (valueDeserializer == null) {
            this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            this.valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
        } else {
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            this.valueDeserializer = valueDeserializer;
        }
    }

    @Override
    public void close() {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        Utils.closeQuietly(keyDeserializer, "key deserializer", firstException);
        Utils.closeQuietly(valueDeserializer, "value deserializer", firstException);
        Throwable exception = firstException.get();

        if (exception != null) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close deserializers", exception);
        }
    }

    @Override
    public String toString() {
        return "Deserializers{" +
                "keyDeserializer=" + keyDeserializer +
                ", valueDeserializer=" + valueDeserializer +
                '}';
    }
}

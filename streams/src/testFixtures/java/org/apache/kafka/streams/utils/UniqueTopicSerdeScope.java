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
package org.apache.kafka.streams.utils;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UniqueTopicSerdeScope {
    private final Map<String, Class<?>> topicTypeRegistry = new TreeMap<>();

    public <T> UniqueTopicSerdeDecorator<T> decorateSerde(final Serde<T> delegate,
                                                          final Properties config,
                                                          final boolean isKey) {
        final UniqueTopicSerdeDecorator<T> decorator = new UniqueTopicSerdeDecorator<>(delegate);
        decorator.configure(config.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)), isKey);
        return decorator;
    }

    public Set<String> registeredTopics() {
        return Collections.unmodifiableSet(topicTypeRegistry.keySet());
    }

    public class UniqueTopicSerdeDecorator<T> implements Serde<T> {
        private final AtomicBoolean isKey = new AtomicBoolean(false);
        private final Serde<T> delegate;

        public UniqueTopicSerdeDecorator(final Serde<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            delegate.configure(configs, isKey);
            this.isKey.set(isKey);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public Serializer<T> serializer() {
            return new UniqueTopicSerializerDecorator<>(isKey, delegate.serializer());
        }

        @Override
        public Deserializer<T> deserializer() {
            return new UniqueTopicDeserializerDecorator<>(isKey, delegate.deserializer());
        }
    }

    public class UniqueTopicSerializerDecorator<T> implements Serializer<T> {
        private final AtomicBoolean isKey;
        private final Serializer<T> delegate;

        public UniqueTopicSerializerDecorator(final AtomicBoolean isKey, final Serializer<T> delegate) {
            this.isKey = isKey;
            this.delegate = delegate;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            delegate.configure(configs, isKey);
            this.isKey.set(isKey);
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            verifyTopic(topic, data);
            return delegate.serialize(topic, data);
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final T data) {
            verifyTopic(topic, data);
            return delegate.serialize(topic, headers, data);
        }

        private void verifyTopic(final String topic, final T data) {
            if (data != null) {
                final String key = topic + (isKey.get() ? "--key" : "--value");
                if (topicTypeRegistry.containsKey(key)) {
                    assertThat(String.format("key[%s] data[%s][%s]", key, data, data.getClass()), topicTypeRegistry.get(key), equalTo(data.getClass()));
                } else {
                    topicTypeRegistry.put(key, data.getClass());
                }
            }
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    public class UniqueTopicDeserializerDecorator<T> implements Deserializer<T> {
        private final AtomicBoolean isKey;
        private final Deserializer<T> delegate;

        public UniqueTopicDeserializerDecorator(final AtomicBoolean isKey, final Deserializer<T> delegate) {
            this.isKey = isKey;
            this.delegate = delegate;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            delegate.configure(configs, isKey);
            this.isKey.set(isKey);
        }

        @Override
        public T deserialize(final String topic, final byte[] data) {
            return delegate.deserialize(topic, data);
        }

        @Override
        public T deserialize(final String topic, final Headers headers, final byte[] data) {
            return delegate.deserialize(topic, headers, data);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}

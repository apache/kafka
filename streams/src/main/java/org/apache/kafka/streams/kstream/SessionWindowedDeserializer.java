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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.internals.SessionKeySchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SessionWindowedDeserializer<T> implements Deserializer<Windowed<T>> {

    /**
     * Default deserializer for the inner deserializer class of a windowed record. Must implement the {@link Serde} interface.
     */
    public static final String WINDOWED_INNER_DESERIALIZER_CLASS = "windowed.inner.deserializer.class";

    private final Logger log = LoggerFactory.getLogger(SessionWindowedDeserializer.class);

    private Deserializer<T> inner;

    // Default constructor needed by Kafka
    public SessionWindowedDeserializer() {}

    public SessionWindowedDeserializer(final Deserializer<T> inner) {
        this.inner = inner;
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        String deserializerConfigKey = WINDOWED_INNER_DESERIALIZER_CLASS;
        String deserializerConfigValue = (String) configs.get(WINDOWED_INNER_DESERIALIZER_CLASS);
        if (deserializerConfigValue == null) {
            final String windowedInnerClassSerdeConfig = (String) configs.get(StreamsConfig.WINDOWED_INNER_CLASS_SERDE);
            if (windowedInnerClassSerdeConfig != null) {
                deserializerConfigKey = StreamsConfig.WINDOWED_INNER_CLASS_SERDE;
                deserializerConfigValue = windowedInnerClassSerdeConfig;
                log.warn("Config {} is deprecated. Please use {} instead.",
                    StreamsConfig.WINDOWED_INNER_CLASS_SERDE, WINDOWED_INNER_DESERIALIZER_CLASS);
            }
        }

        Serde<T> windowedInnerDeserializerClass = null;
        if (deserializerConfigValue != null) {
            try {
                windowedInnerDeserializerClass = Utils.newInstance(deserializerConfigValue, Serde.class);
            } catch (final ClassNotFoundException e) {
                throw new ConfigException(deserializerConfigKey, deserializerConfigValue,
                    "Serde class " + deserializerConfigValue + " could not be found.");
            }
        }

        if (inner != null && deserializerConfigValue != null) {
            if (!inner.getClass().getName().equals(windowedInnerDeserializerClass.deserializer().getClass().getName())) {
                throw new IllegalArgumentException("Inner class deserializer set using constructor "
                    + "(" + inner.getClass().getName() + ")" +
                    " is different from the one set in " + deserializerConfigKey + " config " +
                    "(" + windowedInnerDeserializerClass.deserializer().getClass().getName() + ").");
            }
        } else if (inner == null && deserializerConfigValue == null) {
            throw new IllegalArgumentException("Inner class deserializer should be set either via constructor " +
                "or via the " + WINDOWED_INNER_DESERIALIZER_CLASS + " config");
        } else if (inner == null)
            inner = windowedInnerDeserializerClass.deserializer();
    }

    @Override
    public Windowed<T> deserialize(final String topic, final byte[] data) {
        return deserialize(topic, new RecordHeaders(), data);
    }

    @Override
    public Windowed<T> deserialize(final String topic, final Headers headers, final byte[] data) {
        WindowedSerdes.verifyInnerDeserializerNotNull(inner, this);

        if (data == null || data.length == 0) {
            return null;
        }

        // for either key or value, their schema is the same hence we will just use session key schema
        return SessionKeySchema.from(data, inner, headers, topic);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

    // Only for testing
    Deserializer<T> innerDeserializer() {
        return inner;
    }
}

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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.internals.WindowKeySchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TimeWindowedSerializer<T> implements WindowedSerializer<T> {

    /**
     * Configuration key for the windowed inner serializer class.
     */
    public static final String WINDOWED_INNER_SERIALIZER_CLASS = "windowed.inner.serializer.class";

    private final Logger log = LoggerFactory.getLogger(TimeWindowedSerializer.class);

    private Serializer<T> inner;

    // Default constructor needed by Kafka
    @SuppressWarnings("WeakerAccess")
    public TimeWindowedSerializer() {}

    public TimeWindowedSerializer(final Serializer<T> inner) {
        this.inner = inner;
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        String serializerConfigFrom = WINDOWED_INNER_SERIALIZER_CLASS;
        String windowedInnerSerializerClassConfig = (String) configs.get(WINDOWED_INNER_SERIALIZER_CLASS);
        if (windowedInnerSerializerClassConfig == null) {
            final String windowedInnerClassSerdeConfig = (String) configs.get(StreamsConfig.WINDOWED_INNER_CLASS_SERDE);
            if (windowedInnerClassSerdeConfig != null) {
                serializerConfigFrom = StreamsConfig.WINDOWED_INNER_CLASS_SERDE;
                windowedInnerSerializerClassConfig = windowedInnerClassSerdeConfig;
                log.warn("Config {} is deprecated. Please use {} instead.",
                    StreamsConfig.WINDOWED_INNER_CLASS_SERDE, WINDOWED_INNER_SERIALIZER_CLASS);
            }
        }
        Serde<T> windowedInnerSerializerClass = null;
        if (windowedInnerSerializerClassConfig != null) {
            try {
                windowedInnerSerializerClass = Utils.newInstance(windowedInnerSerializerClassConfig, Serde.class);
            } catch (final ClassNotFoundException e) {
                throw new ConfigException(serializerConfigFrom, windowedInnerSerializerClassConfig,
                    "Serde class " + windowedInnerSerializerClassConfig + " could not be found.");
            }
        }

        if (inner != null && windowedInnerSerializerClassConfig != null) {
            if (!inner.getClass().getName().equals(windowedInnerSerializerClass.serializer().getClass().getName())) {
                throw new IllegalArgumentException("Inner class serializer set using constructor "
                    + "(" + inner.getClass().getName() + ")" +
                    " is different from the one set in " + serializerConfigFrom + " config " +
                    "(" + windowedInnerSerializerClass.serializer().getClass().getName() + ").");
            }
        } else if (inner == null && windowedInnerSerializerClassConfig == null) {
            throw new IllegalArgumentException("Inner class serializer should be set either via constructor " +
                "or via the " + WINDOWED_INNER_SERIALIZER_CLASS + " config");
        } else if (inner == null)
            inner = windowedInnerSerializerClass.serializer();
    }

    @Override
    public byte[] serialize(final String topic, final Windowed<T> data) {
        WindowedSerdes.verifyInnerSerializerNotNull(inner, this);

        if (data == null) {
            return null;
        }

        return WindowKeySchema.toBinary(data, inner, topic);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

    @Override
    public byte[] serializeBaseKey(final String topic, final Windowed<T> data) {
        WindowedSerdes.verifyInnerSerializerNotNull(inner, this);

        return inner.serialize(topic, data.key());
    }

    // Only for testing
    Serializer<T> innerSerializer() {
        return inner;
    }
}

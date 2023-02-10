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
import org.apache.kafka.streams.state.internals.SessionKeySchema;

import java.util.Map;

public class SessionWindowedSerializer<T> implements WindowedSerializer<T> {

    private Serializer<T> inner;

    // Default constructor needed by Kafka
    public SessionWindowedSerializer() {}

    public SessionWindowedSerializer(final Serializer<T> inner) {
        this.inner = inner;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final String windowedInnerClassSerdeConfig = (String) configs.get(StreamsConfig.WINDOWED_INNER_CLASS_SERDE);
        Serde<T> windowInnerClassSerde = null;
        if (windowedInnerClassSerdeConfig != null) {
            try {
                windowInnerClassSerde = Utils.newInstance(windowedInnerClassSerdeConfig, Serde.class);
            } catch (final ClassNotFoundException e) {
                throw new ConfigException(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, windowedInnerClassSerdeConfig,
                    "Serde class " + windowedInnerClassSerdeConfig + " could not be found.");
            }
        }

        if (inner != null && windowedInnerClassSerdeConfig != null) {
            if (!inner.getClass().getName().equals(windowInnerClassSerde.serializer().getClass().getName())) {
                throw new IllegalArgumentException("Inner class serializer set using constructor "
                    + "(" + inner.getClass().getName() + ")" +
                    " is different from the one set in windowed.inner.class.serde config " +
                    "(" + windowInnerClassSerde.serializer().getClass().getName() + ").");
            }
        } else if (inner == null && windowedInnerClassSerdeConfig == null) {
            throw new IllegalArgumentException("Inner class serializer should be set either via constructor " +
                "or via the windowed.inner.class.serde config");
        } else if (inner == null)
            inner = windowInnerClassSerde.serializer();
    }

    @Override
    public byte[] serialize(final String topic, final Windowed<T> data) {
        WindowedSerdes.verifyInnerSerializerNotNull(inner, this);

        if (data == null) {
            return null;
        }
        // for either key or value, their schema is the same hence we will just use session key schema
        return SessionKeySchema.toBinary(data, inner, topic);
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

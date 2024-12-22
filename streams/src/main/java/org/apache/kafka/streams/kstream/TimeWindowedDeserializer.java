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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.internals.WindowKeySchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TimeWindowedDeserializer<T> implements Deserializer<Windowed<T>> {

    /**
     * Configuration key for the window size in milliseconds.
     */
    public static final String WINDOW_SIZE_MS_CONFIG = "window.size.ms";

    /**
     * Configuration key for the windowed inner deserializer class.
     */
    public static final String WINDOWED_INNER_DESERIALIZER_CLASS = "windowed.inner.deserializer.class";

    private final Logger log = LoggerFactory.getLogger(TimeWindowedDeserializer.class);

    private Long windowSize;
    private boolean isChangelogTopic;

    private Deserializer<T> inner;

    // Default constructor needed by Kafka
    public TimeWindowedDeserializer() {
        this(null, null);
    }

    public TimeWindowedDeserializer(final Deserializer<T> inner, final Long windowSize) {
        this.inner = inner;
        this.windowSize = windowSize;
        this.isChangelogTopic = false;
    }

    public Long getWindowSize() {
        return this.windowSize;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        configureWindowSizeMs(configs);
        configureWindowInnerDeserializerClass(configs);
    }

    @Override
    public Windowed<T> deserialize(final String topic, final byte[] data) {
        WindowedSerdes.verifyInnerDeserializerNotNull(inner, this);

        if (data == null || data.length == 0) {
            return null;
        }

        // toStoreKeyBinary was used to serialize the data.
        if (this.isChangelogTopic) {
            return WindowKeySchema.fromStoreKey(data, windowSize, inner, topic);
        }

        // toBinary was used to serialize the data
        return WindowKeySchema.from(data, windowSize, inner, topic);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

    public void setIsChangelogTopic(final boolean isChangelogTopic) {
        this.isChangelogTopic = isChangelogTopic;
    }

    // Only for testing
    Deserializer<T> innerDeserializer() {
        return inner;
    }

    private void configureWindowSizeMs(final Map<String, ?> configs) {
        //check to see if the window size config is set and the window size is already set from the constructor
        final Long configWindowSize;
        if (configs.get(WINDOW_SIZE_MS_CONFIG) instanceof String) {
            configWindowSize = Long.parseLong((String) configs.get(WINDOW_SIZE_MS_CONFIG));
        } else {
            configWindowSize = (Long) configs.get(WINDOW_SIZE_MS_CONFIG);
        }
        if (windowSize != null && configWindowSize != null) {
            throw new IllegalArgumentException("Window size should not be set in both the time windowed deserializer constructor and the window.size.ms config");
        } else if (windowSize == null && configWindowSize == null) {
            throw new IllegalArgumentException("Window size needs to be set either through the time windowed deserializer " +
                "constructor or the window.size.ms config but not both");
        } else {
            windowSize = windowSize == null ? configWindowSize : windowSize;
        }
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    private void configureWindowInnerDeserializerClass(final Map<String, ?> configs) {
        String deserializerConfigFrom = WINDOWED_INNER_DESERIALIZER_CLASS;
        String windowedInnerDeserializerClassConfig = (String) configs.get(WINDOWED_INNER_DESERIALIZER_CLASS);
        if (windowedInnerDeserializerClassConfig == null) {
            final String windowedInnerClassSerdeConfig = (String) configs.get(StreamsConfig.WINDOWED_INNER_CLASS_SERDE);
            if (windowedInnerClassSerdeConfig != null) {
                deserializerConfigFrom = StreamsConfig.WINDOWED_INNER_CLASS_SERDE;
                windowedInnerDeserializerClassConfig = windowedInnerClassSerdeConfig;
                log.warn("Config {} is deprecated. Please use {} instead.",
                    StreamsConfig.WINDOWED_INNER_CLASS_SERDE, WINDOWED_INNER_DESERIALIZER_CLASS);
            }
        }

        Serde<T> windowedInnerDeserializerClass = null;
        if (windowedInnerDeserializerClassConfig != null) {
            try {
                windowedInnerDeserializerClass = Utils.newInstance(windowedInnerDeserializerClassConfig, Serde.class);
            } catch (final ClassNotFoundException e) {
                throw new ConfigException(deserializerConfigFrom, windowedInnerDeserializerClassConfig,
                    "Serde class " + windowedInnerDeserializerClassConfig + " could not be found.");
            }
        }

        if (inner != null && windowedInnerDeserializerClassConfig != null) {
            if (!inner.getClass().getName().equals(windowedInnerDeserializerClass.deserializer().getClass().getName())) {
                throw new IllegalArgumentException("Inner class deserializer set using constructor "
                    + "(" + inner.getClass().getName() + ")" +
                    " is different from the one set in " + windowedInnerDeserializerClassConfig + " config " +
                    "(" + windowedInnerDeserializerClass.deserializer().getClass().getName() + ").");
            }
        } else if (inner == null && windowedInnerDeserializerClassConfig == null) {
            throw new IllegalArgumentException("Inner class deserializer should be set either via  constructor " +
                "or via the " + WINDOWED_INNER_DESERIALIZER_CLASS + " config");
        } else if (inner == null)
            inner = windowedInnerDeserializerClass.deserializer();
    }
}

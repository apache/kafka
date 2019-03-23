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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class WindowedSerdes {

    static public class TimeWindowedSerde<T> extends Serdes.WrapperSerde<Windowed<T>> {
        // Default constructor needed for reflection object creation
        public TimeWindowedSerde() {
            super(new TimeWindowedSerializer<>(), new TimeWindowedDeserializer<>());
        }

        public TimeWindowedSerde(final Serde<T> inner) {
            super(new TimeWindowedSerializer<>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer()));
        }

        // This constructor can be used for serialize/deserialize a windowed topic
        public TimeWindowedSerde(final Serde<T> inner, final long windowSize) {
            super(new TimeWindowedSerializer<>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer(), windowSize));
        }

        // Helper method for users to specify whether the input topic is a changelog topic for deserializing the key properly.
        public TimeWindowedSerde<T> forChangelog(final boolean isChangelogTopic) {
            final TimeWindowedDeserializer deserializer = (TimeWindowedDeserializer) this.deserializer();
            deserializer.setIsChangelogTopic(isChangelogTopic);
            return this;
        }
    }

    static public class SessionWindowedSerde<T> extends Serdes.WrapperSerde<Windowed<T>> {
        // Default constructor needed for reflection object creation
        public SessionWindowedSerde() {
            super(new SessionWindowedSerializer<>(), new SessionWindowedDeserializer<>());
        }

        public SessionWindowedSerde(final Serde<T> inner) {
            super(new SessionWindowedSerializer<>(inner.serializer()), new SessionWindowedDeserializer<>(inner.deserializer()));
        }
    }

    /**
     * Construct a {@code TimeWindowedSerde} object for the specified inner class type.
     */
    static public <T> Serde<Windowed<T>> timeWindowedSerdeFrom(final Class<T> type) {
        return new TimeWindowedSerde<>(Serdes.serdeFrom(type));
    }

    /**
     * Construct a {@code TimeWindowedSerde} object to deserialize changelog topic
     * for the specified inner class type and window size.
     */
    static public <T> Serde<Windowed<T>> timeWindowedSerdeFrom(final Class<T> type, final long windowSize) {
        return new TimeWindowedSerde<>(Serdes.serdeFrom(type), windowSize);
    }

    /**
     * Construct a {@code SessionWindowedSerde} object for the specified inner class type.
     */
    static public <T> Serde<Windowed<T>> sessionWindowedSerdeFrom(final Class<T> type) {
        return new SessionWindowedSerde<>(Serdes.serdeFrom(type));
    }
}

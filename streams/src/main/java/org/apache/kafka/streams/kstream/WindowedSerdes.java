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
        // Default constructor needed by Kafka
        public TimeWindowedSerde() {
            super(new TimeWindowedSerializer<T>(), new TimeWindowedDeserializer<T>());
        }

        public TimeWindowedSerde(final Serde<T> inner) {
            super(new TimeWindowedSerializer<>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer()));
        }
    }

    static public class SessionWindowedSerde<T> extends Serdes.WrapperSerde<Windowed<T>> {
        // Default constructor needed by Kafka
        public SessionWindowedSerde() {
            super(new SessionWindowedSerializer<T>(), new SessionWindowedDeserializer<T>());
        }

        public SessionWindowedSerde(final Serde<T> inner) {
            super(new SessionWindowedSerializer<>(inner.serializer()), new SessionWindowedDeserializer<>(inner.deserializer()));
        }
    }

    static public <T> Serde<Windowed<T>> timeWindowedSerdeFrom(final Class<T> type) {
        return new TimeWindowedSerde<>(Serdes.serdeFrom(type));
    }

    static public <T> Serde<Windowed<T>> sessionWindowedSerdeFrom(final Class<T> type) {
        return new TimeWindowedSerde<>(Serdes.serdeFrom(type));
    }
}

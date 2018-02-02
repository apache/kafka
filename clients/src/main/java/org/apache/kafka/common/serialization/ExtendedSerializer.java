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
package org.apache.kafka.common.serialization;

import java.util.Map;

import org.apache.kafka.common.header.Headers;

/**
 * A Serializer that has access to the headers associated with the record.
 *
 * Prefer {@link Serializer} if access to the headers is not required. Once Kafka drops support for Java 7, the
 * {@code serialize()} method introduced by this interface will be added to Serializer with a default implementation
 * so that backwards compatibility is maintained. This interface may be deprecated once that happens.
 *
 * A class that implements this interface is expected to have a constructor with no parameters.
 * @param <T>
 */
public interface ExtendedSerializer<T> extends Serializer<T> {

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param headers headers associated with the record
     * @param data typed data
     * @return serialized bytes
     */
    byte[] serialize(String topic, Headers headers, T data);

    class Wrapper<T> implements ExtendedSerializer<T> {

        private final Serializer<T> serializer;

        public Wrapper(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public byte[] serialize(String topic, Headers headers, T data) {
            return serialize(topic, data);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(String topic, T data) {
            return serializer.serialize(topic, data);
        }

        @Override
        public void close() {
            serializer.close();
        }

        public static <T> ExtendedSerializer<T> ensureExtended(Serializer<T> serializer) {
            return serializer == null ? null : serializer instanceof ExtendedSerializer ? (ExtendedSerializer<T>) serializer : new ExtendedSerializer.Wrapper<>(serializer);
        }
    }
}

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

import org.apache.kafka.common.header.Headers;

import java.util.Map;

/**
 * {@link PipeSerializer} solves the problem of needing to do a two step serialization. For example, you want to serialize CloudEvent and then encrypt them.
 * <p>
 * To configure:
 * <code>
 * pipe.serializer.from=com.example.CloudEventSerializer
 * pipe.serializer.to=com.example.EncryptionSerializer
 * </code>
 *
 * @param <T>
 */
public class PipeSerializer<T> implements Serializer<T> {
    private Serializer<T> from;
    private Serializer<byte[]> to;

    public PipeSerializer() {
    }

    public PipeSerializer(Serializer<T> from, Serializer<byte[]> to) {
        this.from = from;
        this.to = to;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            from = (Serializer<T>) Class.forName(String.valueOf(configs.get("pipe.serializer.from"))).getDeclaredConstructor().newInstance();
            to = (Serializer<byte[]>) Class.forName(String.valueOf(configs.get("pipe.serializer.to"))).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        from.configure(configs, isKey);
        to.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return to.serialize(topic, from.serialize(topic, data));
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return to.serialize(topic, headers, from.serialize(topic, headers, data));
    }
}

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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.util.Map;
import java.util.Objects;

public abstract class WrappingNullableSerde<T, InnerK, InnerV> implements Serde<T> {
    private final WrappingNullableSerializer<T, InnerK, InnerV> serializer;
    private final WrappingNullableDeserializer<T, InnerK, InnerV> deserializer;

    protected WrappingNullableSerde(final WrappingNullableSerializer<T, InnerK, InnerV> serializer,
                                    final WrappingNullableDeserializer<T, InnerK, InnerV> deserializer) {
        Objects.requireNonNull(serializer, "serializer can't be null");
        Objects.requireNonNull(deserializer, "deserializer can't be null");
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    public void setIfUnset(final SerdeGetter getter) {
        serializer.setIfUnset(getter);
        deserializer.setIfUnset(getter);
    }
}

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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.nio.ByteBuffer;

public class ChangedDeserializer<T> implements Deserializer<Change<T>>, WrappingNullableDeserializer<Change<T>, Void, T> {

    private static final int NEWFLAG_SIZE = 1;

    private Deserializer<T> inner;

    public ChangedDeserializer(final Deserializer<T> inner) {
        this.inner = inner;
    }

    public Deserializer<T> inner() {
        return inner;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setIfUnset(final SerdeGetter getter) {
        if (inner == null) {
            inner = (Deserializer<T>) getter.valueSerde().deserializer();
        }
    }

    @Override
    public Change<T> deserialize(final String topic, final Headers headers, final byte[] data) {

        final byte[] bytes = new byte[data.length - NEWFLAG_SIZE];

        System.arraycopy(data, 0, bytes, 0, bytes.length);

        if (ByteBuffer.wrap(data).get(data.length - NEWFLAG_SIZE) != 0) {
            return new Change<>(inner.deserialize(topic, headers, bytes), null);
        } else {
            return new Change<>(null, inner.deserialize(topic, headers, bytes));
        }
    }

    @Override
    public Change<T> deserialize(final String topic, final byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    public void close() {
        inner.close();
    }
}

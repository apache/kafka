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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;

import java.nio.ByteBuffer;
import java.util.Map;

public class ChangedSerializer<T> implements Serializer<Change<T>> {

    private static final int NEWFLAG_SIZE = 1;

    private Serializer<T> inner;

    public ChangedSerializer(final Serializer<T> inner) {
        this.inner = inner;
    }

    public Serializer<T> inner() {
        return inner;
    }

    public void setInner(final Serializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // do nothing
    }

    /**
     * @throws StreamsException if both old and new values of data are null, or if
     * both values are not null
     */
    @Override
    public byte[] serialize(final String topic, final Headers headers, final Change<T> data) {
        final byte[] serializedKey;

        // only one of the old / new values would be not null
        if (data.newValue != null) {
            if (data.oldValue != null) {
                throw new StreamsException("Both old and new values are not null (" + data.oldValue
                    + " : " + data.newValue + ") in ChangeSerializer, which is not allowed.");
            }

            serializedKey = inner.serialize(topic, headers, data.newValue);
        } else {
            if (data.oldValue == null) {
                throw new StreamsException("Both old and new values are null in ChangeSerializer, which is not allowed.");
            }

            serializedKey = inner.serialize(topic, headers, data.oldValue);
        }

        final ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + NEWFLAG_SIZE);
        buf.put(serializedKey);
        buf.put((byte) (data.newValue != null ? 1 : 0));

        return buf.array();
    }

    @Override
    public byte[] serialize(final String topic, final Change<T> data) {
        return serialize(topic, null, data);
    }

    @Override
    public void close() {
        inner.close();
    }
}

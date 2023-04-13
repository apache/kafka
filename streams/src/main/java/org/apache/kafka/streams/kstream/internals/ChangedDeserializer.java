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
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.nio.ByteBuffer;

public class ChangedDeserializer<T> implements Deserializer<Change<T>>, WrappingNullableDeserializer<Change<T>, Void, T> {

    private static final int NEW_OLD_FLAG_SIZE = 1;

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
        // The format we need to deserialize is:
        // {BYTE_ARRAY oldValue}{BYTE newOldFlag=0}
        // {BYTE_ARRAY newValue}{BYTE newOldFlag=1}
        // {UINT32 newDataLength}{BYTE_ARRAY newValue}{BYTE_ARRAY oldValue}{BYTE newOldFlag=2}
        final ByteBuffer buffer = ByteBuffer.wrap(data);
        final byte newOldFlag = buffer.get(data.length - NEW_OLD_FLAG_SIZE);

        final byte[] newData;
        final byte[] oldData;
        if (newOldFlag == (byte) 0) {
            newData = null;
            final int oldDataLength = data.length - NEW_OLD_FLAG_SIZE;
            oldData = new byte[oldDataLength];
            buffer.get(oldData);
        } else if (newOldFlag == (byte) 1) {
            oldData = null;
            final int newDataLength = data.length - NEW_OLD_FLAG_SIZE;
            newData = new byte[newDataLength];
            buffer.get(newData);
        } else if (newOldFlag == (byte) 2) {
            final int newDataLength = Math.toIntExact(ByteUtils.readUnsignedInt(buffer));
            newData = new byte[newDataLength];

            final int oldDataLength = data.length - Integer.BYTES - newDataLength - NEW_OLD_FLAG_SIZE;
            oldData = new byte[oldDataLength];

            buffer.get(newData);
            buffer.get(oldData);
        } else {
            throw new StreamsException("Encountered unknown byte value `" + newOldFlag + "` for oldNewFlag in ChangedDeserializer.");
        }

        return new Change<>(
                inner.deserialize(topic, headers, newData),
                inner.deserialize(topic, headers, oldData));
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

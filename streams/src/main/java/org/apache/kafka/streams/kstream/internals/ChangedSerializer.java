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
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.internals.UpgradeFromValues;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.nio.ByteBuffer;
import java.util.Map;

public class ChangedSerializer<T> implements Serializer<Change<T>>, WrappingNullableSerializer<Change<T>, Void, T> {

    private static final int ENCODING_FLAG_SIZE = 1;
    private static final int MAX_VARINT_LENGTH = 5;
    private Serializer<T> inner;
    private boolean isUpgrade;

    public ChangedSerializer(final Serializer<T> inner) {
        this.inner = inner;
    }

    public Serializer<T> inner() {
        return inner;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setIfUnset(final SerdeGetter getter) {
        if (inner == null) {
            inner = (Serializer<T>) getter.valueSerde().serializer();
        }
    }

    @SuppressWarnings("checkstyle:cyclomaticComplexity")
    private static boolean isUpgrade(final Map<String, ?> configs) {
        final Object upgradeFrom = configs.get(StreamsConfig.UPGRADE_FROM_CONFIG);
        if (upgradeFrom == null) {
            return false;
        }

        switch (UpgradeFromValues.getValueFromString((String) upgradeFrom)) {
            case UPGRADE_FROM_0100:
            case UPGRADE_FROM_0101:
            case UPGRADE_FROM_0102:
            case UPGRADE_FROM_0110:
            case UPGRADE_FROM_10:
            case UPGRADE_FROM_11:
            case UPGRADE_FROM_20:
            case UPGRADE_FROM_21:
            case UPGRADE_FROM_22:
            case UPGRADE_FROM_23:
            case UPGRADE_FROM_24:
            case UPGRADE_FROM_25:
            case UPGRADE_FROM_26:
            case UPGRADE_FROM_27:
            case UPGRADE_FROM_28:
            case UPGRADE_FROM_30:
            case UPGRADE_FROM_31:
            case UPGRADE_FROM_32:
            case UPGRADE_FROM_33:
            case UPGRADE_FROM_34:
                // there is no need to add new version here
                return true;
            default:
                return false;
        }
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        this.isUpgrade = isUpgrade(configs);
    }

    /**
     * @throws StreamsException if both old and new values of data are null, or if
     * both values are not null and is upgrading from a version less than 3.4
     */
    @Override
    public byte[] serialize(final String topic, final Headers headers, final Change<T> data) {
        final boolean oldValueIsNotNull = data.oldValue != null;
        final boolean newValueIsNotNull = data.newValue != null;

        final byte[] newData = inner.serialize(topic, headers, data.newValue);
        final byte[] oldData = inner.serialize(topic, headers, data.oldValue);

        final int newDataLength = newValueIsNotNull ? newData.length : 0;
        final int oldDataLength = oldValueIsNotNull ? oldData.length : 0;

        // The serialization format is:
        // {BYTE_ARRAY oldValue}{BYTE encodingFlag=0}
        // {BYTE_ARRAY newValue}{BYTE encodingFlag=1}
        // {VARINT newDataLength}{BYTE_ARRAY newValue}{BYTE_ARRAY oldValue}{BYTE encodingFlag=2}
        if (newValueIsNotNull && oldValueIsNotNull) {
            if (isUpgrade) {
                throw new StreamsException("Both old and new values are not null (" + data.oldValue
                        + " : " + data.newValue + ") in ChangeSerializer, which is not allowed unless upgrading.");
            } else {
                final int capacity = MAX_VARINT_LENGTH + newDataLength + oldDataLength + ENCODING_FLAG_SIZE;
                final ByteBuffer buf = ByteBuffer.allocate(capacity);
                ByteUtils.writeVarint(newDataLength, buf);
                buf.put(newData).put(oldData).put((byte) 2);

                final byte[] serialized = new byte[buf.position()];
                buf.position(0);
                buf.get(serialized);

                return serialized;
            }
        } else if (newValueIsNotNull) {
            final int capacity = newDataLength + ENCODING_FLAG_SIZE;
            final ByteBuffer buf = ByteBuffer.allocate(capacity);
            buf.put(newData).put((byte) 1);

            return buf.array();
        } else if (oldValueIsNotNull) {
            final int capacity = oldDataLength + ENCODING_FLAG_SIZE;
            final ByteBuffer buf = ByteBuffer.allocate(capacity);
            buf.put(oldData).put((byte) 0);

            return buf.array();
        } else {
            throw new StreamsException("Both old and new values are null in ChangeSerializer, which is not allowed.");
        }
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

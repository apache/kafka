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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;

public class ChangedSerdeTest {
    private static final String TOPIC = "some-topic";

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private static final ChangedSerializer<String> CHANGED_STRING_SERIALIZER =
            new ChangedSerializer<>(STRING_SERIALIZER);
    private static final ChangedDeserializer<String> CHANGED_STRING_DESERIALIZER =
            new ChangedDeserializer<>(Serdes.String().deserializer());

    private static final int ENCODING_FLAG_SIZE = 1;
    private static final int IS_LATEST_FLAG_SIZE = 1;
    private static final int MAX_VARINT_LENGTH = 5;

    final String nonNullNewValue = "hello";
    final String nonNullOldValue = "world";

    private static <T> void checkRoundTrip(final T data, final Serializer<T> serializer, final Deserializer<T> deserializer) {
        final byte[] serialized = serializer.serialize(TOPIC, data);
        assertThat(serialized, is(notNullValue()));
        final T deserialized = deserializer.deserialize(TOPIC, serialized);
        assertThat(deserialized, is(data));
    }

    @Test
    public void serializerShouldThrowIfGivenAChangeWithBothNewAndOldValuesAsNull() {
        final Change<String> data = new Change<>(null, null);

        assertThrows(
                StreamsException.class,
                () -> CHANGED_STRING_SERIALIZER.serialize(TOPIC, data));
    }

    @Test
    public void serializerShouldThrowIfGivenAChangeWithBothNonNullNewAndOldValuesAndIsUpgrade() {
        final Change<String> data = new Change<>(nonNullNewValue, nonNullOldValue);
        final ChangedSerializer<String> serializer = new ChangedSerializer<>(Serdes.String().serializer());
        final Map<String, String> configs = Collections.singletonMap(StreamsConfig.UPGRADE_FROM_CONFIG, StreamsConfig.UPGRADE_FROM_33);
        serializer.configure(configs, false);

        assertThrows(
                StreamsException.class,
                () -> serializer.serialize(TOPIC, data));
    }

    @Test
    public void shouldSerializeAndDeserializeChangeWithNonNullNewAndOldValues() {
        final Change<String> data = new Change<>(nonNullNewValue, nonNullOldValue);
        checkRoundTrip(data, CHANGED_STRING_SERIALIZER, CHANGED_STRING_DESERIALIZER);
    }

    @Test
    public void shouldSerializeAndDeserializeChangeWithNonNullNewValueAndNullOldValue() {
        final Change<String> data = new Change<>(nonNullNewValue, null);
        checkRoundTrip(data, CHANGED_STRING_SERIALIZER, CHANGED_STRING_DESERIALIZER);
    }

    @Test
    public void shouldSerializeAndDeserializeChangeWithNullNewValueAndNonNullOldValue() {
        final Change<String> data = new Change<>(null, nonNullOldValue);
        checkRoundTrip(data, CHANGED_STRING_SERIALIZER, CHANGED_STRING_DESERIALIZER);
    }

    @Test
    public void shouldThrowErrorIfEncountersAnUnknownByteValueForOldNewFlag() {
        final Change<String> data = new Change<>(null, nonNullOldValue);
        final byte[] serialized = CHANGED_STRING_SERIALIZER.serialize(TOPIC, data);
        assertThat(serialized, is(notNullValue()));

        // mutate the serialized array to replace OLD_NEW_FLAG with an unsupported byte value
        final ByteBuffer buffer = ByteBuffer.wrap(serialized);
        buffer.position(serialized.length - 1);
        buffer.put((byte) -1);

        Assert.assertThrows(
            StreamsException.class,
            () -> CHANGED_STRING_DESERIALIZER.deserialize(TOPIC, serialized));
    }

    @Test
    public void shouldDeserializeReservedVersions3Through5() {
        // `isLatest = true`
        checkRoundTripForReservedVersion(new Change<>(nonNullNewValue, null, true));
        checkRoundTripForReservedVersion(new Change<>(null, nonNullOldValue, true));
        checkRoundTripForReservedVersion(new Change<>(nonNullNewValue, nonNullOldValue, true));

        // `isLatest = false`
        checkRoundTripForReservedVersion(new Change<>(nonNullNewValue, null, false));
        checkRoundTripForReservedVersion(new Change<>(null, nonNullOldValue, false));
        checkRoundTripForReservedVersion(new Change<>(nonNullNewValue, nonNullOldValue, false));
    }

    // versions 3 through 5 are reserved in the deserializer in case we want to use them in the
    // future (in which case we save users from needing to perform another rolling upgrade by
    // introducing these reserved versions in the same AK release as version 2).
    // so, this serialization code is not actually in the serializer itself, but only here for
    // now for purposes of testing the deserializer.
    private static byte[] serializeVersions3Through5(final String topic, final Change<String> data) {
        final boolean oldValueIsNotNull = data.oldValue != null;
        final boolean newValueIsNotNull = data.newValue != null;

        final byte[] newData = STRING_SERIALIZER.serialize(topic, null, data.newValue);
        final byte[] oldData = STRING_SERIALIZER.serialize(topic, null, data.oldValue);

        final int newDataLength = newValueIsNotNull ? newData.length : 0;
        final int oldDataLength = oldValueIsNotNull ? oldData.length : 0;

        // The serialization format is:
        // {BYTE_ARRAY oldValue}{BYTE isLatest}{BYTE encodingFlag=3}
        // {BYTE_ARRAY newValue}{BYTE isLatest}{BYTE encodingFlag=4}
        // {VARINT newDataLength}{BYTE_ARRAY newValue}{BYTE_ARRAY oldValue}{BYTE isLatest}{BYTE encodingFlag=5}
        final ByteBuffer buf;
        final byte isLatest = data.isLatest ? (byte) 1 : (byte) 0;
        if (newValueIsNotNull && oldValueIsNotNull) {
            final int capacity = MAX_VARINT_LENGTH + newDataLength + oldDataLength + IS_LATEST_FLAG_SIZE + ENCODING_FLAG_SIZE;
            buf = ByteBuffer.allocate(capacity);
            ByteUtils.writeVarint(newDataLength, buf);
            buf.put(newData).put(oldData).put(isLatest).put((byte) 5);
        } else if (newValueIsNotNull) {
            final int capacity = newDataLength + IS_LATEST_FLAG_SIZE + ENCODING_FLAG_SIZE;
            buf = ByteBuffer.allocate(capacity);
            buf.put(newData).put(isLatest).put((byte) 4);
        } else if (oldValueIsNotNull) {
            final int capacity = oldDataLength + IS_LATEST_FLAG_SIZE + ENCODING_FLAG_SIZE;
            buf = ByteBuffer.allocate(capacity);
            buf.put(oldData).put(isLatest).put((byte) 3);
        } else {
            throw new StreamsException("Both old and new values are null in ChangeSerializer, which is not allowed.");
        }

        final byte[] serialized = new byte[buf.position()];
        buf.position(0);
        buf.get(serialized);

        return serialized;
    }

    private static void checkRoundTripForReservedVersion(final Change<String> data) {
        final byte[] serialized = serializeVersions3Through5(TOPIC, data);
        assertThat(serialized, is(notNullValue()));
        final Change<String> deserialized = CHANGED_STRING_DESERIALIZER.deserialize(TOPIC, serialized);
        assertThat(deserialized, is(data));
    }
}

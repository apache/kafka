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

    private static final ChangedSerializer<String> CHANGED_STRING_SERIALIZER =
            new ChangedSerializer<>(Serdes.String().serializer());

    private static final ChangedDeserializer<String> CHANGED_STRING_DESERIALIZER =
            new ChangedDeserializer<>(Serdes.String().deserializer());

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
}

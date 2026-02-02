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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("unchecked")
public class StateSerdesTest {

    @Test
    public void shouldThrowIfTopicNameIsNullForBuiltinTypes() {
        assertThrows(NullPointerException.class, () -> StateSerdes.withBuiltinTypes(null, byte[].class, byte[].class));
    }

    @Test
    public void shouldThrowIfKeyClassIsNullForBuiltinTypes() {
        assertThrows(NullPointerException.class, () -> StateSerdes.withBuiltinTypes("anyName", null, byte[].class));
    }

    @Test
    public void shouldThrowIfValueClassIsNullForBuiltinTypes() {
        assertThrows(NullPointerException.class, () -> StateSerdes.withBuiltinTypes("anyName", byte[].class, null));
    }

    @Test
    public void shouldReturnSerdesForBuiltInKeyAndValueTypesForBuiltinTypes() {
        final Class[] supportedBuildInTypes = new Class[] {
            String.class,
            Short.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class,
            byte[].class,
            ByteBuffer.class,
            Bytes.class
        };

        for (final Class keyClass : supportedBuildInTypes) {
            for (final Class valueClass : supportedBuildInTypes) {
                assertNotNull(StateSerdes.withBuiltinTypes("anyName", keyClass, valueClass));
            }
        }
    }

    @Test
    public void shouldThrowForUnknownKeyTypeForBuiltinTypes() {
        assertThrows(IllegalArgumentException.class, () -> StateSerdes.withBuiltinTypes("anyName", Class.class, byte[].class));
    }

    @Test
    public void shouldThrowForUnknownValueTypeForBuiltinTypes() {
        assertThrows(IllegalArgumentException.class, () -> StateSerdes.withBuiltinTypes("anyName", byte[].class, Class.class));
    }

    @Test
    public void shouldThrowIfTopicNameIsNull() {
        assertThrows(NullPointerException.class, () -> new StateSerdes<>(null, Serdes.ByteArray(), Serdes.ByteArray()));
    }

    @Test
    public void shouldThrowIfKeyClassIsNull() {
        assertThrows(NullPointerException.class, () -> new StateSerdes<>("anyName", null, Serdes.ByteArray()));
    }

    @Test
    public void shouldThrowIfValueClassIsNull() {
        assertThrows(NullPointerException.class, () -> new StateSerdes<>("anyName", Serdes.ByteArray(), null));
    }

    @Test
    public void shouldThrowIfIncompatibleSerdeForValue() throws ClassNotFoundException {
        final Class myClass = Class.forName("java.lang.String");
        final StateSerdes<Object, Object> stateSerdes = new StateSerdes<Object, Object>("anyName", Serdes.serdeFrom(myClass), Serdes.serdeFrom(myClass));
        final Integer myInt = 123;
        final Exception e = assertThrows(StreamsException.class, () -> stateSerdes.rawValue(myInt));
        assertThat(
            e.getMessage(),
            equalTo(
                "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                "is not compatible to the actual value type (value type: java.lang.Integer). " +
                "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
    }

    @Test
    public void shouldSkipValueAndTimestampeInformationForErrorOnTimestampAndValueSerialization() throws ClassNotFoundException {
        final Class myClass = Class.forName("java.lang.String");
        final StateSerdes<Object, Object> stateSerdes =
            new StateSerdes<Object, Object>("anyName", Serdes.serdeFrom(myClass), new ValueAndTimestampSerde(Serdes.serdeFrom(myClass)));
        final Integer myInt = 123;
        final Exception e = assertThrows(StreamsException.class, () -> stateSerdes.rawValue(ValueAndTimestamp.make(myInt, 0L)));
        assertThat(
            e.getMessage(),
            equalTo(
                "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                    "is not compatible to the actual value type (value type: java.lang.Integer). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
    }

    @Test
    public void shouldThrowIfIncompatibleSerdeForKey() throws ClassNotFoundException {
        final Class myClass = Class.forName("java.lang.String");
        final StateSerdes<Object, Object> stateSerdes = new StateSerdes<Object, Object>("anyName", Serdes.serdeFrom(myClass), Serdes.serdeFrom(myClass));
        final Integer myInt = 123;
        final Exception e = assertThrows(StreamsException.class, () -> stateSerdes.rawKey(myInt));
        assertThat(
            e.getMessage(),
            equalTo(
                "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                    "is not compatible to the actual key type (key type: java.lang.Integer). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
    }

    @Test
    public void shouldSerializeAndDeserializeKeyWithHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());
        final Headers headers = new RecordHeaders()
            .add("header-key", "header-value".getBytes());

        final String key = "test-key";
        final byte[] serialized = stateSerdes.rawKey(key, headers);
        final String deserialized = stateSerdes.keyFrom(serialized, headers);

        assertEquals(key, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeValueWithHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());
        final Headers headers = new RecordHeaders()
            .add("header-key", "header-value".getBytes());

        final String value = "test-value";
        final byte[] serialized = stateSerdes.rawValue(value, headers);
        final String deserialized = stateSerdes.valueFrom(serialized, headers);

        assertEquals(value, deserialized);
    }

    @Test
    public void shouldSerializeKeyWithNullHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());

        final String key = "test-key";
        final byte[] serializedWithNull = stateSerdes.rawKey(key, null);
        final byte[] serializedWithoutHeaders = stateSerdes.rawKey(key);

        assertArrayEquals(serializedWithoutHeaders, serializedWithNull);
    }

    @Test
    public void shouldSerializeValueWithNullHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());

        final String value = "test-value";
        final byte[] serializedWithNull = stateSerdes.rawValue(value, null);
        final byte[] serializedWithoutHeaders = stateSerdes.rawValue(value);

        assertArrayEquals(serializedWithoutHeaders, serializedWithNull);
    }

    @Test
    public void shouldDeserializeKeyWithNullHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());

        final String key = "test-key";
        final byte[] serialized = stateSerdes.rawKey(key);

        final String deserializedWithNull = stateSerdes.keyFrom(serialized, null);
        final String deserializedWithoutHeaders = stateSerdes.keyFrom(serialized);

        assertEquals(deserializedWithoutHeaders, deserializedWithNull);
    }

    @Test
    public void shouldDeserializeValueWithNullHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());

        final String value = "test-value";
        final byte[] serialized = stateSerdes.rawValue(value);

        final String deserializedWithNull = stateSerdes.valueFrom(serialized, null);
        final String deserializedWithoutHeaders = stateSerdes.valueFrom(serialized);

        assertEquals(deserializedWithoutHeaders, deserializedWithNull);
    }

    @Test
    public void shouldSerializeKeyWithEmptyHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());
        final Headers emptyHeaders = new RecordHeaders();

        final String key = "test-key";
        final byte[] serialized = stateSerdes.rawKey(key, emptyHeaders);

        assertNotNull(serialized);
    }

    @Test
    public void shouldSerializeValueWithEmptyHeaders() {
        final StateSerdes<String, String> stateSerdes =
            new StateSerdes<>("test-topic", Serdes.String(), Serdes.String());
        final Headers emptyHeaders = new RecordHeaders();

        final String value = "test-value";
        final byte[] serialized = stateSerdes.rawValue(value, emptyHeaders);

        assertNotNull(serialized);
    }

    @Test
    public void shouldThrowIfIncompatibleSerdeForKeyWithHeaders() throws ClassNotFoundException {
        final Class myClass = Class.forName("java.lang.String");
        final StateSerdes<Object, Object> stateSerdes =
            new StateSerdes<Object, Object>("anyName", Serdes.serdeFrom(myClass), Serdes.serdeFrom(myClass));
        final Integer myInt = 123;
        final Headers headers = new RecordHeaders().add("key", "value".getBytes());

        final Exception e = assertThrows(StreamsException.class, () -> stateSerdes.rawKey(myInt, headers));
        assertThat(
            e.getMessage(),
            equalTo(
                "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                    "is not compatible to the actual key type (key type: java.lang.Integer). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
    }

    @Test
    public void shouldThrowIfIncompatibleSerdeForValueWithHeaders() throws ClassNotFoundException {
        final Class myClass = Class.forName("java.lang.String");
        final StateSerdes<Object, Object> stateSerdes =
            new StateSerdes<Object, Object>("anyName", Serdes.serdeFrom(myClass), Serdes.serdeFrom(myClass));
        final Integer myInt = 123;
        final Headers headers = new RecordHeaders().add("key", "value".getBytes());

        final Exception e = assertThrows(StreamsException.class, () -> stateSerdes.rawValue(myInt, headers));
        assertThat(
            e.getMessage(),
            equalTo(
                "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                    "is not compatible to the actual value type (value type: java.lang.Integer). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
    }

}

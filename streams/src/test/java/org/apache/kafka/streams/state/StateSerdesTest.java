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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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

}

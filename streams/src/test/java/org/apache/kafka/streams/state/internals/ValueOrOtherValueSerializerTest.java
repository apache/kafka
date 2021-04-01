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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;

public class ValueOrOtherValueSerializerTest {
    private static final String TOPIC = "some-topic";

    private static final ValueOrOtherValueSerde<String, Integer> STRING_OR_INTEGER_SERDE =
        new ValueOrOtherValueSerde<>(Serdes.String(), Serdes.Integer());

    @Test
    public void shouldSerializeStringValue() {
        final String value = "some-string";

        final ValueOrOtherValue<String, Integer> valueOrOtherValue = ValueOrOtherValue.makeValue(value);

        final byte[] serialized =
            STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, valueOrOtherValue);

        assertThat(serialized, is(notNullValue()));

        final ValueOrOtherValue<String, Integer> deserialized =
            STRING_OR_INTEGER_SERDE.deserializer().deserialize(TOPIC, serialized);

        assertThat(deserialized, is(valueOrOtherValue));
    }

    @Test
    public void shouldSerializeIntegerValue() {
        final int value = 5;

        final ValueOrOtherValue<String, Integer> valueOrOtherValue = ValueOrOtherValue.makeOtherValue(value);

        final byte[] serialized =
            STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, valueOrOtherValue);

        assertThat(serialized, is(notNullValue()));

        final ValueOrOtherValue<String, Integer> deserialized =
            STRING_OR_INTEGER_SERDE.deserializer().deserialize(TOPIC, serialized);

        assertThat(deserialized, is(valueOrOtherValue));
    }

    @Test
    public void shouldThrowIfSerializeValueAsNull() {
        assertThrows(NullPointerException.class,
            () -> STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, ValueOrOtherValue.makeValue(null)));
    }

    @Test
    public void shouldThrowIfSerializeOtherValueAsNull() {
        assertThrows(NullPointerException.class,
            () -> STRING_OR_INTEGER_SERDE.serializer().serialize(TOPIC, ValueOrOtherValue.makeOtherValue(null)));
    }
}

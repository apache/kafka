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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NullableValueAndTimestampSerdeTest {

    private final static NullableValueAndTimestampSerde<String> SERDE = new NullableValueAndTimestampSerde<>(new StringSerde());
    private final static Serializer<ValueAndTimestamp<String>> SERIALIZER = SERDE.serializer();
    private final static Deserializer<ValueAndTimestamp<String>> DESERIALIZER = SERDE.deserializer();

    @Test
    public void shouldSerdeNull() {
        assertThat(SERIALIZER.serialize(null, null), is(nullValue()));
        assertThat(DESERIALIZER.deserialize(null, null), is(nullValue()));
    }

    @Test
    public void shouldSerdeNonNull() {
        final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make("foo", 10L);

        final byte[] rawValueAndTimestamp = SERIALIZER.serialize(null, valueAndTimestamp);
        assertThat(rawValueAndTimestamp, is(notNullValue()));

        assertThat(DESERIALIZER.deserialize(null, rawValueAndTimestamp), is(valueAndTimestamp));
    }

    @Test
    public void shouldSerdeNonNullWithNullValue() {
        final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.makeAllowNullable(null, 10L);

        final byte[] rawValueAndTimestamp = SERIALIZER.serialize(null, valueAndTimestamp);
        assertThat(rawValueAndTimestamp, is(notNullValue()));

        assertThat(DESERIALIZER.deserialize(null, rawValueAndTimestamp), is(valueAndTimestamp));
    }

    @Test
    public void shouldSerializeNonNullWithEmptyBytes() {
        // empty string serializes to empty bytes
        final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make("", 10L);

        final byte[] rawValueAndTimestamp = SERIALIZER.serialize(null, valueAndTimestamp);
        assertThat(rawValueAndTimestamp, is(notNullValue()));

        assertThat(DESERIALIZER.deserialize(null, rawValueAndTimestamp), is(valueAndTimestamp));
    }
}
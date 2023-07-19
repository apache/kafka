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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValueAndTimestampSerializerTest {
    private static final String TOPIC = "some-topic";
    private static final long TIMESTAMP = 23;

    private static final ValueAndTimestampSerde<String> STRING_SERDE =
            new ValueAndTimestampSerde<>(Serdes.String());

    @Test
    public void shouldSerializeNonNullDataUsingTheInternalSerializer() {
        final String value = "some-string";

        final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(value, TIMESTAMP);

        final byte[] serialized =
                STRING_SERDE.serializer().serialize(TOPIC, valueAndTimestamp);

        assertThat(serialized, is(notNullValue()));

        final ValueAndTimestamp<String> deserialized =
                STRING_SERDE.deserializer().deserialize(TOPIC, serialized);

        assertThat(deserialized, is(valueAndTimestamp));
    }

    @Test
    public void shouldDropSerializedValueIfEqualWithGreaterTimestamp() {
        final String value = "food";

        final ValueAndTimestamp<String> oldValueAndTimestamp = ValueAndTimestamp.make(value, TIMESTAMP);
        final byte[] oldSerializedValue = STRING_SERDE.serializer().serialize(TOPIC, oldValueAndTimestamp);
        final ValueAndTimestamp<String> newValueAndTimestamp = ValueAndTimestamp.make(value, TIMESTAMP + 1);
        final byte[] newSerializedValue = STRING_SERDE.serializer().serialize(TOPIC, newValueAndTimestamp);
        assertTrue(ValueAndTimestampSerializer.valuesAreSameAndTimeIsIncreasing(oldSerializedValue, newSerializedValue));
    }

    @Test
    public void shouldKeepSerializedValueIfOutOfOrder() {
        final String value = "balls";

        final ValueAndTimestamp<String> oldValueAndTimestamp = ValueAndTimestamp.make(value, TIMESTAMP);
        final byte[] oldSerializedValue = STRING_SERDE.serializer().serialize(TOPIC, oldValueAndTimestamp);
        final ValueAndTimestamp<String> outOfOrderValueAndTimestamp = ValueAndTimestamp.make(value, TIMESTAMP - 1);
        final byte[] outOfOrderSerializedValue = STRING_SERDE.serializer().serialize(TOPIC, outOfOrderValueAndTimestamp);
        assertFalse(ValueAndTimestampSerializer.valuesAreSameAndTimeIsIncreasing(oldSerializedValue, outOfOrderSerializedValue));
    }

    @Test
    public void shouldSerializeNullDataAsNull() {
        final byte[] serialized =
                STRING_SERDE.serializer().serialize(TOPIC, ValueAndTimestamp.make(null, TIMESTAMP));

        assertThat(serialized, is(nullValue()));
    }

    @Test
    public void shouldReturnNullWhenTheInternalSerializerReturnsNull() {
        // Testing against regressions with respect to https://github.com/apache/kafka/pull/7679

        final Serializer<String> alwaysNullSerializer = (topic, data) -> null;

        final ValueAndTimestampSerializer<String> serializer =
                new ValueAndTimestampSerializer<>(alwaysNullSerializer);

        final byte[] serialized = serializer.serialize(TOPIC, "non-null-data", TIMESTAMP);

        assertThat(serialized, is(nullValue()));
    }
}

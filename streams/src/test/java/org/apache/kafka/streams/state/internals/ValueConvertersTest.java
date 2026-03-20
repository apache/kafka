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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ValueConvertersTest {

    @Test
    public void extractValueShouldReturnPlainValue() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();

        final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make("value", 42L);
        assertThat(converter.apply(valueAndTimestamp), is("value"));
    }

    @Test
    public void extractValueShouldReturnNullWhenInputIsNull() {
        final Function<ValueAndTimestamp<String>, String> converter = ValueConverters.extractValue();

        assertNull(converter.apply(null));
    }

    @Test
    public void extractValueFromHeadersShouldReturnPlainValue() {
        final Function<ValueTimestampHeaders<String>, String> converter = ValueConverters.extractValueFromHeaders();

        final ValueTimestampHeaders<String> vth = ValueTimestampHeaders.make("value", 42L, new RecordHeaders());
        assertThat(converter.apply(vth), is("value"));
    }

    @Test
    public void extractValueFromHeadersShouldReturnNullWhenInputIsNull() {
        final Function<ValueTimestampHeaders<String>, String> converter = ValueConverters.extractValueFromHeaders();

        assertNull(converter.apply(null));
    }

    @Test
    public void headersToValueAndTimestampShouldConvertCorrectly() {
        final Function<ValueTimestampHeaders<String>, ValueAndTimestamp<String>> converter =
            ValueConverters.extractValueAndTimestampFromHeaders();

        final ValueTimestampHeaders<String> vth = ValueTimestampHeaders.make("value", 42L, new RecordHeaders());
        final ValueAndTimestamp<String> result = converter.apply(vth);

        assertThat(result.value(), is("value"));
        assertThat(result.timestamp(), is(42L));
    }

    @Test
    public void headersToValueAndTimestampShouldReturnNullWhenInputIsNull() {
        final Function<ValueTimestampHeaders<String>, ValueAndTimestamp<String>> converter =
            ValueConverters.extractValueAndTimestampFromHeaders();

        assertNull(converter.apply(null));
    }

    @Test
    public void headersToValueAndTimestampShouldDiscardHeaders() {
        final Function<ValueTimestampHeaders<String>, ValueAndTimestamp<String>> converter =
            ValueConverters.extractValueAndTimestampFromHeaders();

        final RecordHeaders headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final ValueTimestampHeaders<String> vth = ValueTimestampHeaders.make("value", 42L, headers);

        final ValueAndTimestamp<String> result = converter.apply(vth);

        // Should only have value and timestamp, headers are discarded
        assertThat(result.value(), is("value"));
        assertThat(result.timestamp(), is(42L));
    }
}
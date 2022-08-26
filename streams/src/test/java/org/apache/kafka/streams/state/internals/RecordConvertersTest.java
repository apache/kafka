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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToTimestampedValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class RecordConvertersTest {

    private final RecordConverter timestampedValueConverter = rawValueToTimestampedValue();

    @Test
    public void shouldPreserveNullValueOnConversion() {
        final ConsumerRecord<byte[], byte[]> nullValueRecord = new ConsumerRecord<>("", 0, 0L, new byte[0], null);
        assertNull(timestampedValueConverter.convert(nullValueRecord).value());
    }

    @Test
    public void shouldAddTimestampToValueOnConversionWhenValueIsNotNull() {
        final long timestamp = 10L;
        final byte[] value = new byte[1];
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
                "topic", 1, 0, timestamp, TimestampType.CREATE_TIME, 0, 0, new byte[0], value,
                new RecordHeaders(), Optional.empty());
        final byte[] expectedValue = ByteBuffer.allocate(9).putLong(timestamp).put(value).array();
        final byte[] actualValue = timestampedValueConverter.convert(inputRecord).value();
        assertArrayEquals(expectedValue, actualValue);
    }
}

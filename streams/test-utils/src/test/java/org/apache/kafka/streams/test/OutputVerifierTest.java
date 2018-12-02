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
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class OutputVerifierTest {
    private final byte[] key = new byte[0];
    private  final byte[] value = new byte[0];

    private final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
        "someTopic",
        Integer.MAX_VALUE,
        Long.MAX_VALUE,
        key,
        value
    );

    private final ProducerRecord<byte[], byte[]> nullKeyValueRecord = new ProducerRecord<byte[], byte[]>(
        "someTopic",
        Integer.MAX_VALUE,
        Long.MAX_VALUE,
        null,
        null
    );

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordForCompareValue() {
        OutputVerifier.compareValue(null, value);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareValue() {
        OutputVerifier.compareValue((ProducerRecord<byte[], byte[]>) null, producerRecord);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullExpectedRecordForCompareValue() {
        OutputVerifier.compareValue(producerRecord, (ProducerRecord<byte[], byte[]>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordForCompareKeyValue() {
        OutputVerifier.compareKeyValue(null, key, value);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareKeyValue() {
        OutputVerifier.compareKeyValue(null, producerRecord);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullExpectedRecordForCompareKeyValue() {
        OutputVerifier.compareKeyValue(producerRecord, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(null, value, 0L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(null, producerRecord);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullExpectedRecordForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(producerRecord, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(null, key, value, 0L);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(null, producerRecord);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullExpectedRecordForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, null);
    }

    @Test
    public void shouldPassIfValueIsEqualForCompareValue() {
        OutputVerifier.compareValue(producerRecord, value);
    }

    @Test
    public void shouldPassIfValueIsEqualWithNullForCompareValue() {
        OutputVerifier.compareValue(nullKeyValueRecord, (byte[]) null);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareValue() {
        OutputVerifier.compareValue(producerRecord, key);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareValue() {
        OutputVerifier.compareValue(producerRecord, (byte[]) null);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValue() {
        OutputVerifier.compareValue(nullKeyValueRecord, value);
    }

    @Test
    public void shouldPassIfValueIsEqualForCompareValueWithProducerRecord() {
        OutputVerifier.compareValue(producerRecord, new ProducerRecord<>(
            "otherTopic",
            0,
            0L,
            value,
            value
        ));
    }

    @Test
    public void shouldPassIfValueIsEqualWithNullForCompareValueWithProducerRecord() {
        OutputVerifier.compareValue(nullKeyValueRecord, new ProducerRecord<byte[], byte[]>(
            "otherTopic",
            0,
            0L,
            value,
            null
        ));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareValueWithProducerRecord() {
        OutputVerifier.compareValue(producerRecord, new ProducerRecord<>(
            "sameTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            value,
            key
        ));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareValueWithProducerRecord() {
        OutputVerifier.compareValue(producerRecord, new ProducerRecord<byte[], byte[]>(
            "sameTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            value,
            null
        ));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValueWithProducerRecord() {
        OutputVerifier.compareValue(nullKeyValueRecord, new ProducerRecord<>(
            "sameTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            value,
            value
        ));
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualForCompareKeyValue() {
        OutputVerifier.compareKeyValue(producerRecord, key, value);
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualWithNullForCompareKeyValue() {
        OutputVerifier.compareKeyValue(nullKeyValueRecord, null, null);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentForCompareKeyValue() {
        OutputVerifier.compareKeyValue(producerRecord, value, value);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValue() {
        OutputVerifier.compareKeyValue(producerRecord, null, value);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValue() {
        OutputVerifier.compareKeyValue(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                null,
                value),
            key,
            value);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareKeyValue() {
        OutputVerifier.compareKeyValue(producerRecord, key, key);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValue() {
        OutputVerifier.compareKeyValue(producerRecord, key, null);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValue() {
        OutputVerifier.compareKeyValue(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                key,
                null),
            key,
            value);
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(producerRecord, new ProducerRecord<>(
            "otherTopic",
            0,
            0L,
            key,
            value));
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualWithNullForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(nullKeyValueRecord, new ProducerRecord<byte[], byte[]>(
            "otherTopic",
            0,
            0L,
            null,
            null));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(producerRecord, new ProducerRecord<>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            value,
            value));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(producerRecord, new ProducerRecord<byte[], byte[]>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            null,
            value));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                null,
                value),
            producerRecord);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(producerRecord, new ProducerRecord<>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            key,
            key));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(producerRecord, new ProducerRecord<byte[], byte[]>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            key,
            null));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValueWithProducerRecord() {
        OutputVerifier.compareKeyValue(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                key,
                null),
            producerRecord);
    }

    @Test
    public void shouldPassIfValueAndTimestampIsEqualForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(producerRecord, value, Long.MAX_VALUE);
    }

    @Test
    public void shouldPassIfValueAndTimestampIsEqualWithNullForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(nullKeyValueRecord, null, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(producerRecord, key, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(producerRecord, null, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(nullKeyValueRecord, value, Long.MAX_VALUE);
    }

    @Test
    public void shouldPassIfValueAndTimestampIsEqualForCompareValueTimestampWithProducerRecord() {
        OutputVerifier.compareValueTimestamp(producerRecord, new ProducerRecord<>(
            "otherTopic",
            0,
            Long.MAX_VALUE,
            value,
            value
        ));
    }

    @Test
    public void shouldPassIfValueAndTimestampIsEqualWithNullForCompareValueTimestampWithProducerRecord() {
        OutputVerifier.compareValueTimestamp(nullKeyValueRecord, new ProducerRecord<byte[], byte[]>(
            "otherTopic",
            0,
            Long.MAX_VALUE,
            value,
            null
        ));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareValueTimestampWithProducerRecord() {
        OutputVerifier.compareValueTimestamp(producerRecord, new ProducerRecord<>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            key,
            key
        ));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareValueTimestampWithProducerRecord() {
        OutputVerifier.compareValueTimestamp(producerRecord, new ProducerRecord<byte[], byte[]>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            key,
            null
        ));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValueTimestampWithProducerRecord() {
        OutputVerifier.compareValueTimestamp(nullKeyValueRecord, new ProducerRecord<>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            key,
            value
        ));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfTimestampIsDifferentForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(producerRecord, value, 0);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfTimestampDifferentWithNullReverseForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(nullKeyValueRecord, null, 0);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfTimestampIsDifferentForCompareValueTimestampWithProducerRecord() {
        OutputVerifier.compareValueTimestamp(producerRecord, new ProducerRecord<>(
            "someTopic",
            Integer.MAX_VALUE,
            0L,
            key,
            value
        ));
    }




















    @Test
    public void shouldPassIfKeyAndValueAndTimestampIsEqualForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, key, value, Long.MAX_VALUE);
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualWithNullForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(nullKeyValueRecord, null, null, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, value, value, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, null, value, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                null,
                value),
            key,
            value,
            Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, key, key, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, key, null, Long.MAX_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                key,
                null),
            key,
            value,
            Long.MAX_VALUE);
    }

    @Test
    public void shouldPassIfKeyAndValueAndTimestampIsEqualForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, new ProducerRecord<>(
            "otherTopic",
            0,
            Long.MAX_VALUE,
            key,
            value));
    }

    @Test
    public void shouldPassIfKeyAndValueAndTimestampIsEqualWithNullForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(nullKeyValueRecord, new ProducerRecord<byte[], byte[]>(
            "otherTopic",
            0,
            Long.MAX_VALUE,
            null,
            null));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, new ProducerRecord<>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            value,
            value));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, new ProducerRecord<byte[], byte[]>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            null,
            value));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                null,
                value),
            producerRecord);
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, new ProducerRecord<>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            key,
            key));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, new ProducerRecord<byte[], byte[]>(
            "someTopic",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            key,
            null));
    }

    @Test(expected = AssertionError.class)
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<byte[], byte[]>(
                "someTopic",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                key,
                null),
            producerRecord);
    }

}

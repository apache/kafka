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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Deprecated
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

    @Test
    public void shouldNotAllowNullProducerRecordForCompareValue() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareValue(null, value));
    }

    @Test
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareValue() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareValue((ProducerRecord<byte[], byte[]>) null, producerRecord));
    }

    @Test
    public void shouldNotAllowNullExpectedRecordForCompareValue() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareValue(producerRecord, (ProducerRecord<byte[], byte[]>) null));
    }

    @Test
    public void shouldNotAllowNullProducerRecordForCompareKeyValue() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareKeyValue(null, key, value));
    }

    @Test
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareKeyValue() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareKeyValue(null, producerRecord));
    }

    @Test
    public void shouldNotAllowNullExpectedRecordForCompareKeyValue() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareKeyValue(producerRecord, null));
    }

    @Test
    public void shouldNotAllowNullProducerRecordForCompareValueTimestamp() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareValueTimestamp(null, value, 0L));
    }

    @Test
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareValueTimestamp() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareValueTimestamp(null, producerRecord));
    }

    @Test
    public void shouldNotAllowNullExpectedRecordForCompareValueTimestamp() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareValueTimestamp(producerRecord, null));
    }

    @Test
    public void shouldNotAllowNullProducerRecordForCompareKeyValueTimestamp() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareKeyValueTimestamp(null, key, value, 0L));
    }

    @Test
    public void shouldNotAllowNullProducerRecordWithExpectedRecordForCompareKeyValueTimestamp() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareKeyValueTimestamp(null, producerRecord));
    }

    @Test
    public void shouldNotAllowNullExpectedRecordForCompareKeyValueTimestamp() {
        assertThrows(NullPointerException.class, () -> OutputVerifier.compareKeyValueTimestamp(producerRecord, null));
    }

    @Test
    public void shouldPassIfValueIsEqualForCompareValue() {
        OutputVerifier.compareValue(producerRecord, value);
    }

    @Test
    public void shouldPassIfValueIsEqualWithNullForCompareValue() {
        OutputVerifier.compareValue(nullKeyValueRecord, (byte[]) null);
    }

    @Test
    public void shouldFailIfValueIsDifferentForCompareValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValue(producerRecord, key));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValue(producerRecord, (byte[]) null));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValue(nullKeyValueRecord, value));
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

    @Test
    public void shouldFailIfValueIsDifferentForCompareValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValue(producerRecord,
            new ProducerRecord<>("sameTopic", Integer.MAX_VALUE, Long.MAX_VALUE, value, key)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValue(producerRecord,
            new ProducerRecord<byte[], byte[]>("sameTopic", Integer.MAX_VALUE, Long.MAX_VALUE, value, null)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValue(nullKeyValueRecord,
            new ProducerRecord<>("sameTopic", Integer.MAX_VALUE, Long.MAX_VALUE, value, value)));
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualForCompareKeyValue() {
        OutputVerifier.compareKeyValue(producerRecord, key, value);
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualWithNullForCompareKeyValue() {
        OutputVerifier.compareKeyValue(nullKeyValueRecord, null, null);
    }

    @Test
    public void shouldFailIfKeyIsDifferentForCompareKeyValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord, value, value));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord, null, value));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, null, value), key, value));
    }

    @Test
    public void shouldFailIfValueIsDifferentForCompareKeyValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord, key, key));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord, key, null));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValue() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(
            new ProducerRecord<byte[], byte[]>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, null), key, value));
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

    @Test
    public void shouldFailIfKeyIsDifferentForCompareKeyValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, value, value)));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, null, value)));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, null, value), producerRecord));
    }

    @Test
    public void shouldFailIfValueIsDifferentForCompareKeyValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, key)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(producerRecord,
            new ProducerRecord<byte[], byte[]>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, null)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValueWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValue(
            new ProducerRecord<byte[], byte[]>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, null),
                producerRecord));
    }

    @Test
    public void shouldPassIfValueAndTimestampIsEqualForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(producerRecord, value, Long.MAX_VALUE);
    }

    @Test
    public void shouldPassIfValueAndTimestampIsEqualWithNullForCompareValueTimestamp() {
        OutputVerifier.compareValueTimestamp(nullKeyValueRecord, null, Long.MAX_VALUE);
    }

    @Test
    public void shouldFailIfValueIsDifferentForCompareValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareValueTimestamp(producerRecord, key, Long.MAX_VALUE));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareValueTimestamp(producerRecord, null, Long.MAX_VALUE));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareValueTimestamp(nullKeyValueRecord, value, Long.MAX_VALUE));
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

    @Test
    public void shouldFailIfValueIsDifferentForCompareValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValueTimestamp(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, key)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValueTimestamp(producerRecord,
            new ProducerRecord<byte[], byte[]>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, null)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReverseForCompareValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValueTimestamp(nullKeyValueRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, value)));
    }

    @Test
    public void shouldFailIfTimestampIsDifferentForCompareValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareValueTimestamp(producerRecord, value, 0));
    }

    @Test
    public void shouldFailIfTimestampDifferentWithNullReverseForCompareValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareValueTimestamp(nullKeyValueRecord, null, 0));
    }

    @Test
    public void shouldFailIfTimestampIsDifferentForCompareValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareValueTimestamp(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, 0L, key, value)));
    }

    @Test
    public void shouldPassIfKeyAndValueAndTimestampIsEqualForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, key, value, Long.MAX_VALUE);
    }

    @Test
    public void shouldPassIfKeyAndValueIsEqualWithNullForCompareKeyValueTimestamp() {
        OutputVerifier.compareKeyValueTimestamp(nullKeyValueRecord, null, null, Long.MAX_VALUE);
    }

    @Test
    public void shouldFailIfKeyIsDifferentForCompareKeyValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareKeyValueTimestamp(producerRecord, value, value, Long.MAX_VALUE));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareKeyValueTimestamp(producerRecord, null, value, Long.MAX_VALUE));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValueTimestamp() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, null, value),
                key, value, Long.MAX_VALUE));
    }

    @Test
    public void shouldFailIfValueIsDifferentForCompareKeyValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareKeyValueTimestamp(producerRecord, key, key, Long.MAX_VALUE));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValueTimestamp() {
        assertThrows(AssertionError.class,
            () -> OutputVerifier.compareKeyValueTimestamp(producerRecord, key, null, Long.MAX_VALUE));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValueTimestamp() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<byte[], byte[]>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, null),
                key, value, Long.MAX_VALUE));
    }

    @Test
    public void shouldPassIfKeyAndValueAndTimestampIsEqualForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(producerRecord, new ProducerRecord<>(
            "otherTopic", 0, Long.MAX_VALUE, key, value));
    }

    @Test
    public void shouldPassIfKeyAndValueAndTimestampIsEqualWithNullForCompareKeyValueTimestampWithProducerRecord() {
        OutputVerifier.compareKeyValueTimestamp(nullKeyValueRecord, new ProducerRecord<byte[], byte[]>(
            "otherTopic", 0, Long.MAX_VALUE, null, null));
    }

    @Test
    public void shouldFailIfKeyIsDifferentForCompareKeyValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, value, value)));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullForCompareKeyValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, null, value)));
    }

    @Test
    public void shouldFailIfKeyIsDifferentWithNullReversForCompareKeyValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, null, value), producerRecord));
    }

    @Test
    public void shouldFailIfValueIsDifferentForCompareKeyValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(producerRecord,
            new ProducerRecord<>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, key)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullForCompareKeyValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(producerRecord,
            new ProducerRecord<byte[], byte[]>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, null)));
    }

    @Test
    public void shouldFailIfValueIsDifferentWithNullReversForCompareKeyValueTimestampWithProducerRecord() {
        assertThrows(AssertionError.class, () -> OutputVerifier.compareKeyValueTimestamp(
            new ProducerRecord<byte[], byte[]>("someTopic", Integer.MAX_VALUE, Long.MAX_VALUE, key, null), producerRecord));
    }

}

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

import java.util.Objects;

/**
 * TODO + JavaDocs for all methods
 */
public class OutputVerifier {

    public static <K, V> void compareValue(final ProducerRecord<K, V> record,
                                           final V expectedValue) throws AssertionError {
        Objects.requireNonNull(record);

        final V recordValue = record.value();
        final AssertionError error = new AssertionError("Expected value=" + expectedValue + " but was value=" + recordValue);

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }
    }

    public static <K, V> void compareValue(final ProducerRecord<K, V> record,
                                           final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareValue(record, expectedRecord.value());
    }

    public static <K, V> void compareKeyValue(final ProducerRecord<K, V> record,
                                              final K expectedKey,
                                             final V expectedValue) throws AssertionError {
        Objects.requireNonNull(record);

        final K recordKey = record.key();
        final V recordValue = record.value();
        final AssertionError error = new AssertionError("Expected <" + expectedKey + ", " + expectedValue + "> " +
            "but was <" + recordKey + ", " + recordValue + ">");

        if (recordKey != null) {
            if (!recordKey.equals(expectedKey)) {
                throw error;
            }
        } else if (expectedKey != null) {
            throw error;
        }

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }
    }

    public static <K, V> void compareKeyValue(final ProducerRecord<K, V> record,
                                              final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareKeyValue(record, expectedRecord.key(), expectedRecord.value());
    }

    public static <K, V> void compareValueTimestamp(final ProducerRecord<K, V> record,
                                                    final V expectedValue,
                                                    final long expectedTimestamp) throws AssertionError {
        Objects.requireNonNull(record);

        final V recordValue = record.value();
        final long recordTimestamp = record.timestamp();
        final AssertionError error = new AssertionError("Expected value=" + expectedValue + " with timestamp=" + expectedTimestamp +
            " but was value=" + recordValue + " with timestamp=" + recordTimestamp);

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }

        if (recordTimestamp != expectedTimestamp) {
            throw error;
        }
    }

    public static <K, V> void compareValueTimestamp(final ProducerRecord<K, V> record,
                                                    final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareValueTimestamp(record, expectedRecord.value(), expectedRecord.timestamp());
    }

    public static <K, V> void compareKeyValueTimestamp(final ProducerRecord<K, V> record,
                                                       final K expectedKey,
                                                       final V expectedValue,
                                                       final long expectedTimestamp) throws AssertionError {
        Objects.requireNonNull(record);

        final K recordKey = record.key();
        final V recordValue = record.value();
        final long recordTimestamp = record.timestamp();
        final AssertionError error = new AssertionError("Expected <" + expectedKey + ", " + expectedValue + "> with timestamp=" + expectedTimestamp +
            " but was <" + recordKey + ", " + recordValue + "> with timestamp=" + recordTimestamp);

        if (recordKey != null) {
            if (!recordKey.equals(expectedKey)) {
                throw error;
            }
        } else if (expectedKey != null) {
            throw error;
        }

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }

        if (recordTimestamp != expectedTimestamp) {
            throw error;
        }
    }

    public static <K, V> void compareKeyValueTimestamp(final ProducerRecord<K, V> record,
                                                       final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareKeyValueTimestamp(record, expectedRecord.key(), expectedRecord.value(), expectedRecord.timestamp());
    }

}

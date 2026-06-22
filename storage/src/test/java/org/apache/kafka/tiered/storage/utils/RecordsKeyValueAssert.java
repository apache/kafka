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
package org.apache.kafka.tiered.storage.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Assertion utility for comparing heterogeneous record types:
 * {@link ProducerRecord}, {@link ConsumerRecord} or {@link Record}.
 *
 * <p>It is conceptually incorrect to try to match records of different natures.
 * Only a committed {@link Record} is univoque, whereas a {@link ProducerRecord} or {@link ConsumerRecord} is
 * a physical representation of a record-to-be or viewed record.
 *
 * <p>This utility breaches that semantic so that testers can avoid performing manual comparisons on
 * targeted internal fields of these type of records. This implementation only compares key and
 * value of the records.
 */
public final class RecordsKeyValueAssert {

    private RecordsKeyValueAssert() {}

    /**
     * Asserts that the actual records correspond to the expected records by comparing keys and values in order.
     *
     * @param <R1> The type of records used to formulate the expectations.
     * @param <R2> The type of records compared against the expectations.
     * @param <K> The type of the record keys.
     * @param <V> The type of the record values.
     * @param expectedRecords The records expected.
     * @param actualRecords The records to verify.
     * @param topicPartition The topic-partition which the records belong to.
     * @param keySerde The {@link Serde} for the keys of the records.
     * @param valueSerde The {@link Serde} for the values of the records.
     */
    public static <R1, R2, K, V> void assertRecordsCorrespondTo(Collection<R1> expectedRecords,
                                                                Collection<R2> actualRecords,
                                                                TopicPartition topicPartition,
                                                                Serde<K> keySerde,
                                                                Serde<V> valueSerde) {
        assertEquals(expectedRecords.size(), actualRecords.size());

        Deserializer<K> keyDeserializer = keySerde.deserializer();
        Deserializer<V> valueDeserializer = valueSerde.deserializer();
        Iterator<R1> expectedIterator = expectedRecords.iterator();
        Iterator<R2> actualIterator = actualRecords.iterator();
        int index = 0;
        while (expectedIterator.hasNext() && actualIterator.hasNext()) {
            R1 expected = expectedIterator.next();
            R2 actual = actualIterator.next();
            SimpleRecord expectedRecord = convert(expected, topicPartition, keySerde, valueSerde);
            SimpleRecord actualRecord = convert(actual, topicPartition, keySerde, valueSerde);
            assertFieldEquals(expectedRecord.key(), actualRecord.key(),
                    keyDeserializer, "Record key", index, topicPartition);
            assertFieldEquals(expectedRecord.value(), actualRecord.value(),
                    valueDeserializer, "Record value", index, topicPartition);
            index++;
        }
    }

    private static void assertFieldEquals(ByteBuffer expected,
                                          ByteBuffer actual,
                                          Deserializer<?> deserializer,
                                          String fieldName,
                                          int index,
                                          TopicPartition topicPartition) {
        if (!Objects.equals(expected, actual)) {
            Object expectedDeserialized = deserializer.deserialize(topicPartition.topic(),
                    Utils.toNullableArray(expected));
            Object actualDeserialized = deserializer.deserialize(topicPartition.topic(),
                    Utils.toNullableArray(actual));
            fail(fieldName + " mismatch at index " + index + " for " + topicPartition
                    + ". Expected: " + expectedDeserialized + ", Actual: " + actualDeserialized);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SimpleRecord convert(Object recordCandidate,
                                               TopicPartition topicPartition,
                                               Serde<K> keySerde,
                                               Serde<V> valueSerde) {
        if (recordCandidate instanceof ProducerRecord<?, ?> record) {
            long timestamp = record.timestamp() != null ? record.timestamp() : RecordBatch.NO_TIMESTAMP;
            ByteBuffer keyBytes =
                    Utils.wrapNullable(keySerde.serializer().serialize(topicPartition.topic(), (K) record.key()));
            ByteBuffer valueBytes =
                    Utils.wrapNullable(valueSerde.serializer().serialize(topicPartition.topic(), (V) record.value()));
            Header[] headers = record.headers() != null ? record.headers().toArray() : Record.EMPTY_HEADERS;
            return new SimpleRecord(timestamp, keyBytes, valueBytes, headers);
        } else if (recordCandidate instanceof ConsumerRecord<?, ?> record) {
            ByteBuffer keyBytes =
                    Utils.wrapNullable(keySerde.serializer().serialize(topicPartition.topic(), (K) record.key()));
            ByteBuffer valueBytes =
                    Utils.wrapNullable(valueSerde.serializer().serialize(topicPartition.topic(), (V) record.value()));
            Header[] headers = record.headers() != null ? record.headers().toArray() : Record.EMPTY_HEADERS;
            return new SimpleRecord(record.timestamp(), keyBytes, valueBytes, headers);
        } else if (recordCandidate instanceof Record record) {
            return new SimpleRecord(record.timestamp(), record.key(), record.value(), record.headers());
        } else {
            throw new IllegalArgumentException("Unsupported record type: " + recordCandidate.getClass().getSimpleName()
                    + ". Supported types: ProducerRecord, ConsumerRecord, Record");
        }
    }
}

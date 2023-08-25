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
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

public final class RecordsKeyValueMatcher<R1, R2, K, V> extends TypeSafeDiagnosingMatcher<Collection<R2>> {

    private final Collection<R1> expectedRecords;
    private final TopicPartition topicPartition;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    /**
     * Heterogeneous matcher between alternative types of records:
     * [[ProducerRecord]], [[ConsumerRecord]] or [[Record]].
     *
     * It is conceptually incorrect to try to match records of different natures.
     * Only a committed [[Record]] is univoque, whereas a [[ProducerRecord]] or [[ConsumerRecord]] is
     * a physical representation of a record-to-be or viewed record.
     *
     * This matcher breaches that semantic so that testers can avoid performing manual comparisons on
     * targeted internal fields of these type of records. This implementation only compares key and
     * value of the records.
     *
     * @param expectedRecords The records expected.
     * @param topicPartition The topic-partition which the records belong to.
     * @param keySerde The [[Serde]] for the keys of the records.
     * @param valueSerde The [[Serde]] for the values of the records.
     * @tparam R1 The type of records used to formulate the expectations.
     * @tparam R2 The type of records compared against the expectations.
     * @tparam K The type of the record keys.
     * @tparam V The type of the record values.
     */
    public RecordsKeyValueMatcher(Collection<R1> expectedRecords,
                                  TopicPartition topicPartition,
                                  Serde<K> keySerde,
                                  Serde<V> valueSerde) {
        this.expectedRecords = expectedRecords;
        this.topicPartition = topicPartition;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Records of ").appendValue(topicPartition).appendText(": ").appendValue(expectedRecords);
    }

    @Override
    protected boolean matchesSafely(Collection<R2> actualRecords, Description mismatchDescription) {
        if (expectedRecords.size() != actualRecords.size()) {
            mismatchDescription.appendText("Number of records differ. Expected: ")
                    .appendValue(expectedRecords.size())
                    .appendText(", Actual: ")
                    .appendValue(actualRecords.size())
                    .appendText("; ");
            return false;
        }
        Iterator<R1> expectedIterator = expectedRecords.iterator();
        Iterator<R2> actualIterator = actualRecords.iterator();
        while (expectedIterator.hasNext() && actualIterator.hasNext()) {
            R1 expected = expectedIterator.next();
            R2 actual = actualIterator.next();
            if (!matches(expected, actual, mismatchDescription)) {
                return false;
            }
        }
        return true;
    }

    private boolean matches(R1 expected, R2 actual, Description mismatchDescription) {
        SimpleRecord expectedRecord = convert(expected);
        SimpleRecord actualRecord = convert(actual);
        if (expectedRecord == null) {
            mismatchDescription.appendText("Invalid expected record type: ")
                    .appendValue(expected.getClass().getSimpleName());
            return false;
        }
        if (actualRecord == null) {
            mismatchDescription.appendText("Invalid actual record type: ")
                    .appendValue(actual.getClass().getSimpleName());
            return false;
        }
        if (!compare(expectedRecord.key(), actualRecord.key(), keySerde.deserializer(), "Record key",
                mismatchDescription) ||
                !compare(expectedRecord.value(), actualRecord.value(), valueSerde.deserializer(), "Record value",
                        mismatchDescription)) {
            return false;
        }
        return true;
    }

    private boolean compare(ByteBuffer lhs,
                            ByteBuffer rhs,
                            Deserializer<?> deserializer,
                            String desc,
                            Description mismatchDescription) {
        if ((lhs != null && !lhs.equals(rhs)) || (lhs == null && rhs != null)) {
            mismatchDescription.appendText(desc).appendText(" mismatch. Expected: ")
                    .appendValue(deserializer.deserialize(topicPartition.topic(), Utils.toNullableArray(lhs)))
                    .appendText("; Actual: ")
                    .appendValue(deserializer.deserialize(topicPartition.topic(), Utils.toNullableArray(rhs)))
                    .appendText("; ");
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private SimpleRecord convert(Object recordCandidate) {
        if (recordCandidate instanceof ProducerRecord) {
            ProducerRecord<?, ?> record = (ProducerRecord<?, ?>) recordCandidate;
            long timestamp = record.timestamp() != null ? record.timestamp() : RecordBatch.NO_TIMESTAMP;
            ByteBuffer keyBytes =
                    Utils.wrapNullable(keySerde.serializer().serialize(topicPartition.topic(), (K) record.key()));
            ByteBuffer valueBytes =
                    Utils.wrapNullable(valueSerde.serializer().serialize(topicPartition.topic(), (V) record.value()));
            Header[] headers = record.headers() != null ? record.headers().toArray() : Record.EMPTY_HEADERS;
            return new SimpleRecord(timestamp, keyBytes, valueBytes, headers);
        } else if (recordCandidate instanceof ConsumerRecord) {
            ConsumerRecord<?, ?> record = (ConsumerRecord<?, ?>) recordCandidate;
            ByteBuffer keyBytes =
                    Utils.wrapNullable(keySerde.serializer().serialize(topicPartition.topic(), (K) record.key()));
            ByteBuffer valueBytes =
                    Utils.wrapNullable(valueSerde.serializer().serialize(topicPartition.topic(), (V) record.value()));
            Header[] headers = record.headers() != null ? record.headers().toArray() : Record.EMPTY_HEADERS;
            return new SimpleRecord(record.timestamp(), keyBytes, valueBytes, headers);
        } else if (recordCandidate instanceof Record) {
            Record record = (Record) recordCandidate;
            return new SimpleRecord(record.timestamp(), record.key(), record.value(), record.headers());
        } else {
            return null;
        }
    }

    /**
     * Provides a matcher which compares the key and value of a sequence of records with those of
     * the expectedRecords sequence, in order.
     *
     * @param expectedRecords The records expected.
     * @param topicPartition The topic-partition which the records belong to.
     * @param keySerde The [[Serde]] for the keys of the records.
     * @param valueSerde The [[Serde]] for the values of the records.
     * @tparam R1 The type of records used to formulate the expectations.
     * @tparam R2 The type of records compared against the expectations.
     * @tparam K The type of the record keys.
     * @tparam V The type of the record values.
     */
    public static <R1, R2, K, V> RecordsKeyValueMatcher<R1, R2, K, V> correspondTo(Collection<R1> expectedRecords,
                                                                                   TopicPartition topicPartition,
                                                                                   Serde<K> keySerde,
                                                                                   Serde<V> valueSerde) {
        return new RecordsKeyValueMatcher<>(expectedRecords, topicPartition, keySerde, valueSerde);
    }
}
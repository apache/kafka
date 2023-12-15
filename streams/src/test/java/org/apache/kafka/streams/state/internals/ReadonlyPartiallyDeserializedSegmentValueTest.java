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

import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@RunWith(Enclosed.class)
public class ReadonlyPartiallyDeserializedSegmentValueTest {

    /**
     * Non-exceptional scenarios which are expected to occur during regular store operation.
     */
    @RunWith(Parameterized.class)
    public static class ExpectedCasesTest {

        private static final List<TestCase> TEST_CASES = new ArrayList<>();

        static {
            // test cases are expected to have timestamps in strictly decreasing order (except for the degenerate case)
            TEST_CASES.add(new TestCase("single record", 10, new TestRecord("foo".getBytes(), 1)));
            TEST_CASES.add(new TestCase("multiple records", 10, new TestRecord("foo".getBytes(), 8), new TestRecord("bar".getBytes(), 3), new TestRecord("baz".getBytes(), 0)));
            TEST_CASES.add(new TestCase("single tombstone", 10, new TestRecord(null, 1)));
            TEST_CASES.add(new TestCase("multiple tombstone", 10, new TestRecord(null, 4), new TestRecord(null, 1)));
            TEST_CASES.add(new TestCase("tombstones and records (r, t, r)", 10, new TestRecord("foo".getBytes(), 5), new TestRecord(null, 2), new TestRecord("bar".getBytes(), 1)));
            TEST_CASES.add(new TestCase("tombstones and records (t, r, t)", 10, new TestRecord(null, 5), new TestRecord("foo".getBytes(), 2), new TestRecord(null, 1)));
            TEST_CASES.add(new TestCase("tombstones and records (r, r, t, t)", 10, new TestRecord("foo".getBytes(), 6), new TestRecord("bar".getBytes(), 5), new TestRecord(null, 2), new TestRecord(null, 1)));
            TEST_CASES.add(new TestCase("tombstones and records (t, t, r, r)", 10, new TestRecord(null, 7), new TestRecord(null, 6), new TestRecord("foo".getBytes(), 2), new TestRecord("bar".getBytes(), 1)));
            TEST_CASES.add(new TestCase("record with empty bytes", 10, new TestRecord(new byte[0], 1)));
            TEST_CASES.add(new TestCase("records with empty bytes (r, e)", 10, new TestRecord("foo".getBytes(), 4), new TestRecord(new byte[0], 1)));
            TEST_CASES.add(new TestCase("records with empty bytes (e, e, r)", 10, new TestRecord(new byte[0], 8), new TestRecord(new byte[0], 2), new TestRecord("foo".getBytes(), 1)));
        }

        private final TestCase testCase;

        public ExpectedCasesTest(final TestCase testCase) {
            this.testCase = testCase;
        }

        @Parameterized.Parameters(name = "{0}")
        public static Collection<TestCase> data() {
            return TEST_CASES;
        }

        @Test
        public void shouldFindInTimeRangesWithDifferentOrders() {

            // create a list of timestamps in ascending order to use them in combination for starting and ending point of the time range.
            final List<Long> timestamps = createTimestampsFromTestRecords(testCase);

            // verify results
//            final List<ResultOrder> orders = Arrays.asList(ResultOrder.ASCENDING, ResultOrder.ANY, ResultOrder.DESCENDING);
            final List<ResultOrder> orders = Arrays.asList(ResultOrder.DESCENDING);

            for (final ResultOrder order: orders) {
                for (final Long from : timestamps) {
                    for (final Long to : timestamps) {
                        // build expected results indices based on time range
                        final List<Integer> expectedRecordsIndices = new ArrayList<>();
                        for (int i = 0; i < testCase.records.size(); i++) {
                            final long recordValidTo = i == 0 ? testCase.nextTimestamp : testCase.records.get(i - 1).timestamp;
                            if (testCase.records.get(i).timestamp <= to && recordValidTo > from) {
                                if (testCase.records.get(i).value != null) { // the results do not include tombstones
                                    expectedRecordsIndices.add(i);
                                }
                            }
                        }
                        // The only object that has access to the find method is the instance of LogicalSegmentIterator.
                        // Therefore, closing the iterator means that the segment calling the find method is destroyed and needs
                        // to be created for the next time range.
                        final ReadonlyPartiallyDeserializedSegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);
                        if (order.equals(ResultOrder.ASCENDING)) {
                            Collections.reverse(expectedRecordsIndices);
                        }
                        int segmentRecordIndex = -1;
                        for (final int index : expectedRecordsIndices) {
                            final long expectedValidTo = index == 0 ? testCase.nextTimestamp : testCase.records.get(index - 1).timestamp;

                            final SegmentSearchResult result = segmentValue.find(from, to, order, segmentRecordIndex);

                            assertNotNull(result);

                            final TestRecord expectedRecord = testCase.records.get(index);

                            assertThat(result.index(), equalTo(index));
                            assertThat(result.value(), equalTo(expectedRecord.value));
                            assertThat(result.validFrom(), equalTo(expectedRecord.timestamp));
                            assertThat(result.validTo(), equalTo(expectedValidTo));

                            segmentRecordIndex = order.equals(ResultOrder.ASCENDING) ? index - 1 : index + 1;
                        }
                        // should return null when the index == record number
                        assertNull(segmentValue.find(from, to, order, segmentRecordIndex));

                        // verify no results within the time range
                        if (expectedRecordsIndices.size() == 0) {
                            assertNull(segmentValue.find(from, to, order, -1));
                        }
                    }
                }
            }
        }

        @Test
        public void shouldThrowWithInvalidPositiveIndex() {
            final ReadonlyPartiallyDeserializedSegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);
            final Exception exception = assertThrows(IllegalArgumentException.class, () ->
                    segmentValue.find(Long.MIN_VALUE, Long.MAX_VALUE, ResultOrder.ANY, 10000000));
            assertEquals("The segment does not contain any record with the specified index.", exception.getMessage());
        }

        @Test
        public void shouldThrowWithInvalidNegativeIndex() {
            final ReadonlyPartiallyDeserializedSegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);
            final Exception exception = assertThrows(IllegalArgumentException.class, () ->
                    segmentValue.find(Long.MIN_VALUE, Long.MAX_VALUE, ResultOrder.ANY, -10000000));
            assertEquals("The segment does not contain any record with the specified index.", exception.getMessage());
        }
    }

    private static ReadonlyPartiallyDeserializedSegmentValue buildSegmentWithInsertLatest(final TestCase testCase) {
        SegmentValue segmentValue = null;
        for (int recordIdx = testCase.records.size() - 1; recordIdx >= 0; recordIdx--) {
            final TestRecord record = testCase.records.get(recordIdx);
            final long validTo = recordIdx == 0 ? testCase.nextTimestamp : testCase.records.get(recordIdx - 1).timestamp;

            if (segmentValue == null) {
                // initialize
                if (testCase.records.size() > 1 && record.value == null) {
                    // when possible, validate that building up a segment starting from the degenerate case is valid as well
                    segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(null, record.timestamp, record.timestamp);
                } else {
                    segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(record.value, record.timestamp, validTo);
                }
            } else {
                // insert latest
                segmentValue.insertAsLatest(record.timestamp, validTo, record.value);
            }
        }
        assertNotNull(segmentValue);
        return new ReadonlyPartiallyDeserializedSegmentValue(segmentValue.serialize());
    }

    private static List<Long> createTimestampsFromTestRecords(final TestCase testCase) {
        final List<Long> timestamps = new ArrayList<>();
        timestamps.add(Long.MIN_VALUE);
        for (int i = testCase.records.size() - 1; i >= 0; i--) {
            final long timestamp = testCase.records.get(i).timestamp;
            if (i == testCase.records.size() - 1) { // the oldest record
                timestamps.add(timestamp - 2);
                timestamps.add(timestamp - 1);
                timestamps.add(timestamp);
            } else if (i != 0) { // records in between
                timestamps.add(timestamp);
                final long mid = (timestamp + testCase.records.get(i - 1).timestamp) / 2;
                if (!timestamps.contains(mid)) {
                    timestamps.add(mid);
                } else {
                    timestamps.add(mid + 1);
                }
            } else { // the newest record
                timestamps.add(timestamp);
                timestamps.add(timestamp + 1);
                timestamps.add(timestamp + 2);
            }
        }
        timestamps.add(Long.MAX_VALUE);
        return timestamps;
    }

    private static class TestRecord {
        final byte[] value;
        final long timestamp;

        TestRecord(final byte[] value, final long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    private static class TestCase {
        final List<TestRecord> records;
        final long nextTimestamp;
        final long minTimestamp;
        final boolean isDegenerate;
        final String name;

        TestCase(final String name, final long nextTimestamp, final TestRecord... records) {
            this(name, nextTimestamp, Arrays.asList(records));
        }

        TestCase(final String name, final long nextTimestamp, final List<TestRecord> records) {
            this.records = records;
            this.nextTimestamp = nextTimestamp;
            this.minTimestamp = records.get(records.size() - 1).timestamp;
            this.isDegenerate = nextTimestamp == minTimestamp;
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
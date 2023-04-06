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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class RocksDBVersionedStoreSegmentValueFormatterTest {

    /**
     * Non-exceptional scenarios which are expected to occur during regular store operation.
     */
    @RunWith(Parameterized.class)
    public static class ExpectedCasesTest {

        private static final List<TestCase> TEST_CASES = new ArrayList<>();

        static {
            // test cases are expected to have timestamps in strictly decreasing order (except for the degenerate case)
            TEST_CASES.add(new TestCase("degenerate", 10, new TestRecord(null, 10)));
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
        public void shouldSerializeAndDeserialize() {
            final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

            final byte[] serialized = segmentValue.serialize();
            final SegmentValue deserialized = RocksDBVersionedStoreSegmentValueFormatter.deserialize(serialized);

            verifySegmentContents(deserialized, testCase);
        }

        @Test
        public void shouldBuildWithInsertLatest() {
            final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

            verifySegmentContents(segmentValue, testCase);
        }

        @Test
        public void shouldBuildWithInsertEarliest() {
            final SegmentValue segmentValue = buildSegmentWithInsertEarliest(testCase);

            verifySegmentContents(segmentValue, testCase);
        }

        @Test
        public void shouldInsertAtIndex() {
            if (testCase.isDegenerate) {
                // cannot insert into degenerate segment
                return;
            }

            // test inserting at each possible index
            for (int insertIdx = 0; insertIdx <= testCase.records.size() - 1; insertIdx++) {
                // build record to insert
                final long newRecordTimestamp;
                if (insertIdx == 0) {
                    newRecordTimestamp = testCase.records.get(0).timestamp + 1;
                    if (newRecordTimestamp == testCase.nextTimestamp) {
                        // cannot insert because no timestamp exists between last record and nextTimestamp
                        continue;
                    }
                } else {
                    newRecordTimestamp = testCase.records.get(insertIdx - 1).timestamp - 1;
                    if (newRecordTimestamp < 0 || (newRecordTimestamp == testCase.records.get(insertIdx).timestamp)) {
                        // cannot insert because timestamps of existing records are adjacent
                        continue;
                    }
                }
                final TestRecord newRecord = new TestRecord("new".getBytes(), newRecordTimestamp);

                final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

                // insert() first requires a call to find()
                segmentValue.find(testCase.records.get(insertIdx).timestamp, false);

                segmentValue.insert(newRecord.timestamp, newRecord.value, insertIdx);

                // create expected results
                final List<TestRecord> expectedRecords = new ArrayList<>(testCase.records);
                expectedRecords.add(insertIdx, newRecord);

                verifySegmentContents(segmentValue, new TestCase("expected", testCase.nextTimestamp, expectedRecords));
            }
        }

        @Test
        public void shouldUpdateAtIndex() {
            if (testCase.isDegenerate) {
                // cannot update degenerate segment
                return;
            }

            // test updating at each possible index
            for (int updateIdx = 0; updateIdx < testCase.records.size(); updateIdx++) {
                // build updated record
                long updatedRecordTimestamp = testCase.records.get(updateIdx).timestamp - 1;
                if (updatedRecordTimestamp < 0 || (updateIdx < testCase.records.size() - 1 && updatedRecordTimestamp == testCase.records.get(updateIdx + 1).timestamp)) {
                    // found timestamp conflict. try again
                    updatedRecordTimestamp = testCase.records.get(updateIdx).timestamp + 1;
                    if (updateIdx > 0 && updatedRecordTimestamp == testCase.records.get(updateIdx - 1).timestamp) {
                        // found timestamp conflict. use original timestamp
                        updatedRecordTimestamp = testCase.records.get(updateIdx).timestamp;
                    }
                }
                final TestRecord updatedRecord = new TestRecord("updated".getBytes(), updatedRecordTimestamp);

                final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

                // updateRecord() first requires a call to find()
                segmentValue.find(testCase.records.get(updateIdx).timestamp, false);
                segmentValue.updateRecord(updatedRecord.timestamp, updatedRecord.value, updateIdx);

                // create expected results
                final List<TestRecord> expectedRecords = new ArrayList<>(testCase.records);
                expectedRecords.remove(updateIdx);
                expectedRecords.add(updateIdx, updatedRecord);

                verifySegmentContents(segmentValue, new TestCase("expected", testCase.nextTimestamp, expectedRecords));
            }
        }

        @Test
        public void shouldFindByTimestamp() {
            if (testCase.isDegenerate) {
                // cannot find() on degenerate segment
                return;
            }

            final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

            // build expected mapping from timestamp -> record
            final Map<Long, Integer> expectedRecordIndices = new HashMap<>();
            // it's important that this for-loop iterates backwards through the record indices, so that
            // when adjacent records have adjacent timestamps, then the record with the later timestamp
            // (i.e., the earlier index) takes precedence
            for (int recordIdx = testCase.records.size() - 1; recordIdx >= 0; recordIdx--) {
                if (recordIdx < testCase.records.size() - 1) {
                    expectedRecordIndices.put(testCase.records.get(recordIdx).timestamp - 1, recordIdx + 1);
                }
                if (recordIdx > 0) {
                    expectedRecordIndices.put(testCase.records.get(recordIdx).timestamp + 1, recordIdx);
                }
                expectedRecordIndices.put(testCase.records.get(recordIdx).timestamp, recordIdx);
            }

            // verify results
            for (final Map.Entry<Long, Integer> entry : expectedRecordIndices.entrySet()) {
                final TestRecord expectedRecord = testCase.records.get(entry.getValue());
                final long expectedValidTo = entry.getValue() == 0 ? testCase.nextTimestamp : testCase.records.get(entry.getValue() - 1).timestamp;

                final SegmentSearchResult result = segmentValue.find(entry.getKey(), true);

                assertThat(result.index(), equalTo(entry.getValue()));
                assertThat(result.value(), equalTo(expectedRecord.value));
                assertThat(result.validFrom(), equalTo(expectedRecord.timestamp));
                assertThat(result.validTo(), equalTo(expectedValidTo));
            }

            // verify exception when timestamp is out of range
            assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.nextTimestamp, false));
            assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.nextTimestamp + 1, false));
            assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.minTimestamp - 1, false));
        }

        @Test
        public void shouldGetTimestamps() {
            final byte[] segmentValue = buildSegmentWithInsertLatest(testCase).serialize();

            assertThat(RocksDBVersionedStoreSegmentValueFormatter.getNextTimestamp(segmentValue), equalTo(testCase.nextTimestamp));
            assertThat(RocksDBVersionedStoreSegmentValueFormatter.getMinTimestamp(segmentValue), equalTo(testCase.minTimestamp));
        }

        @Test
        public void shouldCreateNewWithRecord() {
            if (testCase.records.size() != 1) {
                return;
            }

            final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(
                testCase.records.get(0).value,
                testCase.records.get(0).timestamp,
                testCase.nextTimestamp);

            verifySegmentContents(segmentValue, testCase);
        }
    }

    /**
     * These scenarios may only be hit in the event of an earlier exception, such as failure to
     * write to a particular segment store of {@link RocksDBVersionedStore} resulting in an
     * inconsistency among segments. These tests verify that the store is able to recover
     * gracefully even if this happens.
     */
    @RunWith(Parameterized.class)
    public static class ExceptionalCasesTest {

        private static final long INSERT_VALID_FROM_TIMESTAMP = 10L;
        private static final long INSERT_VALID_TO_TIMESTAMP = 13L;
        private static final byte[] INSERT_VALUE = "new".getBytes();
        private static final List<TestCase> TEST_CASES = new ArrayList<>();

        static {
            // test cases are expected to have timestamps in strictly decreasing order
            TEST_CASES.add(new TestCase("truncate all, single record", 15, new TestRecord(null, 12)));
            TEST_CASES.add(new TestCase("truncate all, single record, exact timestamp match", 15, new TestRecord(null, INSERT_VALID_FROM_TIMESTAMP)));
            TEST_CASES.add(new TestCase("truncate all, multiple records", 15, new TestRecord(null, 12), new TestRecord("foo".getBytes(), 11)));
            TEST_CASES.add(new TestCase("truncate all, multiple records, exact timestamp match", 15, new TestRecord(null, 12), new TestRecord("foo".getBytes(), 11), new TestRecord(null, INSERT_VALID_FROM_TIMESTAMP)));
            TEST_CASES.add(new TestCase("partial truncation, single record", 15, new TestRecord(null, 8)));
            TEST_CASES.add(new TestCase("partial truncation, multiple records", 15, new TestRecord("foo".getBytes(), 12), new TestRecord("bar".getBytes(), 8)));
            TEST_CASES.add(new TestCase("partial truncation, on record boundary", 15, new TestRecord("foo".getBytes(), 12), new TestRecord("bar".getBytes(), INSERT_VALID_FROM_TIMESTAMP), new TestRecord("baz".getBytes(), 8)));
        }

        private final TestCase testCase;

        public ExceptionalCasesTest(final TestCase testCase) {
            this.testCase = testCase;
        }

        @Parameterized.Parameters(name = "{0}")
        public static Collection<TestCase> data() {
            return TEST_CASES;
        }

        @Test
        public void shouldRecoverFromStoreInconsistencyOnInsertLatest() {
            final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

            segmentValue.insertAsLatest(INSERT_VALID_FROM_TIMESTAMP, INSERT_VALID_TO_TIMESTAMP, INSERT_VALUE);

            final TestCase expectedRecords = buildExpectedRecordsForInsertLatest(testCase);
            verifySegmentContents(segmentValue, expectedRecords);
        }

        private static TestCase buildExpectedRecordsForInsertLatest(final TestCase originalTestCase) {
            final List<TestRecord> originalRecords = originalTestCase.records;
            final List<TestRecord> newRecords = new ArrayList<>();

            for (int i = originalRecords.size() - 1; i >= 0; i--) {
                final TestRecord originalRecord = originalRecords.get(i);
                if (originalRecord.timestamp < INSERT_VALID_FROM_TIMESTAMP) {
                    newRecords.add(0, originalRecord);
                } else {
                    break;
                }
            }
            newRecords.add(0, new TestRecord(INSERT_VALUE, INSERT_VALID_FROM_TIMESTAMP));
            return new TestCase("expected", INSERT_VALID_TO_TIMESTAMP, newRecords);
        }
    }

    private static SegmentValue buildSegmentWithInsertLatest(final TestCase testCase) {
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
        return segmentValue;
    }

    private static SegmentValue buildSegmentWithInsertEarliest(final TestCase testCase) {
        final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(null, testCase.nextTimestamp, testCase.nextTimestamp);
        for (int recordIdx = 0; recordIdx < testCase.records.size(); recordIdx++) {
            final TestRecord record = testCase.records.get(recordIdx);
            segmentValue.insertAsEarliest(record.timestamp, record.value);
        }
        return segmentValue;
    }

    private static void verifySegmentContents(final SegmentValue segmentValue, final TestCase expectedRecords) {
        if (!expectedRecords.isDegenerate) { // cannot find() on degenerate segment
            // verify expected records
            for (int recordIdx = 0; recordIdx < expectedRecords.records.size(); recordIdx++) {
                final TestRecord expectedRecord = expectedRecords.records.get(recordIdx);
                final long expectedValidTo = recordIdx == 0 ? expectedRecords.nextTimestamp : expectedRecords.records.get(recordIdx - 1).timestamp;

                final SegmentSearchResult result = segmentValue.find(expectedRecord.timestamp, true);

                assertThat(result.index(), equalTo(recordIdx));
                assertThat(result.value(), equalTo(expectedRecord.value));
                assertThat(result.validFrom(), equalTo(expectedRecord.timestamp));
                assertThat(result.validTo(), equalTo(expectedValidTo));
            }
        }

        // verify expected exceptions from timestamp out-of-bounds
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(expectedRecords.nextTimestamp, false));
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(expectedRecords.minTimestamp - 1, false));
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
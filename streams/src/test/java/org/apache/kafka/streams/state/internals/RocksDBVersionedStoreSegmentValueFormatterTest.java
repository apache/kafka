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

import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RocksDBVersionedStoreSegmentValueFormatterTest {

    private static final long INSERT_VALID_FROM_TIMESTAMP = 10L;
    private static final long INSERT_VALID_TO_TIMESTAMP = 13L;
    private static final byte[] INSERT_VALUE = "new".getBytes();
    /**
     * Non-exceptional scenarios which are expected to occur during regular store operation.
     */
    // test cases are expected to have timestamps in strictly decreasing order (except for the degenerate case)
    private static Stream<Arguments> nonExceptionalData() {
        return Stream.of(
            Arguments.of(new TestCase("degenerate", 10, new TestRecord(null, 10))),
            Arguments.of(new TestCase("single record", 10, new TestRecord("foo".getBytes(), 1))),
            Arguments.of(new TestCase("multiple records", 10, new TestRecord("foo".getBytes(), 8), new TestRecord("bar".getBytes(), 3), new TestRecord("baz".getBytes(), 0))),
            Arguments.of(new TestCase("single tombstone", 10, new TestRecord(null, 1))),
            Arguments.of(new TestCase("multiple tombstone", 10, new TestRecord(null, 4), new TestRecord(null, 1))),
            Arguments.of(new TestCase("tombstones and records (r, t, r)", 10, new TestRecord("foo".getBytes(), 5), new TestRecord(null, 2), new TestRecord("bar".getBytes(), 1))),
            Arguments.of(new TestCase("tombstones and records (t, r, t)", 10, new TestRecord(null, 5), new TestRecord("foo".getBytes(), 2), new TestRecord(null, 1))),
            Arguments.of(new TestCase("tombstones and records (r, r, t, t)", 10, new TestRecord("foo".getBytes(), 6), new TestRecord("bar".getBytes(), 5), new TestRecord(null, 2), new TestRecord(null, 1))),
            Arguments.of(new TestCase("tombstones and records (t, t, r, r)", 10, new TestRecord(null, 7), new TestRecord(null, 6), new TestRecord("foo".getBytes(), 2), new TestRecord("bar".getBytes(), 1))),
            Arguments.of(new TestCase("record with empty bytes", 10, new TestRecord(new byte[0], 1))),
            Arguments.of(new TestCase("records with empty bytes (r, e)", 10, new TestRecord("foo".getBytes(), 4), new TestRecord(new byte[0], 1))),
            Arguments.of(new TestCase("records with empty bytes (e, e, r)", 10, new TestRecord(new byte[0], 8), new TestRecord(new byte[0], 2), new TestRecord("foo".getBytes(), 1)))
        );
    }
    
    /**
     * These scenarios may only be hit in the event of an earlier exception, such as failure to
     * write to a particular segment store of {@link RocksDBVersionedStore} resulting in an
     * inconsistency among segments. These tests verify that the store is able to recover
     * gracefully even if this happens.
     */
    private static Stream<Arguments> exceptionalData() {
        return Stream.of(
                Arguments.of(new TestCase("truncate all, single record", 15, new TestRecord(null, 12))),
                Arguments.of(new TestCase("truncate all, single record, exact timestamp match", 15, new TestRecord(null, INSERT_VALID_FROM_TIMESTAMP))),
                Arguments.of(new TestCase("truncate all, multiple records", 15, new TestRecord(null, 12), new TestRecord("foo".getBytes(), 11))),
                Arguments.of(new TestCase("truncate all, multiple records, exact timestamp match", 15, new TestRecord(null, 12), new TestRecord("foo".getBytes(), 11), new TestRecord(null, INSERT_VALID_FROM_TIMESTAMP))),
                Arguments.of(new TestCase("partial truncation, single record", 15, new TestRecord(null, 8))),
                Arguments.of(new TestCase("partial truncation, multiple records", 15, new TestRecord("foo".getBytes(), 12), new TestRecord("bar".getBytes(), 8))),
                Arguments.of(new TestCase("partial truncation, on record boundary", 15, new TestRecord("foo".getBytes(), 12), new TestRecord("bar".getBytes(), INSERT_VALID_FROM_TIMESTAMP), new TestRecord("baz".getBytes(), 8)))
        );
    }

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldSerializeAndDeserialize(final TestCase testCase) {
        final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

        final byte[] serialized = segmentValue.serialize();
        final SegmentValue deserialized = RocksDBVersionedStoreSegmentValueFormatter.deserialize(serialized);

        verifySegmentContents(deserialized, testCase);
    }

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldBuildWithInsertLatest(final TestCase testCase) {
        final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

        verifySegmentContents(segmentValue, testCase);
    }

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldBuildWithInsertEarliest(final TestCase testCase) {
        final SegmentValue segmentValue = buildSegmentWithInsertEarliest(testCase);

        verifySegmentContents(segmentValue, testCase);
    }

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldInsertAtIndex(final TestCase testCase) {
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

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldUpdateAtIndex(final TestCase testCase) {
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

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldFindByTimestamp(final TestCase testCase) {
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

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldFindAll(final TestCase testCase) {
        if (testCase.isDegenerate) {
            // cannot find() on degenerate segment
            return;
        }

        final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);


        // verify results
        final List<SegmentSearchResult> results =
            segmentValue.findAll(testCase.records.get(testCase.records.size() - 1).timestamp, testCase.records.get(0).timestamp);

        int i = 0;
        int index = 0;
        for (final TestRecord expectedRecord : testCase.records) {
            if (expectedRecord.value == null) { // should not return tombstones
                index++;
                continue;
            }
            final long expectedValidTo = index == 0 ? testCase.nextTimestamp : testCase.records.get(index - 1).timestamp;
            assertThat(results.get(i).index(), equalTo(index));
            assertThat(results.get(i).value(), equalTo(expectedRecord.value));
            assertThat(results.get(i).validFrom(), equalTo(expectedRecord.timestamp));
            assertThat(results.get(i).validTo(), equalTo(expectedValidTo));
            i++;
            index++;
        }

        // verify exception when timestamp is out of range
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.nextTimestamp, false));
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.nextTimestamp + 1, false));
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.minTimestamp - 1, false));
    }

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldGetTimestamps(final TestCase testCase) {
        final byte[] segmentValue = buildSegmentWithInsertLatest(testCase).serialize();

        assertThat(RocksDBVersionedStoreSegmentValueFormatter.nextTimestamp(segmentValue), equalTo(testCase.nextTimestamp));
        assertThat(RocksDBVersionedStoreSegmentValueFormatter.minTimestamp(segmentValue), equalTo(testCase.minTimestamp));
    }

    @ParameterizedTest
    @MethodSource("nonExceptionalData")
    public void shouldCreateNewWithRecord(final TestCase testCase) {
        if (testCase.records.size() != 1) {
            return;
        }

        final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(
            testCase.records.get(0).value,
            testCase.records.get(0).timestamp,
            testCase.nextTimestamp);

        verifySegmentContents(segmentValue, testCase);
    }

    @ParameterizedTest
    @MethodSource("exceptionalData")
    public void shouldRecoverFromStoreInconsistencyOnInsertLatest(final TestCase testCase) {
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
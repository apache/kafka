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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RocksDBVersionedStoreTest {

    private static final String STORE_NAME = "myversionedrocks";
    private static final String METRICS_SCOPE = "versionedrocksdb";
    private static final long HISTORY_RETENTION = 300_000L;
    private static final long SEGMENT_INTERVAL = HISTORY_RETENTION / 3;
    private static final long BASE_TIMESTAMP = 10L;
    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();

    private InternalMockProcessorContext context;

    private RocksDBVersionedStore store;

    @Before
    public void before() {
        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        );
        context.setTime(BASE_TIMESTAMP);

        store = new RocksDBVersionedStore(STORE_NAME, METRICS_SCOPE, HISTORY_RETENTION, SEGMENT_INTERVAL);
        store.init((StateStoreContext) context, store);
    }

    @After
    public void after() {
        store.close();
    }

    @Test
    public void shouldPutLatest() {
        putToStore("k", "v", BASE_TIMESTAMP);
        putToStore("k", "v2", BASE_TIMESTAMP + 1);

        verifyGetValueFromStore("k", "v2", BASE_TIMESTAMP + 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP + 1, "v2", BASE_TIMESTAMP + 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP + 2, "v2", BASE_TIMESTAMP + 1);
    }

    @Test
    public void shouldPutNullAsLatest() {
        putToStore("k", null, BASE_TIMESTAMP);
        putToStore("k", null, BASE_TIMESTAMP + 1);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP + 1);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP + 2);
    }

    @Test
    public void shouldPutOlderWithNonNullLatest() {
        putToStore("k", "v", BASE_TIMESTAMP);
        putToStore("k", "v2", BASE_TIMESTAMP - 2);
        putToStore("k", "v1", BASE_TIMESTAMP - 1);
        putToStore("k", "v4", BASE_TIMESTAMP - 4);

        verifyGetValueFromStore("k", "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 1, "v1", BASE_TIMESTAMP - 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 2, "v2", BASE_TIMESTAMP - 2);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 3, "v4", BASE_TIMESTAMP - 4);
    }

    @Test
    public void shouldPutOlderWithNullLatest() {
        putToStore("k", null, BASE_TIMESTAMP);
        putToStore("k", "v2", BASE_TIMESTAMP - 2);
        putToStore("k", "v1", BASE_TIMESTAMP - 1);
        putToStore("k", "v4", BASE_TIMESTAMP - 4);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 1, "v1", BASE_TIMESTAMP - 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 2, "v2", BASE_TIMESTAMP - 2);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 3, "v4", BASE_TIMESTAMP - 4);
    }

    @Test
    public void shouldPutOlderNullWithNonNullLatest() {
        putToStore("k", "v", BASE_TIMESTAMP);
        putToStore("k", null, BASE_TIMESTAMP - 2);
        putToStore("k", null, BASE_TIMESTAMP - 1);
        putToStore("k", null, BASE_TIMESTAMP - 4);
        putToStore("k", "v5", BASE_TIMESTAMP - 5);
        putToStore("k", "v3", BASE_TIMESTAMP - 3);
        putToStore("k", null, BASE_TIMESTAMP - 6);

        verifyGetValueFromStore("k", "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "v", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 1);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 2);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 3, "v3", BASE_TIMESTAMP - 3);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 4);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 5, "v5", BASE_TIMESTAMP - 5);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 6);
    }

    @Test
    public void shouldPutOlderNullWithNullLatest() {
        putToStore("k", null, BASE_TIMESTAMP);
        putToStore("k", null, BASE_TIMESTAMP - 2);
        putToStore("k", null, BASE_TIMESTAMP - 1);
        putToStore("k", null, BASE_TIMESTAMP - 4);
        putToStore("k", "v3", BASE_TIMESTAMP - 3);
        putToStore("k", "v5", BASE_TIMESTAMP - 5);
        putToStore("k", null, BASE_TIMESTAMP - 6);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 1);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 2);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 3, "v3", BASE_TIMESTAMP - 3);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 4);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 5, "v5", BASE_TIMESTAMP - 5);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 6);
    }

    @Test
    public void shouldPutRepeatTimestampAsLatest() {
        putToStore("k", "to_be_replaced", BASE_TIMESTAMP);
        putToStore("k", "b", BASE_TIMESTAMP);

        verifyGetValueFromStore("k", "b", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "b", BASE_TIMESTAMP);

        putToStore("k", null, BASE_TIMESTAMP);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);

        putToStore("k", null, BASE_TIMESTAMP);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);

        putToStore("k", "b", BASE_TIMESTAMP);

        verifyGetValueFromStore("k", "b", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "b", BASE_TIMESTAMP);
    }

    @Test
    public void shouldPutRepeatTimestamps() {
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL - 10);
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL - 10);
        putToStore("k", null, SEGMENT_INTERVAL - 10);
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL - 1);
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL + 1);
        putToStore("k", null, SEGMENT_INTERVAL - 1);
        putToStore("k", null, SEGMENT_INTERVAL + 1);
        putToStore("k", null, SEGMENT_INTERVAL + 10);
        putToStore("k", null, SEGMENT_INTERVAL + 5);
        putToStore("k", "vp5", SEGMENT_INTERVAL + 5);
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL - 5);
        putToStore("k", "vn5", SEGMENT_INTERVAL - 5);
        putToStore("k", null, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL + 20);
        putToStore("k", "vn6", SEGMENT_INTERVAL - 6);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 30);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 15);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 6, "vp5", SEGMENT_INTERVAL + 5);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 2);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 5, "vn5", SEGMENT_INTERVAL - 5);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 6, "vn6", SEGMENT_INTERVAL - 6);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 8);
    }

    @Test
    public void shouldPutIntoMultipleSegments() {
        putToStore("k", null, SEGMENT_INTERVAL - 20);
        putToStore("k", "vn10", SEGMENT_INTERVAL - 10);
        putToStore("k", null, SEGMENT_INTERVAL - 1);
        putToStore("k", null, SEGMENT_INTERVAL + 1);
        putToStore("k", "vp10", SEGMENT_INTERVAL + 10);
        putToStore("k", null, SEGMENT_INTERVAL + 20);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 30);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 15, "vp10", SEGMENT_INTERVAL + 10);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 5);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 2);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 5, "vn10", SEGMENT_INTERVAL - 10);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 15);
    }

    @Test
    public void shouldMoveRecordToOlderSegmentDuringPut() {
        putToStore("k", "vp20", SEGMENT_INTERVAL + 20);
        putToStore("k", "vp10", SEGMENT_INTERVAL + 10);
        putToStore("k", "vn10", SEGMENT_INTERVAL - 10);
        putToStore("k", "vn2", SEGMENT_INTERVAL - 2);
        putToStore("k", "vn1", SEGMENT_INTERVAL - 1);
        putToStore("k", "vp1", SEGMENT_INTERVAL + 1);

        verifyGetValueFromStore("k", "vp20", SEGMENT_INTERVAL + 20);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 30, "vp20", SEGMENT_INTERVAL + 20);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 15, "vp10", SEGMENT_INTERVAL + 10);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 5, "vp1", SEGMENT_INTERVAL + 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL, "vn1", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 1, "vn1", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 2, "vn2", SEGMENT_INTERVAL - 2);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 5, "vn10", SEGMENT_INTERVAL - 10);
    }

    @Test
    public void shouldMoveRecordToOlderSegmentWithNullsDuringPut() {
        putToStore("k", null, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL - 1);
        putToStore("k", null, SEGMENT_INTERVAL + 1);
        putToStore("k", null, SEGMENT_INTERVAL - 10);
        putToStore("k", null, SEGMENT_INTERVAL + 10);
        putToStore("k", "vp5", SEGMENT_INTERVAL + 5);
        putToStore("k", "vn5", SEGMENT_INTERVAL - 5);
        putToStore("k", "vn6", SEGMENT_INTERVAL - 6);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 30);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 15);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 6, "vp5", SEGMENT_INTERVAL + 5);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 2);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 5, "vn5", SEGMENT_INTERVAL - 5);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 6, "vn6", SEGMENT_INTERVAL - 6);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 8);
    }

    @Test
    public void shouldFallThroughToExistingOlderSegmentAsLatestDuringPut() {
        putToStore("k", null, SEGMENT_INTERVAL - 5);
        putToStore("k", "vn6", SEGMENT_INTERVAL - 6);
        putToStore("k", "vp20", SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL + 10);
        putToStore("k", null, SEGMENT_INTERVAL - 1);
        putToStore("k", "vn2", SEGMENT_INTERVAL - 2);

        verifyGetValueFromStore("k", "vp20", SEGMENT_INTERVAL + 20);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 30, "vp20", SEGMENT_INTERVAL + 20);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 12);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 2, "vn2", SEGMENT_INTERVAL - 2);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 5);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 6, "vn6", SEGMENT_INTERVAL - 6);
    }

    @Test
    public void shouldPutNonLatestTombstoneIntoNewSegmentWithValidTo() {
        putToStore("k", "vp30", SEGMENT_INTERVAL + 30);
        putToStore("k", null, SEGMENT_INTERVAL - 10); // this put should result in tombstone with validTo=SEGMENT_INTERVAL+30
        putToStore("k", "vn5", SEGMENT_INTERVAL - 5);
        putToStore("k", "vn1", SEGMENT_INTERVAL - 1);
        putToStore("k", null, SEGMENT_INTERVAL - 2);

        verifyGetValueFromStore("k", "vp30", SEGMENT_INTERVAL + 30);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 10, "vn1", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 1, "vn1", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 2);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 5, "vn5", SEGMENT_INTERVAL - 5);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 10);
    }

    @Test
    public void shouldGetFromOlderSegments() {
        // use a different key to create three different segments
        putToStore("ko", null, SEGMENT_INTERVAL - 10);
        putToStore("ko", null, 2 * SEGMENT_INTERVAL - 10);
        putToStore("ko", null, 3 * SEGMENT_INTERVAL - 10);

        // return null after visiting all segments
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 20);

        // insert data to create non-empty (first) segment
        putToStore("k", "v", SEGMENT_INTERVAL - 20);
        putToStore("k", null, SEGMENT_INTERVAL - 10);

        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 30);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 15, "v", SEGMENT_INTERVAL - 20);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 5);

        // insert data to create non-empty (third) segment
        putToStore("k", "v2", 3 * SEGMENT_INTERVAL - 20);
        putToStore("k", null, 3 * SEGMENT_INTERVAL - 10);

        // presence of non-empty later segment does not affect results of getting from earlier segment
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 30);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 15, "v", SEGMENT_INTERVAL - 20);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 5);
    }

    @Test
    public void shouldNotGetExpired() {
        putToStore("k", "v_old", 0);
        putToStore("k", "v", SEGMENT_INTERVAL - 10);

        // old record has not yet expired
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 11, "v_old", 0);

        putToStore("ko", "vo", HISTORY_RETENTION + SEGMENT_INTERVAL - 11);

        // old record still has not yet expired
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 11, "v_old", 0);

        putToStore("ko", "vo2", HISTORY_RETENTION + SEGMENT_INTERVAL - 10);

        // old record is expired now
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 11);
    }

    @Test
    public void shouldDistinguishEmptyAndNull() {
        putToStore("k", null, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL - 10);
        putToStore("k", null, SEGMENT_INTERVAL - 1);
        putToStore("k", null, SEGMENT_INTERVAL + 1);
        putToStore("k", null, SEGMENT_INTERVAL + 10);
        // empty string is serialized as an empty byte array, which is different from null (tombstone)
        putToStore("k", "", SEGMENT_INTERVAL + 5);
        putToStore("k", "", SEGMENT_INTERVAL - 5);
        putToStore("k", "", SEGMENT_INTERVAL - 6);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 30);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 15);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 6, "", SEGMENT_INTERVAL + 5);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL + 2);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 5, "", SEGMENT_INTERVAL - 5);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 6, "", SEGMENT_INTERVAL - 6);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 8);
    }

    private void putToStore(final String key, final String value, final long timestamp) {
        store.put(
            new Bytes(key.getBytes(UTF_8)),
            value == null ? null : value.getBytes(UTF_8),
            timestamp
        );
    }

    private VersionedRecord<String> getFromStore(final String key) {
        final VersionedRecord<byte[]> versionedRecord
            = store.get(new Bytes(STRING_SERIALIZER.serialize(null, key)));
        return deserializedRecord(versionedRecord);
    }

    private VersionedRecord<String> getFromStore(final String key, final long asOfTimestamp) {
        final VersionedRecord<byte[]> versionedRecord
            = store.get(new Bytes(STRING_SERIALIZER.serialize(null, key)), asOfTimestamp);
        return deserializedRecord(versionedRecord);
    }

    private void verifyGetValueFromStore(final String key, final String expectedValue, final long expectedTimestamp) {
        final VersionedRecord<String> latest = getFromStore(key);
        assertThat(latest.value(), equalTo(expectedValue));
        assertThat(latest.timestamp(), equalTo(expectedTimestamp));
    }

    private void verifyGetNullFromStore(final String key) {
        final VersionedRecord<String> record = getFromStore(key);
        assertThat(record, nullValue());
    }

    private void verifyTimestampedGetValueFromStore(final String key, final long timestamp, final String expectedValue, final long expectedTimestamp) {
        final VersionedRecord<String> latest = getFromStore(key, timestamp);
        assertThat(latest.value(), equalTo(expectedValue));
        assertThat(latest.timestamp(), equalTo(expectedTimestamp));
    }

    private void verifyTimestampedGetNullFromStore(final String key, final long timestamp) {
        final VersionedRecord<String> record = getFromStore(key, timestamp);
        assertThat(record, nullValue());
    }

    private static VersionedRecord<String> deserializedRecord(final VersionedRecord<byte[]> versionedRecord) {
        return versionedRecord == null
            ? null
            : new VersionedRecord<>(
            STRING_DESERIALIZER.deserialize(null, versionedRecord.value()),
            versionedRecord.timestamp());
    }
}
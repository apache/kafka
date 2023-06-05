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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
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
    private static final long GRACE_PERIOD = HISTORY_RETENTION; // history retention doubles as grace period for now
    private static final long SEGMENT_INTERVAL = HISTORY_RETENTION / 3;
    private static final long BASE_TIMESTAMP = 10L;
    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();
    private static final String DROPPED_RECORDS_METRIC = "dropped-records-total";
    private static final String TASK_LEVEL_GROUP = "stream-task-metrics";

    private InternalMockProcessorContext context;
    private Map<String, String> expectedMetricsTags;

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

        expectedMetricsTags = mkMap(
            mkEntry("thread-id", Thread.currentThread().getName()),
            mkEntry("task-id", context.taskId().toString())
        );

        store = new RocksDBVersionedStore(STORE_NAME, METRICS_SCOPE, HISTORY_RETENTION, SEGMENT_INTERVAL);
        store.init((StateStoreContext) context, store);
    }

    @After
    public void after() {
        store.close();
    }

    @Test
    public void shouldPutLatest() {
        putToStore("k", "v", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "v2", BASE_TIMESTAMP + 1, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        verifyGetValueFromStore("k", "v2", BASE_TIMESTAMP + 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP + 1, "v2", BASE_TIMESTAMP + 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP + 2, "v2", BASE_TIMESTAMP + 1);
    }

    @Test
    public void shouldPutNullAsLatest() {
        putToStore("k", null, BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, BASE_TIMESTAMP + 1, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP + 1);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP + 2);
    }

    @Test
    public void shouldPutOlderWithNonNullLatest() {
        putToStore("k", "v", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "v2", BASE_TIMESTAMP - 2, BASE_TIMESTAMP);
        putToStore("k", "v1", BASE_TIMESTAMP - 1, BASE_TIMESTAMP);
        putToStore("k", "v4", BASE_TIMESTAMP - 4, BASE_TIMESTAMP - 2);

        verifyGetValueFromStore("k", "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 1, "v1", BASE_TIMESTAMP - 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 2, "v2", BASE_TIMESTAMP - 2);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 3, "v4", BASE_TIMESTAMP - 4);
    }

    @Test
    public void shouldPutOlderWithNullLatest() {
        putToStore("k", null, BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "v2", BASE_TIMESTAMP - 2, BASE_TIMESTAMP);
        putToStore("k", "v1", BASE_TIMESTAMP - 1, BASE_TIMESTAMP);
        putToStore("k", "v4", BASE_TIMESTAMP - 4, BASE_TIMESTAMP - 2);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 1, "v1", BASE_TIMESTAMP - 1);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 2, "v2", BASE_TIMESTAMP - 2);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP - 3, "v4", BASE_TIMESTAMP - 4);
    }

    @Test
    public void shouldPutOlderNullWithNonNullLatest() {
        putToStore("k", "v", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, BASE_TIMESTAMP - 2, BASE_TIMESTAMP);
        putToStore("k", null, BASE_TIMESTAMP - 1, BASE_TIMESTAMP);
        putToStore("k", null, BASE_TIMESTAMP - 4, BASE_TIMESTAMP - 2);
        putToStore("k", "v5", BASE_TIMESTAMP - 5, BASE_TIMESTAMP - 4);
        putToStore("k", "v3", BASE_TIMESTAMP - 3, BASE_TIMESTAMP - 2);
        putToStore("k", null, BASE_TIMESTAMP - 6, BASE_TIMESTAMP - 5);

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
        putToStore("k", null, BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, BASE_TIMESTAMP - 2, BASE_TIMESTAMP);
        putToStore("k", null, BASE_TIMESTAMP - 1, BASE_TIMESTAMP);
        putToStore("k", null, BASE_TIMESTAMP - 4, BASE_TIMESTAMP - 2);
        putToStore("k", "v3", BASE_TIMESTAMP - 3, BASE_TIMESTAMP - 2);
        putToStore("k", "v5", BASE_TIMESTAMP - 5, BASE_TIMESTAMP - 4);
        putToStore("k", null, BASE_TIMESTAMP - 6, BASE_TIMESTAMP - 5);

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
        putToStore("k", "to_be_replaced", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "b", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        verifyGetValueFromStore("k", "b", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "b", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 1);

        putToStore("k", null, BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 1);

        putToStore("k", null, BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        verifyGetNullFromStore("k");
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 1);

        putToStore("k", "b", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        verifyGetValueFromStore("k", "b", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "b", BASE_TIMESTAMP);
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP - 1);
    }

    @Test
    public void shouldPutRepeatTimestamps() {
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL + 20);
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL + 20); // replace existing null with non-null, with timestamps spanning segments
        putToStore("k", null, SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL + 20); // replace existing non-null with null
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL - 1, SEGMENT_INTERVAL + 20);
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL + 1, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL - 1, SEGMENT_INTERVAL + 1); // replace existing non-null with null
        putToStore("k", null, SEGMENT_INTERVAL + 1, SEGMENT_INTERVAL + 20); // replace existing non-null with null, with timestamps spanning segments
        putToStore("k", null, SEGMENT_INTERVAL + 10, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL + 5, SEGMENT_INTERVAL + 10);
        putToStore("k", "vp5", SEGMENT_INTERVAL + 5, SEGMENT_INTERVAL + 10); // replace existing null with non-null
        putToStore("k", "to_be_replaced", SEGMENT_INTERVAL - 5, SEGMENT_INTERVAL - 1);
        putToStore("k", "vn5", SEGMENT_INTERVAL - 5, SEGMENT_INTERVAL - 1); // replace existing non-null with non-null
        putToStore("k", null, SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED); // replace existing non-null (latest value) with null
        putToStore("k", null, SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED); // replace existing null with null
        putToStore("k", "vn6", SEGMENT_INTERVAL - 6, SEGMENT_INTERVAL - 5);

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
        putToStore("k", null, SEGMENT_INTERVAL - 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "vn10", SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL - 1, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL + 1, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "vp10", SEGMENT_INTERVAL + 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

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
        putToStore("k", "vp20", SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "vp10", SEGMENT_INTERVAL + 10, SEGMENT_INTERVAL + 20);
        putToStore("k", "vn10", SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL + 10);
        putToStore("k", "vn2", SEGMENT_INTERVAL - 2, SEGMENT_INTERVAL + 10);
        putToStore("k", "vn1", SEGMENT_INTERVAL - 1, SEGMENT_INTERVAL + 10);
        putToStore("k", "vp1", SEGMENT_INTERVAL + 1, SEGMENT_INTERVAL + 10);

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
        putToStore("k", null, SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL - 1, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL + 1, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL - 1);
        putToStore("k", null, SEGMENT_INTERVAL + 10, SEGMENT_INTERVAL + 20);
        putToStore("k", "vp5", SEGMENT_INTERVAL + 5, SEGMENT_INTERVAL + 10);
        putToStore("k", "vn5", SEGMENT_INTERVAL - 5, SEGMENT_INTERVAL - 1);
        putToStore("k", "vn6", SEGMENT_INTERVAL - 6, SEGMENT_INTERVAL - 5);

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
        putToStore("k", null, SEGMENT_INTERVAL - 5, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "vn6", SEGMENT_INTERVAL - 6, SEGMENT_INTERVAL - 5);
        putToStore("k", "vp20", SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL + 10, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL - 1, SEGMENT_INTERVAL + 10);
        putToStore("k", "vn2", SEGMENT_INTERVAL - 2, SEGMENT_INTERVAL - 1);

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
        putToStore("k", "vp30", SEGMENT_INTERVAL + 30, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL + 30); // this put should result in tombstone with validTo=SEGMENT_INTERVAL+30
        putToStore("k", "vn5", SEGMENT_INTERVAL - 5, SEGMENT_INTERVAL + 30);
        putToStore("k", "vn1", SEGMENT_INTERVAL - 1, SEGMENT_INTERVAL + 30);
        putToStore("k", null, SEGMENT_INTERVAL - 2, SEGMENT_INTERVAL - 1);

        verifyGetValueFromStore("k", "vp30", SEGMENT_INTERVAL + 30);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL + 10, "vn1", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 1, "vn1", SEGMENT_INTERVAL - 1);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 2);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 5, "vn5", SEGMENT_INTERVAL - 5);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 10);
    }

    @Test
    public void shouldDelete() {
        putToStore("k", "vp20", SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "vp10", SEGMENT_INTERVAL + 10, SEGMENT_INTERVAL + 20);
        putToStore("k", "vn10", SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL + 10);
        putToStore("k", "vn2", SEGMENT_INTERVAL - 2, SEGMENT_INTERVAL + 10);

        VersionedRecord<String> deleted = deleteFromStore("k", SEGMENT_INTERVAL - 5); // delete from segment
        assertThat(deleted.value(), equalTo("vn10"));
        assertThat(deleted.timestamp(), equalTo(SEGMENT_INTERVAL - 10));

        deleted = deleteFromStore("k", SEGMENT_INTERVAL + 10); // delete existing timestamp
        assertThat(deleted.value(), equalTo("vp10"));
        assertThat(deleted.timestamp(), equalTo(SEGMENT_INTERVAL + 10));

        deleted = deleteFromStore("k", SEGMENT_INTERVAL + 10); // delete the same timestamp again
        assertThat(deleted, nullValue());

        deleted = deleteFromStore("k", SEGMENT_INTERVAL + 25); // delete from latest value store
        assertThat(deleted.value(), equalTo("vp20"));
        assertThat(deleted.timestamp(), equalTo(SEGMENT_INTERVAL + 20));
    }

    @Test
    public void shouldNotPutExpired() {
        putToStore("k", "v", HISTORY_RETENTION + 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        // grace period has not elapsed
        putToStore("k1", "v1", HISTORY_RETENTION + 10 - GRACE_PERIOD, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        verifyGetValueFromStore("k1", "v1", HISTORY_RETENTION + 10 - GRACE_PERIOD);

        // grace period has elapsed, so this put does not take place
        putToStore("k2", "v2", HISTORY_RETENTION + 9 - GRACE_PERIOD, PUT_RETURN_CODE_NOT_PUT);
        verifyGetNullFromStore("k2");

        verifyExpiredRecordSensor(1);
    }

    @Test
    public void shouldNotDeleteExpired() {
        putToStore("k1", "v1", 1L, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k2", "v2", 1L, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("kother", "vother", HISTORY_RETENTION + 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED); // use separate key to advance stream time

        // grace period has not elapsed
        VersionedRecord<String> deleted = deleteFromStore("k1", HISTORY_RETENTION + 10 - GRACE_PERIOD);
        assertThat(deleted.value(), equalTo("v1"));
        assertThat(deleted.timestamp(), equalTo(1L));
        verifyGetNullFromStore("k1");

        // grace period has elapsed, so this delete does not take place
        deleted = deleteFromStore("k2", HISTORY_RETENTION + 9 - GRACE_PERIOD);
        assertThat(deleted, nullValue()); // return value is null even though record exists because delete did not take place
        verifyGetValueFromStore("k2", "v2", 1L);

        verifyExpiredRecordSensor(1);
    }

    @Test
    public void shouldGetFromOlderSegments() {
        // use a different key to create three different segments
        putToStore("ko", null, SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("ko", null, 2 * SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("ko", null, 3 * SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        // return null after visiting all segments
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 20);

        // insert data to create non-empty (first) segment
        putToStore("k", "v", SEGMENT_INTERVAL - 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 30);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 15, "v", SEGMENT_INTERVAL - 20);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 5);

        // insert data to create non-empty (third) segment
        putToStore("k", "v2", 3 * SEGMENT_INTERVAL - 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, 3 * SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        // presence of non-empty later segment does not affect results of getting from earlier segment
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 30);
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 15, "v", SEGMENT_INTERVAL - 20);
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 5);
    }

    @Test
    public void shouldNotGetExpired() {
        putToStore("k", "v_old", 0, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", "v", SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        // old record has not yet expired
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 11, "v_old", 0);

        putToStore("ko", "vo", HISTORY_RETENTION + SEGMENT_INTERVAL - 11, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        // old record still has not yet expired
        verifyTimestampedGetValueFromStore("k", SEGMENT_INTERVAL - 11, "v_old", 0);

        putToStore("ko", "vo2", HISTORY_RETENTION + SEGMENT_INTERVAL - 10, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        // old record is expired now
        verifyTimestampedGetNullFromStore("k", SEGMENT_INTERVAL - 11);
    }

    @Test
    public void shouldGetExpiredIfLatestValue() {
        putToStore("k", "v", 1, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("ko", "vo_old", 1, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("ko", "vo_new", HISTORY_RETENTION + 12, PUT_RETURN_CODE_VALID_TO_UNDEFINED);

        // expired get on key where latest satisfies timestamp bound still returns data
        verifyTimestampedGetValueFromStore("k", 10, "v", 1);

        // same expired get on key where latest value does not satisfy timestamp bound does not return data
        verifyTimestampedGetNullFromStore("ko", 10);
    }

    @Test
    public void shouldDistinguishEmptyAndNull() {
        putToStore("k", null, SEGMENT_INTERVAL + 20, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        putToStore("k", null, SEGMENT_INTERVAL - 10, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL - 1, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL + 1, SEGMENT_INTERVAL + 20);
        putToStore("k", null, SEGMENT_INTERVAL + 10, SEGMENT_INTERVAL + 20);
        // empty string is serialized as an empty byte array, which is different from null (tombstone)
        putToStore("k", "", SEGMENT_INTERVAL + 5, SEGMENT_INTERVAL + 10);
        putToStore("k", "", SEGMENT_INTERVAL - 5, SEGMENT_INTERVAL - 1);
        putToStore("k", "", SEGMENT_INTERVAL - 6, SEGMENT_INTERVAL - 5);

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

    @Test
    public void shouldRestore() {
        final List<DataRecord> records = new ArrayList<>();
        records.add(new DataRecord("k", "vp20", SEGMENT_INTERVAL + 20));
        records.add(new DataRecord("k", "vp10", SEGMENT_INTERVAL + 10));
        records.add(new DataRecord("k", "vn10", SEGMENT_INTERVAL - 10));
        records.add(new DataRecord("k", "vn2", SEGMENT_INTERVAL - 2));
        records.add(new DataRecord("k", "vn1", SEGMENT_INTERVAL - 1));
        records.add(new DataRecord("k", "vp1", SEGMENT_INTERVAL + 1));

        store.restoreBatch(getChangelogRecords(records));

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
    public void shouldRestoreWithNulls() {
        final List<DataRecord> records = new ArrayList<>();
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 20));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL - 1));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 1));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL - 10));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 10));
        records.add(new DataRecord("k", "vp5", SEGMENT_INTERVAL + 5));
        records.add(new DataRecord("k", "vn5", SEGMENT_INTERVAL - 5));
        records.add(new DataRecord("k", "vn6", SEGMENT_INTERVAL - 6));

        store.restoreBatch(getChangelogRecords(records));

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
    public void shouldRestoreWithNullsAndRepeatTimestamps() {
        final List<DataRecord> records = new ArrayList<>();
        records.add(new DataRecord("k", "to_be_replaced", SEGMENT_INTERVAL + 20));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL - 10));
        records.add(new DataRecord("k", "to_be_replaced", SEGMENT_INTERVAL - 10)); // replaces existing null with non-null, with timestamps spanning segments
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL - 10)); // replaces existing non-null with null
        records.add(new DataRecord("k", "to_be_replaced", SEGMENT_INTERVAL - 1));
        records.add(new DataRecord("k", "to_be_replaced", SEGMENT_INTERVAL + 1));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL - 1)); // replaces existing non-null with null
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 1)); // replaces existing non-null with null, with timestamps spanning segments
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 10));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 5));
        records.add(new DataRecord("k", "vp5", SEGMENT_INTERVAL + 5)); // replaces existing null with non-null
        records.add(new DataRecord("k", "to_be_replaced", SEGMENT_INTERVAL - 5));
        records.add(new DataRecord("k", "vn5", SEGMENT_INTERVAL - 5)); // replaces existing non-null with non-null
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 20)); // replaces existing non-null (latest value) with null
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL + 20)); // replaces existing null with null
        records.add(new DataRecord("k", "vn6", SEGMENT_INTERVAL - 6));

        store.restoreBatch(getChangelogRecords(records));

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
    public void shouldRestoreMultipleBatches() {
        final List<DataRecord> records = new ArrayList<>();
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL - 20));
        records.add(new DataRecord("k", "vn10", SEGMENT_INTERVAL - 10));
        records.add(new DataRecord("k", null, SEGMENT_INTERVAL - 1));

        final List<DataRecord> moreRecords = new ArrayList<>();
        moreRecords.add(new DataRecord("k", null, SEGMENT_INTERVAL + 1));
        moreRecords.add(new DataRecord("k", "vp10", SEGMENT_INTERVAL + 10));
        moreRecords.add(new DataRecord("k", null, SEGMENT_INTERVAL + 20));

        store.restoreBatch(getChangelogRecords(records));
        store.restoreBatch(getChangelogRecords(moreRecords));

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
    public void shouldNotRestoreExpired() {
        final List<DataRecord> records = new ArrayList<>();
        records.add(new DataRecord("k", "v", HISTORY_RETENTION + 10));
        records.add(new DataRecord("k1", "v1", HISTORY_RETENTION + 10 - GRACE_PERIOD)); // grace period has not elapsed
        records.add(new DataRecord("k2", "v2", HISTORY_RETENTION + 9 - GRACE_PERIOD)); // grace period has elapsed, so this record should not be restored

        store.restoreBatch(getChangelogRecords(records));

        verifyGetValueFromStore("k", "v", HISTORY_RETENTION + 10);
        verifyGetValueFromStore("k1", "v1", HISTORY_RETENTION + 10 - GRACE_PERIOD);
        verifyGetNullFromStore("k2");

        verifyExpiredRecordSensor(0);
    }

    @Test
    public void shouldRestoreEvenIfRecordWouldBeExpiredByEndOfBatch() {
        final List<DataRecord> records = new ArrayList<>();
        records.add(new DataRecord("k2", "v2", HISTORY_RETENTION - GRACE_PERIOD)); // this record will be older than grace period by the end of the batch, but should still be restored
        records.add(new DataRecord("k", "v", HISTORY_RETENTION + 10));

        store.restoreBatch(getChangelogRecords(records));

        verifyGetValueFromStore("k2", "v2", HISTORY_RETENTION - GRACE_PERIOD);
        verifyGetValueFromStore("k", "v", HISTORY_RETENTION + 10);
    }

    @Test
    public void shouldAllowZeroHistoryRetention() {
        // recreate store with zero history retention
        store.close();
        store = new RocksDBVersionedStore(STORE_NAME, METRICS_SCOPE, 0L, SEGMENT_INTERVAL);
        store.init((StateStoreContext) context, store);

        // put and get
        putToStore("k", "v", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        verifyGetValueFromStore("k", "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "v", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP + 1, "v", BASE_TIMESTAMP); // query in "future" is allowed

        // update existing record at same timestamp
        putToStore("k", "updated", BASE_TIMESTAMP, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        verifyGetValueFromStore("k", "updated", BASE_TIMESTAMP);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP, "updated", BASE_TIMESTAMP);

        // put new record version
        putToStore("k", "v2", BASE_TIMESTAMP + 2, PUT_RETURN_CODE_VALID_TO_UNDEFINED);
        verifyGetValueFromStore("k", "v2", BASE_TIMESTAMP + 2);
        verifyTimestampedGetValueFromStore("k", BASE_TIMESTAMP + 2, "v2", BASE_TIMESTAMP + 2);

        // query in past (history retention expired) returns null
        verifyTimestampedGetNullFromStore("k", BASE_TIMESTAMP + 1);

        // delete existing key
        deleteFromStore("k", BASE_TIMESTAMP + 3);
        verifyGetNullFromStore("k");

        // put in past (grace period expired) does not update the store
        putToStore("k2", "v", BASE_TIMESTAMP + 2, PUT_RETURN_CODE_NOT_PUT);
        verifyGetNullFromStore("k2");
        verifyExpiredRecordSensor(1);
    }

    private void putToStore(final String key, final String value, final long timestamp, final long expectedValidTo) {
        final long validTo = store.put(
            new Bytes(STRING_SERIALIZER.serialize(null, key)),
            STRING_SERIALIZER.serialize(null, value),
            timestamp
        );
        assertThat(validTo, equalTo(expectedValidTo));
    }

    private VersionedRecord<String> deleteFromStore(final String key, final long timestamp) {
        final VersionedRecord<byte[]> versionedRecord
            = store.delete(new Bytes(STRING_SERIALIZER.serialize(null, key)), timestamp);
        return deserializedRecord(versionedRecord);
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

    private void verifyExpiredRecordSensor(final int expectedValue) {
        final Metric metric = context.metrics().metrics().get(
            new MetricName(DROPPED_RECORDS_METRIC, TASK_LEVEL_GROUP, "", expectedMetricsTags)
        );
        assertEquals((Double) metric.metricValue(), expectedValue, 0.001);
    }

    private static VersionedRecord<String> deserializedRecord(final VersionedRecord<byte[]> versionedRecord) {
        return versionedRecord == null
            ? null
            : new VersionedRecord<>(
            STRING_DESERIALIZER.deserialize(null, versionedRecord.value()),
            versionedRecord.timestamp());
    }

    private static List<ConsumerRecord<byte[], byte[]>> getChangelogRecords(final List<DataRecord> data) {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();

        for (final DataRecord d : data) {
            final byte[] rawKey = STRING_SERIALIZER.serialize(null, d.key);
            final byte[] rawValue = STRING_SERIALIZER.serialize(null, d.value);
            records.add(new ConsumerRecord<>(
                "",
                0,
                0L,
                d.timestamp,
                TimestampType.CREATE_TIME,
                rawKey.length,
                rawValue == null ? 0 : rawValue.length,
                rawKey,
                rawValue,
                new RecordHeaders(),
                Optional.empty()
            ));
        }

        return records;
    }

    private static class DataRecord {
        final String key;
        final String value;
        final long timestamp;

        DataRecord(final String key, final String value, final long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}
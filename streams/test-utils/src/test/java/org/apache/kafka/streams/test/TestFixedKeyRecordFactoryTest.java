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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.TestFixedKeyRecordFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class TestFixedKeyRecordFactoryTest {

    @Test
    void testFullFixedKeyRecord() {
        final String key = "key";
        final String value = "value";
        final RecordHeaders headers = new RecordHeaders();
        final long timeStamp = 1L;

        final FixedKeyRecord<String, String> expected = InternalFixedKeyRecordFactory.create(
                new Record<>(key, value, timeStamp, headers));
        final FixedKeyRecord<String, String> testFixedKeyRecord = TestFixedKeyRecordFactory.createFixedKeyRecord(
                key, value, timeStamp, headers);

        assertThat(testFixedKeyRecord, equalTo(expected));

        assertThat(testFixedKeyRecord, equalTo(expected));
        assertThat(testFixedKeyRecord.key(), equalTo(expected.key()));
        assertThat(testFixedKeyRecord.value(), equalTo(expected.value()));
        assertThat(testFixedKeyRecord.headers(), equalTo(expected.headers()));
        assertThat(testFixedKeyRecord.timestamp(), equalTo(expected.timestamp()));

        assertThat(testFixedKeyRecord.key(), equalTo(key));
        assertThat(testFixedKeyRecord.value(), equalTo(value));
        assertThat(testFixedKeyRecord.headers(), equalTo(headers));
        assertThat(testFixedKeyRecord.timestamp(), equalTo(timeStamp));

    }

    @Test
    void testPartialFixedKeyRecord() {
        final String key = "key";
        final String value = "value";
        final RecordHeaders headers = new RecordHeaders();

        final List<FixedKeyRecord<String, String>> expectedFixedKeyRecords = Arrays.asList(
                InternalFixedKeyRecordFactory.create(new Record<>(key, value, 0L, headers)),
                InternalFixedKeyRecordFactory.create(new Record<>(key, value, 0L, null)),
                InternalFixedKeyRecordFactory.create(new Record<>(key, value, 1L, null)));

        final List<FixedKeyRecord<String, String>> testFixedKeyRecords = Arrays.asList(
                TestFixedKeyRecordFactory.createFixedKeyRecord(key, value, headers),
                TestFixedKeyRecordFactory.createFixedKeyRecord(key, value),
                TestFixedKeyRecordFactory.createFixedKeyRecord(key, value, 1L));

        for (int i = 0; i < expectedFixedKeyRecords.size(); i++) {
            final FixedKeyRecord<String, String> expected = expectedFixedKeyRecords.get(i);
            final FixedKeyRecord<String, String> testFixedKeyRecord = testFixedKeyRecords.get(i);

            assertThat(testFixedKeyRecord, equalTo(expected));
            assertThat(testFixedKeyRecord.key(), equalTo(expected.key()));
            assertThat(testFixedKeyRecord.value(), equalTo(expected.value()));
            assertThat(testFixedKeyRecord.headers(), equalTo(expected.headers()));
            assertThat(testFixedKeyRecord.timestamp(), equalTo(expected.timestamp()));
        }

    }

}
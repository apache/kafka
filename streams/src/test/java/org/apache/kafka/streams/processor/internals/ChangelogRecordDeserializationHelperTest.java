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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.PositionSerde;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY;
import static org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_KEY;
import static org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChangelogRecordDeserializationHelperTest {

    private static ConsumerRecord<byte[], byte[]> record() {
        return new ConsumerRecord<>("changelog", 0, 0L, new byte[0], new byte[0]);
    }

    @Test
    public void shouldUpdatePositionFromConsistencyHeaders() {
        final Position expected = Position.emptyPosition().withComponent("input", 0, 42L);
        final ConsumerRecord<byte[], byte[]> record = record();
        record.headers().add(CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        record.headers().add(new RecordHeader(CHANGELOG_POSITION_HEADER_KEY, PositionSerde.serialize(expected).array()));

        final Position position = Position.emptyPosition();
        ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(record, true, position);

        assertEquals(expected, position);
    }

    @Test
    public void shouldSkipRecordWithEmptyVersionHeader() {
        final ConsumerRecord<byte[], byte[]> record = record();
        record.headers().add(new RecordHeader(CHANGELOG_VERSION_HEADER_KEY, new byte[0]));

        final Position position = Position.emptyPosition();
        ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(record, true, position);

        assertEquals(Position.emptyPosition(), position);
    }

    @Test
    public void shouldSkipRecordWithNullVersionHeader() {
        final ConsumerRecord<byte[], byte[]> record = record();
        record.headers().add(new RecordHeader(CHANGELOG_VERSION_HEADER_KEY, null));

        final Position position = Position.emptyPosition();
        ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(record, true, position);

        assertEquals(Position.emptyPosition(), position);
    }

    @Test
    public void shouldThrowWhenPositionHeaderIsMissing() {
        final ConsumerRecord<byte[], byte[]> record = record();
        record.headers().add(CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);

        assertThrows(
            StreamsException.class,
            () -> ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(record, true, Position.emptyPosition())
        );
    }

    @Test
    public void shouldSkipRecordWithNullPositionHeader() {
        final ConsumerRecord<byte[], byte[]> record = record();
        record.headers().add(CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        record.headers().add(new RecordHeader(CHANGELOG_POSITION_HEADER_KEY, null));

        final Position position = Position.emptyPosition();
        ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(record, true, position);

        assertEquals(Position.emptyPosition(), position);
    }

    @Test
    public void shouldSkipRecordWithEmptyPositionHeader() {
        final ConsumerRecord<byte[], byte[]> record = record();
        record.headers().add(CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        record.headers().add(new RecordHeader(CHANGELOG_POSITION_HEADER_KEY, new byte[0]));

        final Position position = Position.emptyPosition();
        ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(record, true, position);

        assertEquals(Position.emptyPosition(), position);
    }

    @Test
    public void shouldIgnoreHeadersWhenConsistencyIsDisabled() {
        final ConsumerRecord<byte[], byte[]> record = record();
        record.headers().add(new RecordHeader(CHANGELOG_VERSION_HEADER_KEY, new byte[0]));

        final Position position = Position.emptyPosition();
        ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(record, false, position);

        assertEquals(Position.emptyPosition(), position);
    }
}

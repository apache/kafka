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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.query.Position;

import java.nio.ByteBuffer;

/**
 * Changelog records without any headers are considered old format.
 * New format changelog records will have a version in their headers.
 * Version 0: This indicates that the changelog records are under version control.
 * Version 1: This indicates that the changelog records have consistency information.
 */
public class ChangelogRecordDeserializationHelper {
    private static final byte[] V_0_CHANGELOG_VERSION_HEADER_VALUE = {(byte) 0};
    private static final byte[] V_1_CHANGELOG_VERSION_HEADER_VALUE = {(byte) 1};

    public static final String CHANGELOG_VERSION_HEADER_KEY = "v";
    public static final RecordHeader CHANGELOG_VERSION_HEADER_RECORD_DEFAULT = new RecordHeader(
            CHANGELOG_VERSION_HEADER_KEY, V_0_CHANGELOG_VERSION_HEADER_VALUE);
    public static final RecordHeader CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY = new RecordHeader(
            CHANGELOG_VERSION_HEADER_KEY, V_1_CHANGELOG_VERSION_HEADER_VALUE);

    public static void applyChecksAndUpdatePosition(
            final ConsumerRecord<byte[], byte[]> record,
            final boolean consistencyEnabled,
            final Position position
    ) {
        final Header versionHeader = record.headers().lastHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_KEY);

        if (versionHeader == null && consistencyEnabled) {
            throw new StreamsException("This should not happen. Consistency requires changelog records with " +
                    "headers.");
        }
        if (versionHeader != null
                && versionHeader.equals(
                ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY)
                && !consistencyEnabled) {
            throw new StreamsException("This should not happen. Consistency is not enabled but the changelog " +
                    "contains records with consistency information.");
        }
        if (versionHeader != null
                && versionHeader.equals(
                ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY)
                && consistencyEnabled) {

            final Header vectorHeader = record.headers().lastHeader(Position.VECTOR_KEY);
            if (vectorHeader == null) {
                throw new StreamsException("This should not happen. Consistency is enabled but the changelog " +
                        "contains records without consistency information.");
            }

            position.merge(Position.deserialize(ByteBuffer.wrap(vectorHeader.value())));
        }
    }


}

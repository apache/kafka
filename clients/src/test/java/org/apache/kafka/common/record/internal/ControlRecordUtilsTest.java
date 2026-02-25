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
package org.apache.kafka.common.record.internal;

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.message.VotersRecord.Voter;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ControlRecordUtilsTest {

    @Test
    public void testCurrentVersions() {
        // If any of these asserts fail, please make sure that Kafka supports reading and
        // writing the latest version for these records.
        assertEquals(
            (short) 0,
            ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION
        );
        assertEquals(
            SnapshotHeaderRecord.HIGHEST_SUPPORTED_VERSION,
            ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION
        );
        assertEquals(
            SnapshotFooterRecord.HIGHEST_SUPPORTED_VERSION,
            ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION
        );
        assertEquals(
            KRaftVersionRecord.HIGHEST_SUPPORTED_VERSION,
            ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION
        );
        assertEquals(
            VotersRecord.HIGHEST_SUPPORTED_VERSION,
            ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION
        );
    }

    @Test
    public void testInvalidControlRecordType() {
        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class,
            () -> testDeserializeRecord(ControlRecordType.COMMIT)
        );
        assertEquals(
            "Expected KRAFT_VOTERS control record type(6), but found COMMIT",
            thrown.getMessage()
        );
    }

    @Test
    public void testDeserializeByteData() {
        testDeserializeRecord(ControlRecordType.KRAFT_VOTERS);
    }

    private void testDeserializeRecord(ControlRecordType controlRecordType) {
        final int voterId = 0;
        final List<Voter> voters = Collections.singletonList(
            new Voter().setVoterId(voterId)
        );
        VotersRecord data = new VotersRecord().setVoters(voters);

        ByteBuffer valueBuffer = ByteBuffer.allocate(256);
        data.write(new ByteBufferAccessor(valueBuffer), new ObjectSerializationCache(), data.highestSupportedVersion());
        valueBuffer.flip();

        byte[] keyData = new byte[]{0, 0, 0, (byte) controlRecordType.type()};

        DefaultRecord record = new DefaultRecord(
            256, (byte) 0, 0, 0L, 0, ByteBuffer.wrap(keyData),  valueBuffer, null
        );

        VotersRecord deserializedData = ControlRecordUtils.deserializeVotersRecord(record);

        assertEquals(voters, deserializedData.voters());
        assertEquals(Collections.singletonList(
            new Voter().setVoterId(voterId)), deserializedData.voters());
    }
}

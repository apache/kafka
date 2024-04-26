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
package org.apache.kafka.raft;

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.ControlRecordType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ControlRecordTest {
    @Test
    void testCtr() {
        // Valid constructions
        new ControlRecord(ControlRecordType.LEADER_CHANGE, new LeaderChangeMessage());
        new ControlRecord(ControlRecordType.SNAPSHOT_HEADER, new SnapshotHeaderRecord());
        new ControlRecord(ControlRecordType.SNAPSHOT_FOOTER, new SnapshotFooterRecord());
        new ControlRecord(ControlRecordType.KRAFT_VERSION, new KRaftVersionRecord());
        new ControlRecord(ControlRecordType.KRAFT_VOTERS, new VotersRecord());

        // Invalid constructions
        assertThrows(
            IllegalArgumentException.class,
            () -> new ControlRecord(ControlRecordType.ABORT, new SnapshotFooterRecord())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ControlRecord(ControlRecordType.LEADER_CHANGE, new SnapshotHeaderRecord())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ControlRecord(ControlRecordType.SNAPSHOT_FOOTER, Mockito.mock(ApiMessage.class))
        );
    }

    @Test
    void testControlRecordTypeValues() {
        // If this test fails then it means that ControlRecordType was changed. Please review the
        // implementation for ControlRecord to see if it needs to be updated based on the changes
        // to ControlRecordType.
        assertEquals(8, ControlRecordType.values().length);
    }
}

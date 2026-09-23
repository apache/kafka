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
package org.apache.kafka.server.share.fetch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordStateTest {

    @Test
    public void testRecordStateValidateTransition() {
        assertThrows(NullPointerException.class, () -> RecordState.AVAILABLE.validateTransition(null));

        // Same-state transitions are always invalid.
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ACQUIRED.validateTransition(RecordState.ACQUIRED));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ARCHIVED));
        assertThrows(IllegalStateException.class, () -> RecordState.TX_PENDING.validateTransition(RecordState.TX_PENDING));

        // ACKNOWLEDGED and ARCHIVED are terminal — no exit transitions.
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ACQUIRED));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ARCHIVED));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ARCHIVING));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.TX_PENDING));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ARCHIVING));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.TX_PENDING));

        // AVAILABLE can only go to ACQUIRED.
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.ARCHIVED));
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.ARCHIVING));
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.TX_PENDING));

        // TX_PENDING can only exit to ACKNOWLEDGED, AVAILABLE, ARCHIVING, or ARCHIVED.
        assertThrows(IllegalStateException.class, () -> RecordState.TX_PENDING.validateTransition(RecordState.ACQUIRED));

        // TX_PENDING can only be entered from ACQUIRED.
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVING.validateTransition(RecordState.TX_PENDING));

        // Valid transitions — existing.
        assertEquals(RecordState.ACQUIRED, RecordState.AVAILABLE.validateTransition(RecordState.ACQUIRED));
        assertEquals(RecordState.AVAILABLE, RecordState.ACQUIRED.validateTransition(RecordState.AVAILABLE));
        assertEquals(RecordState.ACKNOWLEDGED, RecordState.ACQUIRED.validateTransition(RecordState.ACKNOWLEDGED));
        assertEquals(RecordState.ARCHIVED, RecordState.ACQUIRED.validateTransition(RecordState.ARCHIVED));
        assertEquals(RecordState.ARCHIVING, RecordState.ACQUIRED.validateTransition(RecordState.ARCHIVING));
        assertEquals(RecordState.ARCHIVED, RecordState.ARCHIVING.validateTransition(RecordState.ARCHIVED));

        // Valid transitions — KIP-1289 TX_PENDING paths.
        assertEquals(RecordState.TX_PENDING, RecordState.ACQUIRED.validateTransition(RecordState.TX_PENDING));
        assertEquals(RecordState.ACKNOWLEDGED, RecordState.TX_PENDING.validateTransition(RecordState.ACKNOWLEDGED));
        assertEquals(RecordState.AVAILABLE, RecordState.TX_PENDING.validateTransition(RecordState.AVAILABLE));
        assertEquals(RecordState.ARCHIVING, RecordState.TX_PENDING.validateTransition(RecordState.ARCHIVING));
        assertEquals(RecordState.ARCHIVED, RecordState.TX_PENDING.validateTransition(RecordState.ARCHIVED));
    }

    @Test
    public void testRecordStateForId() {
        assertEquals(RecordState.AVAILABLE, RecordState.forId((byte) 0));
        assertEquals(RecordState.ACQUIRED, RecordState.forId((byte) 1));
        assertEquals(RecordState.ACKNOWLEDGED, RecordState.forId((byte) 2));
        assertEquals(RecordState.ARCHIVING, RecordState.forId((byte) 3));
        assertEquals(RecordState.ARCHIVED, RecordState.forId((byte) 4));
        assertEquals(RecordState.TX_PENDING, RecordState.forId((byte) 5));
        assertThrows(IllegalArgumentException.class, () -> RecordState.forId((byte) 6));
    }
}

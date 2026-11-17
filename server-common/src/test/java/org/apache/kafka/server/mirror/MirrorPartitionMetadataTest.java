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
package org.apache.kafka.server.mirror;

import org.apache.kafka.server.common.OffsetAndEpoch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MirrorPartitionMetadataTest {
    @Test
    void testBuilder() {
        MirrorPartitionMetadata metadata = MirrorPartitionMetadata.builder()
            .setState(MirrorPartitionState.FAILED)
            .setStateEpoch(7)
            .setLastMirror(new OffsetAndEpoch(4_294_967_313L, 42))
            .setErrorMessage("error")
            .setRetryAttempt((short) 3)
            .setPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        assertEquals(MirrorPartitionState.FAILED, metadata.state());
        assertEquals(7, metadata.stateEpoch());
        assertEquals(new OffsetAndEpoch(4_294_967_313L, 42), metadata.lastMirror());
        assertEquals("error", metadata.errorMessage());
        assertEquals((short) 3, metadata.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, metadata.previousState());
    }

    @Test
    void testEmpty() {
        MirrorPartitionMetadata empty = MirrorPartitionMetadata.EMPTY;

        assertEquals(MirrorPartitionState.UNKNOWN, empty.state());
        assertEquals(0, empty.stateEpoch());
        assertEquals(new OffsetAndEpoch(-1L, -1), empty.lastMirror());
        assertEquals(MirrorPartitionMetadata.EMPTY_LAST_MIRROR, empty.lastMirror());
        assertNull(empty.errorMessage());
        assertEquals((short) 0, empty.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, empty.previousState());
        assertEquals(empty, MirrorPartitionMetadata.builder().build());
    }

    @Test
    void testNullFieldsAreRejected() {
        assertThrows(NullPointerException.class, () -> MirrorPartitionMetadata.builder().setState(null).build());
        assertThrows(NullPointerException.class, () -> MirrorPartitionMetadata.builder().setLastMirror(null).build());
        assertThrows(NullPointerException.class, () -> MirrorPartitionMetadata.builder().setPreviousState(null).build());
    }

    @Test
    void testEquality() {
        MirrorPartitionMetadata metadata = MirrorPartitionMetadata.builder()
            .setState(MirrorPartitionState.STOPPED)
            .setStateEpoch(2)
            .setLastMirror(new OffsetAndEpoch(100L, 10))
            .setPreviousState(MirrorPartitionState.STOPPING)
            .build();

        assertEquals(metadata, MirrorPartitionMetadata.builder()
            .setState(MirrorPartitionState.STOPPED)
            .setStateEpoch(2)
            .setLastMirror(new OffsetAndEpoch(100L, 10))
            .setPreviousState(MirrorPartitionState.STOPPING)
            .build());
        assertNotEquals(metadata, MirrorPartitionMetadata.builder()
            .setState(MirrorPartitionState.STOPPED)
            .setStateEpoch(3)
            .setLastMirror(new OffsetAndEpoch(100L, 10))
            .setPreviousState(MirrorPartitionState.STOPPING)
            .build());
    }
}

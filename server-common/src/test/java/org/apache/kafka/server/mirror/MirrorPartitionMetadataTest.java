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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MirrorPartitionMetadataTest {
    @Test
    void testAccessors() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata(
            MirrorPartitionState.FAILED, 7, 42, 4_294_967_313L, "error", (short) 3, MirrorPartitionState.MIRRORING);

        assertEquals(MirrorPartitionState.FAILED, metadata.state());
        assertEquals(7, metadata.stateEpoch());
        assertEquals(42, metadata.lastMirrorEpoch());
        assertEquals(4_294_967_313L, metadata.lastMirrorOffset());
        assertEquals("error", metadata.errorMessage());
        assertEquals((short) 3, metadata.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, metadata.previousState());
    }

    @Test
    void testNullErrorMessage() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata(
            MirrorPartitionState.MIRRORING, 1, -1, -1L, null, (short) 0, MirrorPartitionState.UNKNOWN);

        assertNull(metadata.errorMessage());
    }

    @Test
    void testNullStateIsRejected() {
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata(
            null, 0, -1, -1L, null, (short) 0, MirrorPartitionState.UNKNOWN));
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata(
            MirrorPartitionState.MIRRORING, 0, -1, -1L, null, (short) 0, null));
    }

    @Test
    void testEquality() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata(
            MirrorPartitionState.STOPPED, 2, 10, 100L, null, (short) 0, MirrorPartitionState.STOPPING);

        assertEquals(metadata, new MirrorPartitionMetadata(
            MirrorPartitionState.STOPPED, 2, 10, 100L, null, (short) 0, MirrorPartitionState.STOPPING));
        assertEquals(metadata.hashCode(), new MirrorPartitionMetadata(
            MirrorPartitionState.STOPPED, 2, 10, 100L, null, (short) 0, MirrorPartitionState.STOPPING).hashCode());
        assertNotEquals(metadata, new MirrorPartitionMetadata(
            MirrorPartitionState.STOPPED, 3, 10, 100L, null, (short) 0, MirrorPartitionState.STOPPING));
    }
}

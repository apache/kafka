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

import org.apache.kafka.common.mirror.LastMirror;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MirrorPartitionMetadataTest {
    @Test
    void testBuilder() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withStateEpoch(7)
            .withLastMirror(new LastMirror(4_294_967_313L, 42))
            .withErrorMessage(Optional.of("error"))
            .withRetryAttempt((short) 3)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        assertEquals(MirrorPartitionState.FAILED, metadata.state());
        assertEquals(7, metadata.stateEpoch());
        assertEquals(new LastMirror(4_294_967_313L, 42), metadata.lastMirror());
        assertEquals(Optional.of("error"), metadata.errorMessage());
        assertEquals((short) 3, metadata.retryAttempt());
        assertEquals(MirrorPartitionState.MIRRORING, metadata.previousState());
    }

    @Test
    void testEmpty() {
        MirrorPartitionMetadata empty = MirrorPartitionMetadata.EMPTY;

        assertEquals(MirrorPartitionState.UNKNOWN, empty.state());
        assertEquals(0, empty.stateEpoch());
        assertEquals(new LastMirror(-1L, -1), empty.lastMirror());
        assertEquals(LastMirror.EMPTY, empty.lastMirror());
        assertEquals(Optional.empty(), empty.errorMessage());
        assertEquals((short) 0, empty.retryAttempt());
        assertEquals(MirrorPartitionState.UNKNOWN, empty.previousState());
        assertEquals(empty, new MirrorPartitionMetadata.Builder().build());
    }

    @Test
    void testPartialBuilderKeepsDefaults() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateEpoch(3)
            .build();

        assertEquals(MirrorPartitionState.MIRRORING, metadata.state());
        assertEquals(3, metadata.stateEpoch());
        assertEquals(MirrorPartitionMetadata.EMPTY.lastMirror(), metadata.lastMirror());
        assertEquals(Optional.empty(), metadata.errorMessage());
        assertEquals(MirrorPartitionMetadata.EMPTY.retryAttempt(), metadata.retryAttempt());
        assertEquals(MirrorPartitionMetadata.EMPTY.previousState(), metadata.previousState());
    }

    @Test
    void testCopyBuilderChangesOnlyTheSetFields() {
        MirrorPartitionMetadata mirroring = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.MIRRORING)
            .withStateEpoch(3)
            .withLastMirror(new LastMirror(100L, 10))
            .build();

        MirrorPartitionMetadata failed = new MirrorPartitionMetadata.Builder(mirroring)
            .withState(MirrorPartitionState.FAILED)
            .withStateEpoch(4)
            .withErrorMessage(Optional.of("error"))
            .withRetryAttempt((short) 1)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        assertEquals(new MirrorPartitionMetadata(MirrorPartitionState.FAILED, 4, new LastMirror(100L, 10), Optional.of("error"),
            (short) 1, MirrorPartitionState.MIRRORING), failed);
        assertEquals(mirroring, new MirrorPartitionMetadata.Builder(mirroring).build());

        MirrorPartitionMetadata recovered = new MirrorPartitionMetadata.Builder(failed)
            .withState(MirrorPartitionState.MIRRORING)
            .withErrorMessage(Optional.empty())
            .withRetryAttempt((short) 0)
            .build();
        assertEquals(Optional.empty(), recovered.errorMessage());
    }

    @Test
    void testNonRetryableError() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.FAILED)
            .withErrorMessage(Optional.of("error"))
            .withRetryAttempt(MirrorPartitionMetadata.NON_RETRYABLE_ERROR)
            .withPreviousState(MirrorPartitionState.MIRRORING)
            .build();

        assertEquals((short) -1, metadata.retryAttempt());
        assertEquals(MirrorPartitionMetadata.NON_RETRYABLE_ERROR, metadata.retryAttempt());
    }

    @Test
    void testNullFieldsAreRejected() {
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withState(null).build());
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withLastMirror(null).build());
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withErrorMessage(null).build());
        assertThrows(NullPointerException.class, () -> new MirrorPartitionMetadata.Builder().withPreviousState(null).build());
    }

    @Test
    void testEquality() {
        MirrorPartitionMetadata metadata = new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.STOPPED)
            .withStateEpoch(2)
            .withLastMirror(new LastMirror(100L, 10))
            .withPreviousState(MirrorPartitionState.STOPPING)
            .build();

        assertEquals(metadata, new MirrorPartitionMetadata.Builder()
            .withState(MirrorPartitionState.STOPPED)
            .withStateEpoch(2)
            .withLastMirror(new LastMirror(100L, 10))
            .withPreviousState(MirrorPartitionState.STOPPING)
            .build());
        assertNotEquals(metadata, new MirrorPartitionMetadata.Builder(metadata).withStateEpoch(3).build());
    }
}

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
package org.apache.kafka.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.kafka.common.record.BufferSupplier.GrowableBufferSupplier;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClientTestContext;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final public class SnapshotWriterTest {
    private final int localId = 0;
    private final Set<Integer> voters = Collections.singleton(localId);

    @Test
    public void testWritingSnapshot() throws IOException {
        OffsetAndEpoch id = new OffsetAndEpoch(10L, 3);
        List<List<String>> expected = buildRecords(3, 3);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(id)) {
            expected.forEach(batch -> {
                assertDoesNotThrow(() -> snapshot.append(batch));
            });
            snapshot.freeze();
        }

        try (RawSnapshotReader reader = context.log.readSnapshot(id).get()) {
            assertSnapshot(expected, reader);
        }
    }

    @Test
    public void testAbortedSnapshot() throws IOException {
        OffsetAndEpoch id = new OffsetAndEpoch(10L, 3);
        List<List<String>> expected = buildRecords(3, 3);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(id)) {
            expected.forEach(batch -> {
                assertDoesNotThrow(() -> snapshot.append(batch));
            });
        }

        assertFalse(context.log.readSnapshot(id).isPresent());
    }

    @Test
    public void testAppendToFrozenSnapshot() throws IOException {
        OffsetAndEpoch id = new OffsetAndEpoch(10L, 3);
        List<List<String>> expected = buildRecords(3, 3);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters).build();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(id)) {
            expected.forEach(batch -> {
                assertDoesNotThrow(() -> snapshot.append(batch));
            });

            snapshot.freeze();

            assertThrows(RuntimeException.class, () -> snapshot.append(expected.get(0)));
        }
    }

    private List<List<String>> buildRecords(int recordsPerBatch, int batches) {
        Random random = new Random(0);
        List<List<String>> result = new ArrayList<>(batches);
        for (int i = 0; i < batches; i++) {
            List<String> batch = new ArrayList<>(recordsPerBatch);
            for (int j = 0; j < recordsPerBatch; j++) {
                batch.add(String.valueOf(random.nextInt()));
            }
            result.add(batch);
        }

        return result;
    }

    private void assertSnapshot(List<List<String>> batches, RawSnapshotReader reader) {
        List<String> expected = new ArrayList<>();
        batches.forEach(expected::addAll);

        List<String> actual = new ArrayList<>(expected.size());
        reader.forEach(batch -> {
            batch.streamingIterator(new GrowableBufferSupplier()).forEachRemaining(record -> {
                actual.add(Utils.utf8(record.value()));
            });
        });

        assertEquals(expected, actual);
    }
}

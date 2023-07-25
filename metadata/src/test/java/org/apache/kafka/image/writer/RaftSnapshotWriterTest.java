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

package org.apache.kafka.image.writer;

import org.apache.kafka.image.FakeSnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;

import static java.util.Collections.emptyList;
import static org.apache.kafka.metadata.RecordTestUtils.testRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class RaftSnapshotWriterTest {
    @Test
    public void testFreezeAndClose() {
        FakeSnapshotWriter snapshotWriter = new FakeSnapshotWriter();
        RaftSnapshotWriter writer = new RaftSnapshotWriter(snapshotWriter, 2);
        writer.write(testRecord(0));
        writer.write(testRecord(1));
        writer.write(testRecord(2));
        writer.close(true);
        assertTrue(snapshotWriter.isFrozen());
        assertTrue(snapshotWriter.isClosed());
        assertEquals(Arrays.asList(
                Arrays.asList(testRecord(0), testRecord(1)),
                Arrays.asList(testRecord(2))), snapshotWriter.batches());
    }

    @Test
    public void testCloseWithoutFreeze() {
        FakeSnapshotWriter snapshotWriter = new FakeSnapshotWriter();
        RaftSnapshotWriter writer = new RaftSnapshotWriter(snapshotWriter, 2);
        writer.write(testRecord(0));
        writer.close();
        assertFalse(snapshotWriter.isFrozen());
        assertTrue(snapshotWriter.isClosed());
        assertEquals(emptyList(), snapshotWriter.batches());
    }
}

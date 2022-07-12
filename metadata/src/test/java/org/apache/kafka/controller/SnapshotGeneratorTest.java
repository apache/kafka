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

package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.SnapshotGenerator.Section;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.apache.kafka.snapshot.MockRawSnapshotWriter;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class SnapshotGeneratorTest {
    private static final List<List<ApiMessageAndVersion>> BATCHES;

    static {
        BATCHES = Arrays.asList(
            Arrays.asList(new ApiMessageAndVersion(new TopicRecord().
                setName("foo").setTopicId(Uuid.randomUuid()), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new TopicRecord().
                setName("bar").setTopicId(Uuid.randomUuid()), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new TopicRecord().
                setName("baz").setTopicId(Uuid.randomUuid()), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceName("foo").setResourceType(ConfigResource.Type.TOPIC.id()).
                    setName("retention.ms").setValue("10000000"), (short) 0),
                new ApiMessageAndVersion(new ConfigRecord().
                    setResourceName("foo").setResourceType(ConfigResource.Type.TOPIC.id()).
                    setName("max.message.bytes").setValue("100000000"), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new ConfigRecord().
                setResourceName("bar").setResourceType(ConfigResource.Type.TOPIC.id()).
                setName("retention.ms").setValue("5000000"), (short) 0)));
    }

    @Test
    public void testGenerateBatches() throws Exception {
        SnapshotWriter<ApiMessageAndVersion> writer = createSnapshotWriter(123, 0);
        List<Section> sections = Arrays.asList(new Section("replication",
                Arrays.asList(BATCHES.get(0), BATCHES.get(1), BATCHES.get(2)).iterator()),
            new Section("configuration",
                Arrays.asList(BATCHES.get(3), BATCHES.get(4)).iterator()));
        SnapshotGenerator generator = new SnapshotGenerator(new LogContext(),
            writer, 2, sections);
        assertFalse(writer.isFrozen());
        assertEquals(123L, generator.lastContainedLogOffset());
        assertEquals(writer, generator.writer());
        assertEquals(OptionalLong.of(0L), generator.generateBatches());
        assertEquals(OptionalLong.of(0L), generator.generateBatches());
        assertFalse(writer.isFrozen());
        assertEquals(OptionalLong.empty(), generator.generateBatches());
        assertTrue(writer.isFrozen());
    }

    private SnapshotWriter<ApiMessageAndVersion> createSnapshotWriter(
        long committedOffset,
        long lastContainedLogTime
    ) {
        return RecordsSnapshotWriter.createWithHeader(
            () -> createNewSnapshot(new OffsetAndEpoch(committedOffset + 1, 1)),
            1024,
            MemoryPool.NONE,
            new MockTime(),
            lastContainedLogTime,
            CompressionType.NONE,
            new MetadataRecordSerde()
        ).get();
    }

    private Optional<RawSnapshotWriter> createNewSnapshot(
            OffsetAndEpoch snapshotId
    ) {
        return Optional.of(new MockRawSnapshotWriter(snapshotId, buffer -> { }));
    }
}

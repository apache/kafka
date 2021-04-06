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
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.SnapshotGenerator.Section;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

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
        MockSnapshotWriter writer = new MockSnapshotWriter(123);
        ExponentialBackoff exponentialBackoff =
            new ExponentialBackoff(100, 2, 400, 0.0);
        List<Section> sections = Arrays.asList(new Section("replication",
                Arrays.asList(BATCHES.get(0), BATCHES.get(1), BATCHES.get(2)).iterator()),
            new Section("configuration",
                Arrays.asList(BATCHES.get(3), BATCHES.get(4)).iterator()));
        SnapshotGenerator generator = new SnapshotGenerator(new LogContext(),
            writer, 2, exponentialBackoff, sections);
        assertFalse(writer.completed());
        assertEquals(123L, generator.epoch());
        assertEquals(writer, generator.writer());
        assertEquals(OptionalLong.of(0L), generator.generateBatches());
        assertEquals(OptionalLong.of(0L), generator.generateBatches());
        assertFalse(writer.completed());
        assertEquals(OptionalLong.empty(), generator.generateBatches());
        assertTrue(writer.completed());
    }

    @Test
    public void testGenerateBatchesWithBackoff() throws Exception {
        MockSnapshotWriter writer = new MockSnapshotWriter(123);
        ExponentialBackoff exponentialBackoff =
            new ExponentialBackoff(100, 2, 400, 0.0);
        List<Section> sections = Arrays.asList(new Section("replication",
                Arrays.asList(BATCHES.get(0), BATCHES.get(1), BATCHES.get(2)).iterator()),
            new Section("configuration",
                Arrays.asList(BATCHES.get(3), BATCHES.get(4)).iterator()));
        SnapshotGenerator generator = new SnapshotGenerator(new LogContext(),
            writer, 2, exponentialBackoff, sections);
        assertEquals(123L, generator.epoch());
        assertEquals(writer, generator.writer());
        assertFalse(writer.completed());
        assertEquals(OptionalLong.of(0L), generator.generateBatches());
        writer.setReady(false);
        assertEquals(OptionalLong.of(100L), generator.generateBatches());
        assertEquals(OptionalLong.of(200L), generator.generateBatches());
        assertEquals(OptionalLong.of(400L), generator.generateBatches());
        assertEquals(OptionalLong.of(400L), generator.generateBatches());
        writer.setReady(true);
        assertFalse(writer.completed());
        assertEquals(OptionalLong.of(0L), generator.generateBatches());
        assertEquals(OptionalLong.empty(), generator.generateBatches());
        assertTrue(writer.completed());
    }
}
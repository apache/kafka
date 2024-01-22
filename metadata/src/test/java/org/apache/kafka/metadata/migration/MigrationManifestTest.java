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

package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MigrationManifestTest {
    @Test
    public void testEmpty() {
        Time time = new MockTime();
        MigrationManifest.Builder manifestBuilder = MigrationManifest.newBuilder(time);
        MigrationManifest manifest = manifestBuilder.build();
        assertEquals(0L, manifest.durationMs());
        assertEquals(
            "0 records were generated in 0 ms across 0 batches. The record types were {}",
            manifest.toString());
    }

    @Test
    public void testOneBatch() {
        Time time = new MockTime();
        MigrationManifest.Builder manifestBuilder = MigrationManifest.newBuilder(time);
        manifestBuilder.acceptBatch(Arrays.asList(
            new ApiMessageAndVersion(new TopicRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new TopicRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new ConfigRecord(), (short) 0),
            new ApiMessageAndVersion(new ConfigRecord(), (short) 0)
        ));
        MigrationManifest manifest = manifestBuilder.build();
        assertEquals(0L, manifest.durationMs());
        assertEquals(
            "13 records were generated in 0 ms across 1 batches. The record types were {TOPIC_RECORD=2, PARTITION_RECORD=9, CONFIG_RECORD=2}",
            manifest.toString()
        );
    }

    @Test
    public void testManyBatch() {
        Time time = new MockTime();
        MigrationManifest.Builder manifestBuilder = MigrationManifest.newBuilder(time);
        manifestBuilder.acceptBatch(Arrays.asList(
            new ApiMessageAndVersion(new TopicRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0)
        ));
        manifestBuilder.acceptBatch(Arrays.asList(
            new ApiMessageAndVersion(new TopicRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new PartitionRecord(), (short) 0),
            new ApiMessageAndVersion(new ConfigRecord(), (short) 0)
        ));
        manifestBuilder.acceptBatch(Collections.singletonList(
            new ApiMessageAndVersion(new ConfigRecord(), (short) 0)
        ));
        MigrationManifest manifest = manifestBuilder.build();
        assertEquals(0L, manifest.durationMs());
        assertEquals(
            "13 records were generated in 0 ms across 3 batches. The record types were {TOPIC_RECORD=2, PARTITION_RECORD=9, CONFIG_RECORD=2}",
            manifest.toString()
        );
    }
}

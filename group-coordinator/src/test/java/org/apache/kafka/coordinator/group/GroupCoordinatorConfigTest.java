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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupCoordinatorConfigTest {
    @Test
    public void testConfigs() {
        PartitionAssignor assignor = new RangeAssignor();
        GroupCoordinatorConfig config = new GroupCoordinatorConfig(
            10,
            30,
            10,
            55,
            Collections.singletonList(assignor),
            2222,
            3333,
            60,
            3000,
            5 * 60 * 1000,
            120,
            10 * 60 * 1000,
            600000L,
            24 * 60 * 60 * 1000L
        );

        assertEquals(10, config.numThreads);
        assertEquals(30, config.consumerGroupSessionTimeoutMs);
        assertEquals(10, config.consumerGroupHeartbeatIntervalMs);
        assertEquals(55, config.consumerGroupMaxSize);
        assertEquals(Collections.singletonList(assignor), config.consumerGroupAssignors);
        assertEquals(2222, config.offsetsTopicSegmentBytes);
        assertEquals(3333, config.offsetMetadataMaxSize);
        assertEquals(60, config.genericGroupMaxSize);
        assertEquals(3000, config.genericGroupInitialRebalanceDelayMs);
        assertEquals(5 * 60 * 1000, config.genericGroupNewMemberJoinTimeoutMs);
        assertEquals(120, config.genericGroupMinSessionTimeoutMs);
        assertEquals(10 * 60 * 1000, config.genericGroupMaxSessionTimeoutMs);
        assertEquals(10 * 60 * 1000, config.offsetsRetentionCheckIntervalMs);
        assertEquals(24 * 60 * 60 * 1000L, config.offsetsRetentionMs);
    }

    public static GroupCoordinatorConfig createGroupCoordinatorConfig(
        int offsetMetadataMaxSize,
        long offsetsRetentionCheckIntervalMs,
        long offsetsRetentionMs
    ) {
        return new GroupCoordinatorConfig(
            1,
            45,
            5,
            Integer.MAX_VALUE,
            Collections.singletonList(new RangeAssignor()),
            1000,
            offsetMetadataMaxSize,
            Integer.MAX_VALUE,
            3000,
            5 * 60 * 1000,
            120,
            10 * 5 * 1000,
            offsetsRetentionCheckIntervalMs,
            offsetsRetentionMs
        );
    }
}

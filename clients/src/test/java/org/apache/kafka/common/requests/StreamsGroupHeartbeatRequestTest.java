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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StreamsGroupHeartbeatRequestTest {

    private static StreamsGroupHeartbeatRequestData dataWithTaskOffsets() {
        return new StreamsGroupHeartbeatRequestData()
            .setGroupId("group")
            .setMemberId("member")
            .setTaskOffsets(List.of(new StreamsGroupHeartbeatRequestData.TaskOffset()
                .setSubtopologyId("sub")
                .setPartition(0)
                .setOffset(42L)))
            .setTaskEndOffsets(List.of(new StreamsGroupHeartbeatRequestData.TaskOffset()
                .setSubtopologyId("sub")
                .setPartition(0)
                .setOffset(100L)));
    }

    @Test
    public void testBuildClearsTaskOffsetsForVersion0() {
        // TaskOffsets/TaskEndOffsets are only consumed by brokers supporting v1+.
        // Against a v0 coordinator they must be omitted.
        StreamsGroupHeartbeatRequest request = new StreamsGroupHeartbeatRequest.Builder(dataWithTaskOffsets())
            .build((short) 0);

        assertNull(request.data().taskOffsets());
        assertNull(request.data().taskEndOffsets());
    }

    @Test
    public void testBuildKeepsTaskOffsetsForVersion1() {
        StreamsGroupHeartbeatRequest request = new StreamsGroupHeartbeatRequest.Builder(dataWithTaskOffsets())
            .build((short) 1);

        assertEquals(1, request.data().taskOffsets().size());
        assertEquals(42L, request.data().taskOffsets().get(0).offset());
        assertEquals(1, request.data().taskEndOffsets().size());
        assertEquals(100L, request.data().taskEndOffsets().get(0).offset());
    }
}

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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.requests.JoinGroupRequest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerGroupMetadataTest {

    private String groupId = "group";

    @Test
    public void testAssignmentConstructor() {
        String memberId = "member";
        int generationId = 2;
        String groupInstanceId = "instance";

        ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata(groupId,
            generationId, memberId, Optional.of(groupInstanceId));

        assertEquals(groupId, groupMetadata.groupId());
        assertEquals(generationId, groupMetadata.generationId());
        assertEquals(memberId, groupMetadata.memberId());
        assertTrue(groupMetadata.groupInstanceId().isPresent());
        assertEquals(groupInstanceId, groupMetadata.groupInstanceId().get());
    }

    @Test
    public void testGroupIdConstructor() {
        ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata(groupId);

        assertEquals(groupId, groupMetadata.groupId());
        assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
        assertFalse(groupMetadata.groupInstanceId().isPresent());
    }

    @Test
    public void testInvalidGroupId() {
        String memberId = "member";
        int generationId = 2;

        assertThrows(NullPointerException.class, () -> new ConsumerGroupMetadata(
            null, generationId, memberId, Optional.empty())
        );
    }

    @Test
    public void testInvalidMemberId() {
        int generationId = 2;

        assertThrows(NullPointerException.class, () -> new ConsumerGroupMetadata(
            groupId, generationId, null, Optional.empty())
        );
    }

    @Test
    public void testInvalidInstanceId() {
        String memberId = "member";
        int generationId = 2;

        assertThrows(NullPointerException.class, () -> new ConsumerGroupMetadata(
            groupId, generationId, memberId, null)
        );
    }
}

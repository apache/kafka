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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class GroupSpecImplTest {

    private Map<String, MemberMetadataAndStateImpl> members;
    private MemberMetadataAndStateImpl member;
    private GroupSpecImpl groupSpec;

    @BeforeEach
    void setUp() {
        members = new HashMap<>();

        member = new MemberMetadataAndStateImpl(
            Optional.of("test-instance"),
            Optional.of("test-rack"),
            "test-process",
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
        members.put("test-member", member);

        groupSpec = new GroupSpecImpl(
            members,
            new AssignmentConfigsImpl(2, List.of("test-tag"))
        );
    }

    @Test
    void testMemberIds() {
        assertEquals(members.keySet(), groupSpec.memberIds());
    }

    @Test
    void testMemberMetadata() {
        assertEquals(member, groupSpec.memberMetadata("test-member"));
    }

    @Test
    void testMemberAssignmentState() {
        assertEquals(member, groupSpec.memberAssignmentState("test-member"));
    }

    @Test
    void testMemberNotFound() {
        assertThrows(IllegalArgumentException.class, () -> groupSpec.memberMetadata("unknown"));
        assertThrows(IllegalArgumentException.class, () -> groupSpec.memberAssignmentState("unknown"));
    }

    @Test
    void testConfigs() {
        assertEquals(2, groupSpec.configs().numStandbyReplicas());
        assertEquals(List.of("test-tag"), groupSpec.configs().rackAwareAssignmentTags());
    }

    @Test
    void testMembersAndConfigsAreUnmodifiable() {
        assertThrows(UnsupportedOperationException.class, () -> groupSpec.members().put("other-member", member));
        assertThrows(UnsupportedOperationException.class, () -> groupSpec.configs().rackAwareAssignmentTags().add("other-tag"));
    }

}

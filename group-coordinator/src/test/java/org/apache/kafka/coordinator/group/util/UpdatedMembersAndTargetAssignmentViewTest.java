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
package org.apache.kafka.coordinator.group.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdatedMembersAndTargetAssignmentViewTest {

    /**
     * A test member.
     */
    private record Member(String name, String instanceId) { }

    /**
     * Creates an {@link UpdatedMembersAndTargetAssignmentView} with two members, one static and one
     * non-static.
     */
    private static UpdatedMembersAndTargetAssignmentView<Member, String> createView() {
        return new UpdatedMembersAndTargetAssignmentView<>(
            Map.of(
                "member-1", new Member("Member1", null),
                "member-2", new Member("Member2", "instance-id")
            ),
            Map.of(
                "instance-id", "member-2"
            ),
            Map.of(
                "member-1", "Assignment-member-1",
                "member-2", "Assignment-member-2"
            ),
            Member::instanceId
        );
    }

    @Test
    public void testAddMember() {
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.addOrUpdateMember("member-3", new Member("Member3", null));

        assertEquals(Map.of(
            "member-1", new Member("Member1", null),
            "member-2", new Member("Member2", "instance-id"),
            "member-3", new Member("Member3", null)
        ), view.members());
        assertEquals(Map.of(
            "instance-id", "member-2"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-2", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testAddStaticMember() {
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.addOrUpdateMember("member-3", new Member("Member3", "instance-id-2"));

        assertEquals(Map.of(
            "member-1", new Member("Member1", null),
            "member-2", new Member("Member2", "instance-id"),
            "member-3", new Member("Member3", "instance-id-2")
        ), view.members());
        assertEquals(Map.of(
            "instance-id", "member-2",
            "instance-id-2", "member-3"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-2", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testReplaceMember() {
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.addOrUpdateMember("member-1", new Member("Member1-updated", null));

        assertEquals(Map.of(
            "member-1", new Member("Member1-updated", null),
            "member-2", new Member("Member2", "instance-id")
        ), view.members());
        assertEquals(Map.of(
            "instance-id", "member-2"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-2", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testReplaceStaticMemberWithSameMemberId() {
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.addOrUpdateMember("member-2", new Member("Member2-updated", "instance-id"));

        assertEquals(Map.of(
            "member-1", new Member("Member1", null),
            "member-2", new Member("Member2-updated", "instance-id")
        ), view.members());
        assertEquals(Map.of(
            "instance-id", "member-2"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-2", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testReplaceStaticMemberWithDifferentMemberId() {
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.addOrUpdateMember("member-3", new Member("Member3", "instance-id"));

        assertEquals(Map.of(
            "member-1", new Member("Member1", null),
            "member-3", new Member("Member3", "instance-id")
        ), view.members());
        assertEquals(Map.of(
            "instance-id", "member-3"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-3", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testReplaceStaticMemberWithNullInstanceId() {
        // This operation is not possible, since a heartbeat with a null instance id will keep any
        // existing instance id.
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.addOrUpdateMember("member-2", new Member("Member2-updated", null));

        assertEquals(Map.of(
            "member-1", new Member("Member1", null),
            "member-2", new Member("Member2-updated", null)
        ), view.members());
        assertEquals(Map.of(), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-2", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testReplaceStaticMemberWithDifferentInstanceId() {
        // This operation will never happen with the official Java client and may be forbidden in
        // the future.
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.addOrUpdateMember("member-2", new Member("Member2-updated", "instance-id-2"));

        assertEquals(Map.of(
            "member-1", new Member("Member1", null),
            "member-2", new Member("Member2-updated", "instance-id-2")
        ), view.members());
        assertEquals(Map.of(
            "instance-id-2", "member-2"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-2", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testRemoveMember() {
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.removeMember("member-1");

        assertEquals(Map.of(
            "member-2", new Member("Member2", "instance-id")
        ), view.members());
        assertEquals(Map.of(
            "instance-id", "member-2"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-2", "Assignment-member-2"
        ), view.targetAssignment());
    }

    @Test
    public void testRemoveStaticMember() {
        UpdatedMembersAndTargetAssignmentView<Member, String> view = createView();

        view.removeMember("member-2");

        assertEquals(Map.of(
            "member-1", new Member("Member1", null)
        ), view.members());
        assertEquals(Map.of(), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1"
        ), view.targetAssignment());
    }
}

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
     * Creates an {@link UpdatedMembersAndTargetAssignmentView} with two members, one static and one
     * non-static.
     */
    private static UpdatedMembersAndTargetAssignmentView<String, String> createView() {
        return new UpdatedMembersAndTargetAssignmentView<>(
            Map.of(
                "member-1", "Member1",
                "member-2", "Member2"
            ),
            Map.of(
                "instance-id", "member-2"
            ),
            Map.of(
                "member-1", "Assignment-member-1",
                "member-2", "Assignment-member-2"
            )
        );
    }

    @Test
    public void testAddMember() {
        UpdatedMembersAndTargetAssignmentView<String, String> view = createView();

        view.addOrUpdateMember("member-3", null, "Member3");

        assertEquals(Map.of(
            "member-1", "Member1",
            "member-2", "Member2",
            "member-3", "Member3"
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
        UpdatedMembersAndTargetAssignmentView<String, String> view = createView();

        view.addOrUpdateMember("member-3", "instance-id-2", "Member3");

        assertEquals(Map.of(
            "member-1", "Member1",
            "member-2", "Member2",
            "member-3", "Member3"
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
        UpdatedMembersAndTargetAssignmentView<String, String> view = createView();

        view.addOrUpdateMember("member-1", null, "Member1-updated");

        assertEquals(Map.of(
            "member-1", "Member1-updated",
            "member-2", "Member2"
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
        UpdatedMembersAndTargetAssignmentView<String, String> view = createView();

        view.addOrUpdateMember("member-2", "instance-id", "Member2-updated");

        assertEquals(Map.of(
            "member-1", "Member1",
            "member-2", "Member2-updated"
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
        UpdatedMembersAndTargetAssignmentView<String, String> view = createView();

        view.addOrUpdateMember("member-3", "instance-id", "Member3");

        assertEquals(Map.of(
            "member-1", "Member1",
            "member-3", "Member3"
        ), view.members());
        assertEquals(Map.of(
            "instance-id", "member-3"
        ), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1",
            "member-3", "Assignment-member-2"
        ), view.targetAssignment());

        // Removing the previous static member does not change the new static member's assignment.
        view.removeMember("member-2", "instance-id");

        assertEquals(Map.of(
            "member-1", "Member1",
            "member-3", "Member3"
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
    public void testRemoveMember() {
        UpdatedMembersAndTargetAssignmentView<String, String> view = createView();

        view.removeMember("member-1", null);

        assertEquals(Map.of(
            "member-2", "Member2"
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
        UpdatedMembersAndTargetAssignmentView<String, String> view = createView();

        view.removeMember("member-2", "instance-id");

        assertEquals(Map.of(
            "member-1", "Member1"
        ), view.members());
        assertEquals(Map.of(), view.staticMembers());
        assertEquals(Map.of(
            "member-1", "Assignment-member-1"
        ), view.targetAssignment());
    }
}

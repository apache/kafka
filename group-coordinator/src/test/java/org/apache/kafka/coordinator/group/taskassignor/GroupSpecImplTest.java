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
package org.apache.kafka.coordinator.group.taskassignor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class GroupSpecImplTest {

    private Map<String, AssignmentMemberSpec> members;
    private List<String> subtopologies;
    private GroupSpecImpl groupSpec;

    @BeforeEach
    void setUp() {
        members = new HashMap<>();
        subtopologies = new ArrayList<>();

        members.put("test-member", new AssignmentMemberSpec(
            Optional.of("test-instance"),
            Optional.of("test-rack"),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "test-process",
            Collections.emptyMap(),
            Collections.emptyMap()
        ));

        subtopologies.add("test-subtopology");

        groupSpec = new GroupSpecImpl(
            members,
            subtopologies
        );
    }

    @Test
    void testMembers() {
        assertEquals(members, groupSpec.members());
    }

    @Test
    void testSubtopologies() {
        assertEquals(subtopologies, groupSpec.subtopologies());
    }
}

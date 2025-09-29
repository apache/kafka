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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class MemberDescriptionTest {

    private static final String MEMBER_ID = "member_id";
    private static final Optional<String> INSTANCE_ID = Optional.of("instanceId");
    private static final String CLIENT_ID = "client_id";
    private static final String HOST = "host";
    private static final MemberAssignment ASSIGNMENT;
    private static final MemberDescription STATIC_MEMBER_DESCRIPTION;

    static {
        ASSIGNMENT = new MemberAssignment(Collections.singleton(new TopicPartition("topic", 1)));
        STATIC_MEMBER_DESCRIPTION = new MemberDescription(MEMBER_ID,
                                                          INSTANCE_ID,
                                                          CLIENT_ID,
                                                          HOST,
                                                          ASSIGNMENT);
    }

    @Test
    public void testEqualsWithoutGroupInstanceId() {
        MemberDescription dynamicMemberDescription = new MemberDescription(MEMBER_ID,
                                                                           CLIENT_ID,
                                                                           HOST,
                                                                           ASSIGNMENT);

        MemberDescription identityDescription = new MemberDescription(MEMBER_ID,
                                                                      CLIENT_ID,
                                                                      HOST,
                                                                      ASSIGNMENT);

        assertNotEquals(STATIC_MEMBER_DESCRIPTION, dynamicMemberDescription);
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), dynamicMemberDescription.hashCode());

        // Check self equality.
        assertEquals(dynamicMemberDescription, dynamicMemberDescription);
        assertEquals(dynamicMemberDescription, identityDescription);
        assertEquals(dynamicMemberDescription.hashCode(), identityDescription.hashCode());
    }

    @Test
    public void testEqualsWithGroupInstanceId() {
        // Check self equality.
        assertEquals(STATIC_MEMBER_DESCRIPTION, STATIC_MEMBER_DESCRIPTION);

        MemberDescription identityDescription = new MemberDescription(MEMBER_ID,
                                                                      INSTANCE_ID,
                                                                      CLIENT_ID,
                                                                      HOST,
                                                                      ASSIGNMENT);

        assertEquals(STATIC_MEMBER_DESCRIPTION, identityDescription);
        assertEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), identityDescription.hashCode());
    }

    @Test
    public void testNonEqual() {
        MemberDescription newMemberDescription = new MemberDescription("new_member",
                                                                       INSTANCE_ID,
                                                                       CLIENT_ID,
                                                                       HOST,
                                                                       ASSIGNMENT);

        assertNotEquals(STATIC_MEMBER_DESCRIPTION, newMemberDescription);
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), newMemberDescription.hashCode());

        MemberDescription newInstanceDescription = new MemberDescription(MEMBER_ID,
                                                                         Optional.of("new_instance"),
                                                                         CLIENT_ID,
                                                                         HOST,
                                                                         ASSIGNMENT);

        assertNotEquals(STATIC_MEMBER_DESCRIPTION, newInstanceDescription);
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), newInstanceDescription.hashCode());

        MemberDescription newTargetAssignmentDescription = new MemberDescription(MEMBER_ID,
                                                                                 INSTANCE_ID,
                                                                                 CLIENT_ID,
                                                                                 HOST,
                                                                                 ASSIGNMENT,
                                                                                 Optional.of(ASSIGNMENT),
                                                                                 Optional.empty(),
                                                                                 Optional.empty());
        assertNotEquals(STATIC_MEMBER_DESCRIPTION, newTargetAssignmentDescription);
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), newTargetAssignmentDescription.hashCode());

        MemberDescription newMemberEpochDescription = new MemberDescription(MEMBER_ID,
                                                                            INSTANCE_ID,
                                                                            CLIENT_ID,
                                                                            HOST,
                                                                            ASSIGNMENT,
                                                                            Optional.empty(),
                                                                            Optional.of(1),
                                                                            Optional.empty());
        assertNotEquals(STATIC_MEMBER_DESCRIPTION, newMemberEpochDescription);
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), newMemberEpochDescription.hashCode());

        MemberDescription newIsClassicDescription = new MemberDescription(MEMBER_ID,
                                                                          INSTANCE_ID,
                                                                          CLIENT_ID,
                                                                          HOST,
                                                                          ASSIGNMENT,
                                                                          Optional.empty(),
                                                                          Optional.empty(),
                                                                          Optional.of(false));
        assertNotEquals(STATIC_MEMBER_DESCRIPTION, newIsClassicDescription);
        assertNotEquals(STATIC_MEMBER_DESCRIPTION.hashCode(), newIsClassicDescription.hashCode());
    }
}

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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.ShareGroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareGroupSpecImplTest {
    private static final String TEST_MEMBER = "test-member";
    private ShareGroupSpecImpl groupSpec;
    private Uuid topicId;

    @BeforeEach
    void setUp() {
        topicId = Uuid.randomUuid();
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId, "topic", 3)
            .buildCoordinatorMetadataImage();
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();
        SubscriptionType subscriptionType = SubscriptionType.HOMOGENEOUS;

        members.put(TEST_MEMBER, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topicId),
            Assignment.EMPTY
        ));

        groupSpec = new ShareGroupSpecImpl(
            metadataImage,
            members,
            subscriptionType,
            Map.of(topicId, new InitMapValue("topic", Set.of(0), 0L))
        );
    }

    @Test
    void testIsPartitionAssignable() {
        // Only initialized partitions are assignable in share groups.
        assertTrue(groupSpec.isPartitionAssignable(topicId, 0));
        assertFalse(groupSpec.isPartitionAssignable(topicId, 1));
    }
}

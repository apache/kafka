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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MemberSubscriptionAndAssignmentImplTest {

    @Test
    public void testPartitionsImmutable() {
        Uuid topicId = Uuid.randomUuid();
        Map<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId, new HashSet<>(Set.of(0, 1)));

        MemberSubscriptionAndAssignmentImpl member = new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(), Optional.empty(), Set.of(topicId), new Assignment(partitions)
        );

        assertThrows(UnsupportedOperationException.class, () -> member.partitions().clear());
        assertThrows(UnsupportedOperationException.class, () -> member.partitions().get(topicId).remove(0));
        assertEquals(Map.of(topicId, Set.of(0, 1)), partitions);
    }

    @Test
    public void testAssignmentIsolatedFromSourceSet() {
        Uuid topicId = Uuid.randomUuid();
        Set<Integer> partitionIds = new HashSet<>(Set.of(0, 1));
        Map<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId, partitionIds);

        MemberSubscriptionAndAssignmentImpl member = new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(), Optional.empty(), Set.of(topicId), new Assignment(partitions)
        );

        partitionIds.remove(0);

        assertEquals(Map.of(topicId, Set.of(0, 1)), member.partitions());
    }

    @Test
    public void testAssignmentIsolatedFromSourceMap() {
        Uuid topicId = Uuid.randomUuid();
        Map<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId, new HashSet<>(Set.of(0, 1)));

        MemberSubscriptionAndAssignmentImpl member = new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(), Optional.empty(), Set.of(topicId), new Assignment(partitions)
        );

        partitions.clear();

        assertEquals(Map.of(topicId, Set.of(0, 1)), member.partitions());
    }
}

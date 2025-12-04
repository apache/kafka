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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class MemberAssignmentImplTest {

    @Test
    public void testPartitionsMutable() {
        // We depend on the the map inside MemberAssignmentImpl remaining mutable in the server-side
        // assignors, otherwise we end up deep copying the map unnecessarily.
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        HashMap<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId1, new HashSet<>());

        MemberAssignmentImpl memberAssignment = new MemberAssignmentImpl(partitions);

        // The map should remain mutable.
        assertDoesNotThrow(() -> memberAssignment.partitions().put(topicId2, Set.of(3, 4, 5)));

        // The sets inside the map should remain mutable.
        assertDoesNotThrow(() -> memberAssignment.partitions().get(topicId1).add(3));
    }
}

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
package org.apache.kafka.coordinator.group.assignor.uniform2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The properties that every assignment of the uniform2 assignor must have, checked on the group
 * spec it was computed from.
 */
public final class AssignmentTestUtils {
    private AssignmentTestUtils() { }

    /**
     * Asserts that the assignment has the properties of the assignor:
     * <ul>
     *     <li>every member of the group is in it;</li>
     *     <li>every partition of every subscribed topic is assigned exactly once, to a
     *     subscriber;</li>
     *     <li>every subscriber of a topic gets its minimum quota or one more;</li>
     *     <li>no extra partition could move between two subscribers so that their assignment sizes
     *     get closer by two;</li>
     *     <li>with a single subscription, all assignment sizes are within one of each other.</li>
     * </ul>
     */
    public static void assertValidAssignment(GroupSpec spec, SubscribedTopicDescriber describer, GroupAssignment result) {
        assertValidAssignment(spec, describer, result, "");
    }

    /**
     * As {@link #assertValidAssignment(GroupSpec, SubscribedTopicDescriber, GroupAssignment)},
     * with a context that prefixes the failure messages.
     */
    public static void assertValidAssignment(
        GroupSpec spec,
        SubscribedTopicDescriber describer,
        GroupAssignment result,
        String context
    ) {
        assertEquals(spec.memberIds(), result.members().keySet(), context);
        var sizes = assertAssignedOnce(spec, describer, result, context);

        var topics = new HashSet<Uuid>();
        spec.memberIds().forEach(id -> topics.addAll(subscribedTopicIds(spec, id)));
        // The topics which do not exist have no partitions to assign.
        topics.removeIf(topicId -> describer.numPartitions(topicId) < 0);
        for (Uuid topicId : topics) {
            assertSpread(spec, describer, result, sizes, topicId, context);
        }

        if (spec.subscriptionType() == SubscriptionType.HOMOGENEOUS && !sizes.isEmpty()) {
            int min = Collections.min(sizes.values());
            int max = Collections.max(sizes.values());
            assertTrue(max - min <= 1, context + ": sizes are not within one of each other: " + sizes);
        }
    }

    /**
     * Checks that every member only gets partitions of the existing topics it subscribes to, never
     * in an empty set, and that every partition of every subscribed topic is assigned exactly once.
     *
     * @return The assignment size of every member.
     */
    private static Map<String, Integer> assertAssignedOnce(
        GroupSpec spec,
        SubscribedTopicDescriber describer,
        GroupAssignment result,
        String context
    ) {
        var owners = new HashMap<Uuid, Map<Integer, String>>();
        var sizes = new HashMap<String, Integer>();
        for (Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            var id = entry.getKey();
            int size = 0;
            for (Map.Entry<Uuid, Set<Integer>> topicEntry : entry.getValue().partitions().entrySet()) {
                var topicId = topicEntry.getKey();
                assertTrue(subscribedTopicIds(spec, id).contains(topicId),
                    context + ": " + id + " is not subscribed to " + topicId);
                assertFalse(topicEntry.getValue().isEmpty(), context + ": empty partition set for " + id + " and " + topicId);
                int numPartitions = describer.numPartitions(topicId);
                for (int partition : topicEntry.getValue()) {
                    assertTrue(partition >= 0 && partition < numPartitions,
                        context + ": " + topicId + "-" + partition + " does not exist");
                    assertNull(owners.computeIfAbsent(topicId, k -> new HashMap<>()).put(partition, id),
                        context + ": " + topicId + "-" + partition + " is assigned twice");
                    size++;
                }
            }
            sizes.put(id, size);
        }
        for (String id : spec.memberIds()) {
            for (Uuid topicId : subscribedTopicIds(spec, id)) {
                int numPartitions = describer.numPartitions(topicId);
                if (numPartitions >= 0) {
                    assertEquals(numPartitions, owners.getOrDefault(topicId, Map.of()).size(),
                        context + ": topic " + topicId + " is not fully assigned");
                }
            }
        }
        return sizes;
    }

    /**
     * Checks that the subscribers of the topic get its minimum quota or one more, and that no extra
     * partition could move between two of them so that their assignment sizes get closer by two.
     */
    private static void assertSpread(
        GroupSpec spec,
        SubscribedTopicDescriber describer,
        GroupAssignment result,
        Map<String, Integer> sizes,
        Uuid topicId,
        String context
    ) {
        var subscribers = new ArrayList<String>();
        for (String id : spec.memberIds()) {
            if (subscribedTopicIds(spec, id).contains(topicId)) {
                subscribers.add(id);
            }
        }
        int minQuota = describer.numPartitions(topicId) / subscribers.size();
        var withExtra = new ArrayList<String>();
        var withoutExtra = new ArrayList<String>();
        for (String id : subscribers) {
            int count = result.members().get(id).partitions().getOrDefault(topicId, Set.of()).size();
            assertTrue(count == minQuota || count == minQuota + 1,
                context + ": " + id + " has " + count + " partitions of " + topicId + " with a minimum quota of " + minQuota);
            (count == minQuota + 1 ? withExtra : withoutExtra).add(id);
        }
        for (String giver : withExtra) {
            for (String receiver : withoutExtra) {
                assertTrue(sizes.get(giver) < sizes.get(receiver) + 2,
                    context + ": an extra partition of " + topicId + " could move from " + giver + " of size "
                        + sizes.get(giver) + " to " + receiver + " of size " + sizes.get(receiver));
            }
        }
    }

    private static Set<Uuid> subscribedTopicIds(GroupSpec spec, String memberId) {
        return spec.memberSubscription(memberId).subscribedTopicIds();
    }
}

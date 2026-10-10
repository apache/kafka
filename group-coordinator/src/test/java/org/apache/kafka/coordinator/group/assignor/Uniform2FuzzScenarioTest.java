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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Event;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Kind;
import org.apache.kafka.coordinator.group.assignor.Uniform2FuzzScenario.Size;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the scenario generator of the fuzzer: a seed determines the scenario, a few seeds cover
 * every size, kind of event and kind of stale input, and the assignments applied keep the partitions
 * held consistent.
 */
public class Uniform2FuzzScenarioTest {
    /**
     * The number of seeds every test goes through.
     */
    private static final int SEEDS = 100;

    /**
     * The number of events of every seed.
     */
    private static final int EVENTS = 20;

    @Test
    public void testSeedDeterminesTheScenario() {
        for (long seed = 0; seed < SEEDS; seed++) {
            Uniform2FuzzScenario first = new Uniform2FuzzScenario(seed);
            Uniform2FuzzScenario second = new Uniform2FuzzScenario(seed);
            assertEquals(first.dump(), second.dump());
            for (int i = 0; i < EVENTS; i++) {
                assertEquals(first.mutate(), second.mutate(), "seed " + seed);
                assertEquals(first.dump(), second.dump(), "seed " + seed);
            }
        }
    }

    /**
     * Every size, kind of event and subscription type happens, a homogeneous group having at least
     * two members, and so does every kind of stale input: a subscription to a topic that does not
     * exist, partitions held of a topic that does not exist or beyond the partition count, an
     * empty subscription. Once an assignment of the assignor is applied, every member holds only
     * partitions of its topics that exist.
     */
    @Test
    public void testEventsCoverEveryKindAndInput() {
        Uniform2Assignor assignor = new Uniform2Assignor();
        Set<Kind> kinds = EnumSet.noneOf(Kind.class);
        Set<Size> sizes = EnumSet.noneOf(Size.class);
        Set<String> staleInputs = new TreeSet<>();
        Set<SubscriptionType> subscriptionTypes = EnumSet.noneOf(SubscriptionType.class);
        for (long seed = 0; seed < SEEDS; seed++) {
            Uniform2FuzzScenario scenario = new Uniform2FuzzScenario(seed);
            sizes.add(scenario.size());
            applyAssignment(scenario, assignor, "seed " + seed + " initial");
            for (int i = 0; i < EVENTS; i++) {
                Event event = scenario.mutate();
                kinds.add(event.kind());
                staleInputs.addAll(staleInputs(scenario));
                SubscriptionType subscriptionType = scenario.spec().subscriptionType();
                if (subscriptionType == SubscriptionType.HETEROGENEOUS || scenario.memberCount() > 1) {
                    subscriptionTypes.add(subscriptionType);
                }
                applyAssignment(scenario, assignor, "seed " + seed + " after " + event);
            }
        }
        assertEquals(EnumSet.complementOf(EnumSet.of(Kind.INIT)), kinds, "every kind of event happens");
        assertEquals(EnumSet.allOf(Size.class), sizes, "every size is generated");
        assertEquals(EnumSet.allOf(SubscriptionType.class), subscriptionTypes, "both subscription types are generated");
        assertEquals(Set.of("beyond the partition count", "empty subscription", "held topic missing", "subscribed topic missing"),
            staleInputs, "every kind of stale input is generated");
    }

    @Test
    public void testApplyReplacesTheCurrentPartitions() {
        Uniform2FuzzScenario scenario = new Uniform2FuzzScenario(7);
        GroupSpec spec = scenario.spec();
        assertTrue(spec.memberIds().stream().allMatch(id -> spec.memberAssignment(id).partitions().isEmpty()),
            "nothing is held at first");

        GroupAssignment result = new Uniform2Assignor().assign(spec, scenario.describer());
        scenario.apply(result);

        Map<String, Map<Uuid, Set<Integer>>> assigned = new HashMap<>();
        result.members().forEach((id, member) -> assigned.put(id, member.partitions()));
        GroupSpec after = scenario.spec();
        Map<String, Map<Uuid, Set<Integer>>> held = new HashMap<>();
        after.memberIds().forEach(id -> held.put(id, after.memberAssignment(id).partitions()));
        assertEquals(assigned, held);
        assertTrue(scenario.dump().contains("current={T"), "the dump shows the current partitions");
    }

    /**
     * Applies the assignment of the assignor, then checks that every member holds only partitions
     * of topics it subscribes to that exist, below their partition count.
     */
    private static void applyAssignment(Uniform2FuzzScenario scenario, Uniform2Assignor assignor, String context) {
        scenario.apply(assignor.assign(scenario.spec(), scenario.describer()));
        GroupSpec spec = scenario.spec();
        SubscribedTopicDescriber describer = scenario.describer();
        assertEquals(scenario.memberCount(), spec.memberIds().size(), context);
        for (String id : spec.memberIds()) {
            Set<Uuid> subscription = spec.memberSubscription(id).subscribedTopicIds();
            String memberContext = context + " member " + id;
            spec.memberAssignment(id).partitions().forEach((topicId, partitions) -> {
                assertTrue(subscription.contains(topicId), memberContext + " holds " + topicId + " without subscribing to it");
                assertFalse(partitions.isEmpty(), memberContext + " holds an empty set of " + topicId);
                partitions.forEach(partition -> assertTrue(partition >= 0 && partition < describer.numPartitions(topicId),
                    memberContext + " holds the unknown partition " + topicId + "-" + partition));
            });
        }
    }

    /**
     * @return The kinds of stale input in the scenario, as the assignor would receive it.
     */
    private static Set<String> staleInputs(Uniform2FuzzScenario scenario) {
        SubscribedTopicDescriber describer = scenario.describer();
        Set<String> stale = new TreeSet<>();
        GroupSpec spec = scenario.spec();
        for (String id : spec.memberIds()) {
            Set<Uuid> subscription = spec.memberSubscription(id).subscribedTopicIds();
            if (subscription.isEmpty()) {
                stale.add("empty subscription");
            }
            for (Uuid topicId : subscription) {
                if (describer.numPartitions(topicId) < 0) {
                    stale.add("subscribed topic missing");
                }
            }
            spec.memberAssignment(id).partitions().forEach((topicId, partitions) -> {
                int partitionCount = describer.numPartitions(topicId);
                if (partitionCount < 0) {
                    stale.add("held topic missing");
                } else if (partitions.stream().anyMatch(partition -> partition >= partitionCount)) {
                    stale.add("beyond the partition count");
                }
            });
        }
        return stale;
    }
}

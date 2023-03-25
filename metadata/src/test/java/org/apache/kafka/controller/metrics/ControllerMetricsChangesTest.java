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

package org.apache.kafka.controller.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.FakePartitionRegistrationType.NORMAL;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.FakePartitionRegistrationType.NON_PREFERRED_LEADER;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.FakePartitionRegistrationType.OFFLINE;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.fakePartitionRegistration;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ControllerMetricsChangesTest {
    @Test
    public void testDelta() {
        assertEquals(0, ControllerMetricsChanges.delta(false, false));
        assertEquals(1, ControllerMetricsChanges.delta(false, true));
        assertEquals(-1, ControllerMetricsChanges.delta(true, false));
        assertEquals(0, ControllerMetricsChanges.delta(true, true));
    }

    private static BrokerRegistration brokerRegistration(
        int brokerId,
        boolean fenced
    ) {
        return new BrokerRegistration(brokerId,
                100L,
                Uuid.fromString("Pxi6QwS2RFuN8VSKjqJZyQ"),
                Collections.emptyList(),
                Collections.emptyMap(),
                Optional.empty(),
                fenced,
                false);
    }

    @Test
    public void testInitialValues() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        assertEquals(0, changes.fencedBrokersChange());
        assertEquals(0, changes.activeBrokersChange());
        assertEquals(0, changes.globalTopicsChange());
        assertEquals(0, changes.globalPartitionsChange());
        assertEquals(0, changes.offlinePartitionsChange());
        assertEquals(0, changes.partitionsWithoutPreferredLeaderChange());
    }

    @Test
    public void testHandleNewUnfencedBroker() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        changes.handleBrokerChange(null, brokerRegistration(1, false));
        assertEquals(0, changes.fencedBrokersChange());
        assertEquals(1, changes.activeBrokersChange());
    }

    @Test
    public void testHandleNewFencedBroker() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        changes.handleBrokerChange(null, brokerRegistration(1, true));
        assertEquals(1, changes.fencedBrokersChange());
        assertEquals(0, changes.activeBrokersChange());
    }

    @Test
    public void testHandleBrokerFencing() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        changes.handleBrokerChange(brokerRegistration(1, false), brokerRegistration(1, true));
        assertEquals(1, changes.fencedBrokersChange());
        assertEquals(-1, changes.activeBrokersChange());
    }

    @Test
    public void testHandleBrokerUnfencing() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        changes.handleBrokerChange(brokerRegistration(1, true), brokerRegistration(1, false));
        assertEquals(-1, changes.fencedBrokersChange());
        assertEquals(1, changes.activeBrokersChange());
    }

    @Test
    public void testHandleDeletedTopic() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        Map<Integer, PartitionRegistration> partitions = new HashMap<>();
        partitions.put(0, fakePartitionRegistration(NORMAL));
        partitions.put(1, fakePartitionRegistration(NORMAL));
        partitions.put(2, fakePartitionRegistration(NON_PREFERRED_LEADER));
        partitions.put(3, fakePartitionRegistration(NON_PREFERRED_LEADER));
        partitions.put(4, fakePartitionRegistration(OFFLINE));
        TopicImage topicImage = new TopicImage("foo",
                Uuid.fromString("wXtW6pQbTS2CL6PjdRCqVw"),
                partitions);
        changes.handleDeletedTopic(topicImage);
        assertEquals(-1, changes.globalTopicsChange());
        assertEquals(-5, changes.globalPartitionsChange());
        assertEquals(-1, changes.offlinePartitionsChange());
        // The offline partition counts as a partition without its preferred leader.
        assertEquals(-3, changes.partitionsWithoutPreferredLeaderChange());
    }

    static final Uuid FOO_ID = Uuid.fromString("wXtW6pQbTS2CL6PjdRCqVw");

    static final TopicDelta TOPIC_DELTA1;

    static final TopicDelta TOPIC_DELTA2;

    static {
        TOPIC_DELTA1 = new TopicDelta(new TopicImage("foo", FOO_ID, Collections.emptyMap()));
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 0).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 1).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 2).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NON_PREFERRED_LEADER).
                toRecord(FOO_ID, 3).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NON_PREFERRED_LEADER).
                toRecord(FOO_ID, 4).message());

        TOPIC_DELTA2 = new TopicDelta(TOPIC_DELTA1.apply());
        TOPIC_DELTA2.replay(new PartitionChangeRecord().
                setPartitionId(1).
                setTopicId(FOO_ID).
                setLeader(1));
        TOPIC_DELTA2.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 5).message());
    }

    @Test
    public void testHandleNewTopic() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        changes.handleTopicChange(null, TOPIC_DELTA1);
        assertEquals(1, changes.globalTopicsChange());
        assertEquals(5, changes.globalPartitionsChange());
        assertEquals(0, changes.offlinePartitionsChange());
        assertEquals(2, changes.partitionsWithoutPreferredLeaderChange());
    }

    @Test
    public void testTopicChange() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        changes.handleTopicChange(TOPIC_DELTA2.image(), TOPIC_DELTA2);
        assertEquals(0, changes.globalTopicsChange());
        assertEquals(1, changes.globalPartitionsChange());
        assertEquals(0, changes.offlinePartitionsChange());
        assertEquals(1, changes.partitionsWithoutPreferredLeaderChange());
    }
}

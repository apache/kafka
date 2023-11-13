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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.MetadataVersion;
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
        return new BrokerRegistration.Builder().
            setId(brokerId).
            setEpoch(100L).
            setIncarnationId(Uuid.fromString("Pxi6QwS2RFuN8VSKjqJZyQ")).
            setFenced(fenced).
            setInControlledShutdown(false).build();
    }

    private static BrokerRegistration zkBrokerRegistration(
        int brokerId
    ) {
        return new BrokerRegistration.Builder().
            setId(brokerId).
            setEpoch(100L).
            setIncarnationId(Uuid.fromString("Pxi6QwS2RFuN8VSKjqJZyQ")).
            setFenced(false).
            setInControlledShutdown(false).
            setIsMigratingZkBroker(true).build();
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
    public void testHandleZkBroker() {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        changes.handleBrokerChange(null, zkBrokerRegistration(1));
        assertEquals(1, changes.migratingZkBrokersChange());
        changes.handleBrokerChange(null, zkBrokerRegistration(2));
        changes.handleBrokerChange(null, zkBrokerRegistration(3));
        assertEquals(3, changes.migratingZkBrokersChange());

        changes.handleBrokerChange(zkBrokerRegistration(3), brokerRegistration(3, true));
        changes.handleBrokerChange(brokerRegistration(3, true), brokerRegistration(3, false));
        assertEquals(2, changes.migratingZkBrokersChange());
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
        ImageWriterOptions options = new ImageWriterOptions.Builder().
                setMetadataVersion(MetadataVersion.IBP_3_7_IV0).build(); // highest MV for PartitionRecord v0
        TOPIC_DELTA1 = new TopicDelta(new TopicImage("foo", FOO_ID, Collections.emptyMap()));
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 0, options).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 1, options).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 2, options).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NON_PREFERRED_LEADER).
                toRecord(FOO_ID, 3, options).message());
        TOPIC_DELTA1.replay((PartitionRecord) fakePartitionRegistration(NON_PREFERRED_LEADER).
                toRecord(FOO_ID, 4, options).message());

        TOPIC_DELTA2 = new TopicDelta(TOPIC_DELTA1.apply());
        TOPIC_DELTA2.replay(new PartitionChangeRecord().
                setPartitionId(1).
                setTopicId(FOO_ID).
                setLeader(1));
        TOPIC_DELTA2.replay((PartitionRecord) fakePartitionRegistration(NORMAL).
                toRecord(FOO_ID, 5, options).message());
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

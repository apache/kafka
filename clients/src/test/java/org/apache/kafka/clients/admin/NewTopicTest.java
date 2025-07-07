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

import org.apache.kafka.common.message.CreateTopicsRequestData;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.requests.CreateTopicsRequest.NO_NUM_PARTITIONS;
import static org.apache.kafka.common.requests.CreateTopicsRequest.NO_REPLICATION_FACTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class NewTopicTest {

    public static final String TEST_TOPIC = "testtopic";
    public static final int NUM_PARTITIONS = 3;
    public static final short REPLICATION_FACTOR = 1;
    public static final String CLEANUP_POLICY_CONFIG_KEY = "cleanup.policy";
    public static final String CLEANUP_POLICY_CONFIG_VALUE = "compact";
    public static final List<Integer> BROKER_IDS = Arrays.asList(1, 2);

    @Test
    public void testConstructorWithPartitionsAndReplicationFactor() {
        NewTopic topic = new NewTopic(TEST_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
        assertEquals(TEST_TOPIC, topic.name());
        assertEquals(NUM_PARTITIONS, topic.numPartitions());
        assertEquals(REPLICATION_FACTOR, topic.replicationFactor());
        assertNull(topic.replicasAssignments());
    }

    @Test
    public void testConstructorWithOptionalValues() {
        Optional<Integer> numPartitions = Optional.empty();
        Optional<Short> replicationFactor = Optional.empty();
        NewTopic topic = new NewTopic(TEST_TOPIC, numPartitions, replicationFactor);
        assertEquals(TEST_TOPIC, topic.name());
        assertEquals(NO_NUM_PARTITIONS, topic.numPartitions());
        assertEquals(NO_REPLICATION_FACTOR, topic.replicationFactor());
        assertNull(topic.replicasAssignments());
    }

    @Test
    public void testConstructorWithReplicasAssignments() {
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, BROKER_IDS);
        NewTopic newTopic = new NewTopic(TEST_TOPIC, replicasAssignments);
        assertEquals(TEST_TOPIC, newTopic.name());
        assertEquals(NO_NUM_PARTITIONS, newTopic.numPartitions());
        assertEquals(NO_REPLICATION_FACTOR, newTopic.replicationFactor());
        assertEquals(replicasAssignments, newTopic.replicasAssignments());
    }

    @Test
    public void testConfigsNotNull() {
        NewTopic newTopic = new NewTopic(TEST_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
        Map<String, String> configs = new HashMap<>();
        configs.put(CLEANUP_POLICY_CONFIG_KEY, CLEANUP_POLICY_CONFIG_VALUE);
        newTopic.configs(configs);
        assertEquals(configs, newTopic.configs());
    }

    @Test
    public void testConfigsNull() {
        NewTopic newTopic = new NewTopic(TEST_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
        assertNull(newTopic.configs());
    }

    @Test
    public void testUnmodifiableReplicasAssignments() {
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, BROKER_IDS);
        NewTopic newTopic = new NewTopic(TEST_TOPIC, replicasAssignments);
        Map<Integer, List<Integer>> returnedAssignments = newTopic.replicasAssignments();

        assertThrows(UnsupportedOperationException.class, () ->
                returnedAssignments.put(1, Arrays.asList(3, 4))
        );
    }

    @Test
    public void testConvertToCreatableTopicWithPartitionsAndReplicationFactor() {
        NewTopic newTopic = new NewTopic(TEST_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
        CreateTopicsRequestData.CreatableTopic creatableTopic = newTopic.convertToCreatableTopic();

        assertEquals(TEST_TOPIC, creatableTopic.name());
        assertTrue(creatableTopic.numPartitions() > 0);
        assertEquals(NUM_PARTITIONS, creatableTopic.numPartitions());
        assertTrue(creatableTopic.replicationFactor() > 0);
        assertEquals(REPLICATION_FACTOR, creatableTopic.replicationFactor());
    }

    @Test
    public void testConvertToCreatableTopicWithReplicasAssignments() {
        int partitionIndex = 0;
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(partitionIndex, BROKER_IDS);
        NewTopic topic = new NewTopic(TEST_TOPIC, replicasAssignments);
        Map<String, String> configs = new HashMap<>();
        configs.put(CLEANUP_POLICY_CONFIG_KEY, CLEANUP_POLICY_CONFIG_VALUE);
        topic.configs(configs);

        CreateTopicsRequestData.CreatableTopic creatableTopic = topic.convertToCreatableTopic();

        assertEquals(TEST_TOPIC, creatableTopic.name());
        assertEquals(NO_NUM_PARTITIONS, creatableTopic.numPartitions());
        assertEquals(NO_REPLICATION_FACTOR, creatableTopic.replicationFactor());
        assertNotNull(creatableTopic.assignments());
        assertEquals(1, creatableTopic.assignments().size());

        CreateTopicsRequestData.CreatableReplicaAssignmentCollection assignmentsCollection = creatableTopic.assignments();

        CreateTopicsRequestData.CreatableReplicaAssignment assignment = assignmentsCollection.find(partitionIndex);
        assertEquals(partitionIndex, assignment.partitionIndex());
        assertEquals(BROKER_IDS, assignment.brokerIds());

        assertNotNull(creatableTopic.configs());
        assertEquals(1, creatableTopic.configs().size());

        CreateTopicsRequestData.CreatableTopicConfig config = creatableTopic.configs().find(CLEANUP_POLICY_CONFIG_KEY);
        assertEquals(CLEANUP_POLICY_CONFIG_KEY, config.name());
        assertEquals(CLEANUP_POLICY_CONFIG_VALUE, config.value());
    }

    @Test
    public void testToString() {
        NewTopic topic1 = new NewTopic(TEST_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
        String expected1 = "(name=" + TEST_TOPIC + ", numPartitions=" + NUM_PARTITIONS
                + ", replicationFactor=" + REPLICATION_FACTOR + ", replicasAssignments=null, configs=null)";
        assertEquals(expected1, topic1.toString());

        Map<String, String> configs = new HashMap<>();
        configs.put(CLEANUP_POLICY_CONFIG_KEY, CLEANUP_POLICY_CONFIG_VALUE);
        topic1.configs(configs);
        String expected2 = "(name=" + TEST_TOPIC + ", numPartitions=" + NUM_PARTITIONS
                + ", replicationFactor=" + REPLICATION_FACTOR + ", replicasAssignments=null, configs="
                + "{" + CLEANUP_POLICY_CONFIG_KEY + "=" + CLEANUP_POLICY_CONFIG_VALUE + "})";
        assertEquals(expected2, topic1.toString());

        int partitionIndex = 0;
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(partitionIndex, BROKER_IDS);
        NewTopic topic2 = new NewTopic(TEST_TOPIC, replicasAssignments);
        String expected3 = "(name=" + TEST_TOPIC + ", numPartitions=default"
                + ", replicationFactor=default, replicasAssignments="
                + "{" + partitionIndex + "=" + BROKER_IDS + "}" + ", configs=null)";
        assertEquals(expected3, topic2.toString());
    }

    @Test
    public void testEqualsAndHashCode() {
        NewTopic topic1 = new NewTopic(TEST_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
        NewTopic topic2 = new NewTopic(TEST_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
        NewTopic topic3 = new NewTopic("another-topic", NUM_PARTITIONS, REPLICATION_FACTOR);

        assertEquals(topic1, topic2);
        assertNotEquals(topic1, topic3);
        assertEquals(topic1.hashCode(), topic2.hashCode());
        assertNotEquals(topic1.hashCode(), topic3.hashCode());
    }
}

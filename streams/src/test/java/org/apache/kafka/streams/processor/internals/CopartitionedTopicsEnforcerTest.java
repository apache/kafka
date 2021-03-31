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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.internals.assignment.CopartitionedTopicsEnforcer;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class CopartitionedTopicsEnforcerTest {

    private final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer("thread ");
    private final Map<TopicPartition, PartitionInfo> partitions = new HashMap<>();
    private final Cluster cluster = Cluster.empty();

    @Before
    public void before() {
        partitions.put(
            new TopicPartition("first", 0),
            new PartitionInfo("first", 0, null, null, null));
        partitions.put(
            new TopicPartition("first", 1),
            new PartitionInfo("first", 1, null, null, null));
        partitions.put(
            new TopicPartition("second", 0),
            new PartitionInfo("second", 0, null, null, null));
        partitions.put(
            new TopicPartition("second", 1),
            new PartitionInfo("second", 1, null, null, null));
    }

    @Test
    public void shouldThrowTopologyBuilderExceptionIfNoPartitionsFoundForCoPartitionedTopic() {
        assertThrows(IllegalStateException.class, () -> validator.enforce(Collections.singleton("topic"),
            Collections.emptyMap(), cluster));
    }

    @Test
    public void shouldThrowTopologyBuilderExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch() {
        partitions.remove(new TopicPartition("second", 0));
        assertThrows(TopologyException.class, () -> validator.enforce(Utils.mkSet("first", "second"),
                          Collections.emptyMap(),
                          cluster.withPartitions(partitions)));
    }


    @Test
    public void shouldEnforceCopartitioningOnRepartitionTopics() {
        final InternalTopicConfig config = createTopicConfig("repartitioned", 10);

        validator.enforce(Utils.mkSet("first", "second", config.name()),
                          Collections.singletonMap(config.name(), config),
                          cluster.withPartitions(partitions));

        assertThat(config.numberOfPartitions(), equalTo(Optional.of(2)));
    }


    @Test
    public void shouldSetNumPartitionsToMaximumPartitionsWhenAllTopicsAreRepartitionTopics() {
        final InternalTopicConfig one = createTopicConfig("one", 1);
        final InternalTopicConfig two = createTopicConfig("two", 15);
        final InternalTopicConfig three = createTopicConfig("three", 5);
        final Map<String, InternalTopicConfig> repartitionTopicConfig = new HashMap<>();

        repartitionTopicConfig.put(one.name(), one);
        repartitionTopicConfig.put(two.name(), two);
        repartitionTopicConfig.put(three.name(), three);

        validator.enforce(Utils.mkSet(one.name(),
                                      two.name(),
                                      three.name()),
                          repartitionTopicConfig,
                          cluster
        );

        assertThat(one.numberOfPartitions(), equalTo(Optional.of(15)));
        assertThat(two.numberOfPartitions(), equalTo(Optional.of(15)));
        assertThat(three.numberOfPartitions(), equalTo(Optional.of(15)));
    }

    @Test
    public void shouldThrowAnExceptionIfRepartitionTopicConfigsWithEnforcedNumOfPartitionsHaveDifferentNumOfPartitiones() {
        final InternalTopicConfig topic1 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-1", 10);
        final InternalTopicConfig topic2 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-2", 5);

        final TopologyException ex = assertThrows(
            TopologyException.class,
            () -> validator.enforce(Utils.mkSet(topic1.name(), topic2.name()),
                                    Utils.mkMap(Utils.mkEntry(topic1.name(), topic1),
                                                Utils.mkEntry(topic2.name(), topic2)),
                                    cluster.withPartitions(partitions))
        );

        final TreeMap<String, Integer> sorted = new TreeMap<>(
            Utils.mkMap(Utils.mkEntry(topic1.name(), topic1.numberOfPartitions().get()),
                        Utils.mkEntry(topic2.name(), topic2.numberOfPartitions().get()))
        );

        assertEquals(String.format("Invalid topology: thread " +
                                   "Following topics do not have the same number of partitions: " +
                                   "[%s]", sorted), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenRepartitionTopicConfigsWithEnforcedNumOfPartitionsAreValid() {
        final InternalTopicConfig topic1 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-1", 10);
        final InternalTopicConfig topic2 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-2", 10);

        validator.enforce(Utils.mkSet(topic1.name(), topic2.name()),
                          Utils.mkMap(Utils.mkEntry(topic1.name(), topic1),
                                      Utils.mkEntry(topic2.name(), topic2)),
                          cluster.withPartitions(partitions));

        assertThat(topic1.numberOfPartitions(), equalTo(Optional.of(10)));
        assertThat(topic2.numberOfPartitions(), equalTo(Optional.of(10)));
    }

    @Test
    public void shouldThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsDoNotMatch() {
        final InternalTopicConfig topic1 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-1", 10);

        final TopologyException ex = assertThrows(
            TopologyException.class,
            () -> validator.enforce(Utils.mkSet(topic1.name(), "second"),
                                    Utils.mkMap(Utils.mkEntry(topic1.name(), topic1)),
                                    cluster.withPartitions(partitions))
        );

        assertEquals(String.format("Invalid topology: thread Number of partitions [%s] " +
                                   "of repartition topic [%s] " +
                                   "doesn't match number of partitions [%s] of the source topic.",
                                   topic1.numberOfPartitions().get(), topic1.name(), 2), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsMatch() {
        final InternalTopicConfig topic1 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-1", 2);

        validator.enforce(Utils.mkSet(topic1.name(), "second"),
                          Utils.mkMap(Utils.mkEntry(topic1.name(), topic1)),
                          cluster.withPartitions(partitions));

        assertThat(topic1.numberOfPartitions(), equalTo(Optional.of(2)));
    }

    @Test
    public void shouldDeductNumberOfPartitionsFromRepartitionTopicWithEnforcedNumberOfPartitions() {
        final InternalTopicConfig topic1 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-1", 2);
        final InternalTopicConfig topic2 = createTopicConfig("repartitioned-2", 5);
        final InternalTopicConfig topic3 = createRepartitionTopicConfigWithEnforcedNumberOfPartitions("repartitioned-3", 2);

        validator.enforce(Utils.mkSet(topic1.name(), topic2.name()),
                          Utils.mkMap(Utils.mkEntry(topic1.name(), topic1),
                                      Utils.mkEntry(topic2.name(), topic2),
                                      Utils.mkEntry(topic3.name(), topic3)),
                          cluster.withPartitions(partitions));

        assertEquals(topic1.numberOfPartitions(), topic2.numberOfPartitions());
        assertEquals(topic2.numberOfPartitions(), topic3.numberOfPartitions());
    }

    private InternalTopicConfig createTopicConfig(final String repartitionTopic,
                                                  final int partitions) {
        final InternalTopicConfig repartitionTopicConfig =
            new RepartitionTopicConfig(repartitionTopic, Collections.emptyMap());

        repartitionTopicConfig.setNumberOfPartitions(partitions);
        return repartitionTopicConfig;
    }

    private InternalTopicConfig createRepartitionTopicConfigWithEnforcedNumberOfPartitions(final String repartitionTopic,
                                                                                           final int partitions) {
        return new RepartitionTopicConfig(repartitionTopic,
                                          Collections.emptyMap(),
                                          partitions,
                                          true);
    }

}
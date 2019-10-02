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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CopartitionedTopicsEnforcerTest {

    private final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer("thread");
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

    @Test(expected = IllegalStateException.class)
    public void shouldThrowTopologyBuilderExceptionIfNoPartitionsFoundForCoPartitionedTopic() {
        validator.enforce(Collections.singleton("topic"),
                          Collections.emptyMap(),
                          cluster);
    }

    @Test(expected = TopologyException.class)
    public void shouldThrowTopologyBuilderExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch() {
        partitions.remove(new TopicPartition("second", 0));
        validator.enforce(Utils.mkSet("first", "second"),
                          Collections.emptyMap(),
                          cluster.withPartitions(partitions));
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

    private InternalTopicConfig createTopicConfig(final String repartitionTopic,
                                                                               final int partitions) {
        final InternalTopicConfig repartitionTopicConfig =
            new RepartitionTopicConfig(repartitionTopic, Collections.emptyMap());

        repartitionTopicConfig.setNumberOfPartitions(partitions);
        return repartitionTopicConfig;
    }

}
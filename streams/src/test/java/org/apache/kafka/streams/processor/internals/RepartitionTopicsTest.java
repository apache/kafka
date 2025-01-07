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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.assignment.CopartitionedTopicsEnforcer;
import org.apache.kafka.streams.processor.internals.testutil.DummyStreamsConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_1;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class RepartitionTopicsTest {

    private static final String SOURCE_TOPIC_NAME1 = "source1";
    private static final String SOURCE_TOPIC_NAME2 = "source2";
    private static final String SOURCE_TOPIC_NAME3 = "source3";
    private static final String SINK_TOPIC_NAME1 = "sink1";
    private static final String SINK_TOPIC_NAME2 = "sink2";
    private static final String REPARTITION_TOPIC_NAME1 = "repartition1";
    private static final String REPARTITION_TOPIC_NAME2 = "repartition2";
    private static final String REPARTITION_TOPIC_NAME3 = "repartition3";
    private static final String REPARTITION_TOPIC_NAME4 = "repartition4";
    private static final String REPARTITION_WITHOUT_PARTITION_COUNT = "repartitionWithoutPartitionCount";
    private static final String SOME_OTHER_TOPIC = "someOtherTopic";
    private static final Map<String, String> TOPIC_CONFIG1 = Collections.singletonMap("config1", "val1");
    private static final Map<String, String> TOPIC_CONFIG2 = Collections.singletonMap("config2", "val2");
    private static final Map<String, String> TOPIC_CONFIG5 = Collections.singletonMap("config5", "val5");
    private static final RepartitionTopicConfig REPARTITION_TOPIC_CONFIG1 =
        new RepartitionTopicConfig(REPARTITION_TOPIC_NAME1, TOPIC_CONFIG1, 4, true);
    private static final RepartitionTopicConfig REPARTITION_TOPIC_CONFIG2 =
        new RepartitionTopicConfig(REPARTITION_TOPIC_NAME2, TOPIC_CONFIG2, 2, true);
    private static final TopicsInfo TOPICS_INFO1 = new TopicsInfo(
        Set.of(REPARTITION_TOPIC_NAME1),
        Set.of(SOURCE_TOPIC_NAME1, SOURCE_TOPIC_NAME2),
        mkMap(
            mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
            mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2)
        ),
        Collections.emptyMap()
    );
    private static final TopicsInfo TOPICS_INFO2 = new TopicsInfo(
        Set.of(SINK_TOPIC_NAME1),
        Set.of(REPARTITION_TOPIC_NAME1),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1)),
        Collections.emptyMap()
    );
    final  StreamsConfig config = new DummyStreamsConfig();

    @Mock
    InternalTopologyBuilder internalTopologyBuilder;
    @Mock
    InternalTopicManager internalTopicManager;
    @Mock
    CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    @Mock
    Cluster clusterMetadata;

    @BeforeEach
    public void setup() {
        when(internalTopologyBuilder.hasNamedTopology()).thenReturn(false);
        when(internalTopologyBuilder.topologyName()).thenReturn(null);
    }

    @Test
    public void shouldSetupRepartitionTopics() {
        when(internalTopologyBuilder.subtopologyToTopicsInfo())
            .thenReturn(mkMap(mkEntry(SUBTOPOLOGY_0, TOPICS_INFO1), mkEntry(SUBTOPOLOGY_1, TOPICS_INFO2)));
        final Set<String> coPartitionGroup1 = Set.of(SOURCE_TOPIC_NAME1, SOURCE_TOPIC_NAME2);
        final Set<String> coPartitionGroup2 = Set.of(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_NAME2);
        final List<Set<String>> coPartitionGroups = Arrays.asList(coPartitionGroup1, coPartitionGroup2);
        when(internalTopologyBuilder.copartitionGroups()).thenReturn(coPartitionGroups);
        when(internalTopicManager.makeReady(
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2)
            ))
        ).thenReturn(Collections.emptySet());
        setupCluster(false);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            new TopologyMetadata(internalTopologyBuilder, config),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            clusterMetadata,
            "[test] "
        );

        repartitionTopics.setup();

        final Map<TopicPartition, PartitionInfo> topicPartitionsInfo = repartitionTopics.topicPartitionsInfo();
        assertThat(topicPartitionsInfo.size(), is(6));
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 1);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 2);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 3);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME2, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME2, 1);

        assertThat(repartitionTopics.topologiesWithMissingInputTopics().isEmpty(), is(true));
        assertThat(repartitionTopics.missingSourceTopicExceptions().isEmpty(), is(true));

        verify(copartitionedTopicsEnforcer).enforce(eq(coPartitionGroup1), any(), eq(clusterMetadata));
        verify(copartitionedTopicsEnforcer).enforce(eq(coPartitionGroup2), any(), eq(clusterMetadata));
    }

    @Test
    public void shouldReturnMissingSourceTopics() {
        final Set<String> missingSourceTopics = Set.of(SOURCE_TOPIC_NAME1);
        when(internalTopologyBuilder.subtopologyToTopicsInfo())
            .thenReturn(mkMap(mkEntry(SUBTOPOLOGY_0, TOPICS_INFO1), mkEntry(SUBTOPOLOGY_1, TOPICS_INFO2)));
        setupClusterWithMissingTopics(missingSourceTopics, false);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            new TopologyMetadata(internalTopologyBuilder, config),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            clusterMetadata,
            "[test] "
        );
        repartitionTopics.setup();

        assertThat(
            repartitionTopics.topologiesWithMissingInputTopics(),
            equalTo(Collections.singleton(UNNAMED_TOPOLOGY))
        );
        final StreamsException exception = repartitionTopics.missingSourceTopicExceptions().poll();
        assertThat(exception, notNullValue());
        assertThat(exception.taskId().isPresent(), is(true));
        assertThat(exception.taskId().get(), equalTo(new TaskId(0, 0)));
    }

    @Test
    public void shouldThrowTaskAssignmentExceptionIfPartitionCountCannotBeComputedForAllRepartitionTopics() {
        final RepartitionTopicConfig repartitionTopicConfigWithoutPartitionCount =
            new RepartitionTopicConfig(REPARTITION_WITHOUT_PARTITION_COUNT, TOPIC_CONFIG5);
        when(internalTopologyBuilder.subtopologyToTopicsInfo())
            .thenReturn(mkMap(
                mkEntry(SUBTOPOLOGY_0, TOPICS_INFO1),
                mkEntry(SUBTOPOLOGY_1, setupTopicInfoWithRepartitionTopicWithoutPartitionCount(repartitionTopicConfigWithoutPartitionCount))
            ));
        setupCluster(false);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            new TopologyMetadata(internalTopologyBuilder, config),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            clusterMetadata,
            "[test] "
        );

        final TaskAssignmentException exception = assertThrows(TaskAssignmentException.class, repartitionTopics::setup);
        assertThat(exception.getMessage(), is("Failed to compute number of partitions for all repartition topics, make sure all user input topics are created and all Pattern subscriptions match at least one topic in the cluster"));
        assertThat(repartitionTopics.topologiesWithMissingInputTopics().isEmpty(), is(true));
        assertThat(repartitionTopics.missingSourceTopicExceptions().isEmpty(), is(true));
    }

    @Test
    public void shouldThrowTaskAssignmentExceptionIfSourceTopicHasNoPartitionCount() {
        final RepartitionTopicConfig repartitionTopicConfigWithoutPartitionCount =
            new RepartitionTopicConfig(REPARTITION_WITHOUT_PARTITION_COUNT, TOPIC_CONFIG5);
        final TopicsInfo topicsInfo = new TopicsInfo(
            Set.of(REPARTITION_WITHOUT_PARTITION_COUNT),
            Set.of(SOURCE_TOPIC_NAME1),
            mkMap(
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, repartitionTopicConfigWithoutPartitionCount)
            ),
            Collections.emptyMap()
        );
        when(internalTopologyBuilder.subtopologyToTopicsInfo())
            .thenReturn(mkMap(
                mkEntry(SUBTOPOLOGY_0, topicsInfo),
                mkEntry(SUBTOPOLOGY_1, setupTopicInfoWithRepartitionTopicWithoutPartitionCount(repartitionTopicConfigWithoutPartitionCount))
            ));
        setupClusterWithMissingPartitionCounts(Set.of(SOURCE_TOPIC_NAME1), true);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            new TopologyMetadata(internalTopologyBuilder, config),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            clusterMetadata,
            "[test] "
        );

        final TaskAssignmentException exception = assertThrows(TaskAssignmentException.class, repartitionTopics::setup);
        assertThat(
            exception.getMessage(),
            is("No partition count found for source topic " + SOURCE_TOPIC_NAME1 + ", but it should have been.")
        );
        assertThat(repartitionTopics.topologiesWithMissingInputTopics().isEmpty(), is(true));
        assertThat(repartitionTopics.missingSourceTopicExceptions().isEmpty(), is(true));
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamExternalSourceTopic() {
        final RepartitionTopicConfig repartitionTopicConfigWithoutPartitionCount =
            new RepartitionTopicConfig(REPARTITION_WITHOUT_PARTITION_COUNT, TOPIC_CONFIG5);
        final TopicsInfo topicsInfo = new TopicsInfo(
            Set.of(REPARTITION_TOPIC_NAME1, REPARTITION_WITHOUT_PARTITION_COUNT),
            Set.of(SOURCE_TOPIC_NAME1, REPARTITION_TOPIC_NAME2),
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, repartitionTopicConfigWithoutPartitionCount)
            ),
            Collections.emptyMap()
        );
        when(internalTopologyBuilder.subtopologyToTopicsInfo())
            .thenReturn(mkMap(
                mkEntry(SUBTOPOLOGY_0, topicsInfo),
                mkEntry(SUBTOPOLOGY_1, setupTopicInfoWithRepartitionTopicWithoutPartitionCount(repartitionTopicConfigWithoutPartitionCount))
            ));
        when(internalTopologyBuilder.copartitionGroups()).thenReturn(Collections.emptyList());
        when(internalTopicManager.makeReady(
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, repartitionTopicConfigWithoutPartitionCount)
            ))
        ).thenReturn(Collections.emptySet());
        setupCluster(true);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            new TopologyMetadata(internalTopologyBuilder, config),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            clusterMetadata,
            "[test] "
        );

        repartitionTopics.setup();

        final Map<TopicPartition, PartitionInfo> topicPartitionsInfo = repartitionTopics.topicPartitionsInfo();
        assertThat(topicPartitionsInfo.size(), is(9));
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 1);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 2);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 3);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME2, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME2, 1);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_WITHOUT_PARTITION_COUNT, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_WITHOUT_PARTITION_COUNT, 1);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_WITHOUT_PARTITION_COUNT, 2);

        assertThat(repartitionTopics.topologiesWithMissingInputTopics().isEmpty(), is(true));
        assertThat(repartitionTopics.missingSourceTopicExceptions().isEmpty(), is(true));
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamInternalRepartitionSourceTopic() {
        final RepartitionTopicConfig repartitionTopicConfigWithoutPartitionCount =
            new RepartitionTopicConfig(REPARTITION_WITHOUT_PARTITION_COUNT, TOPIC_CONFIG5);
        final TopicsInfo topicsInfo = new TopicsInfo(
            Set.of(REPARTITION_TOPIC_NAME2, REPARTITION_WITHOUT_PARTITION_COUNT),
            Set.of(SOURCE_TOPIC_NAME1, REPARTITION_TOPIC_NAME1),
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, repartitionTopicConfigWithoutPartitionCount)
            ),
            Collections.emptyMap()
        );
        when(internalTopologyBuilder.subtopologyToTopicsInfo())
            .thenReturn(mkMap(
                mkEntry(SUBTOPOLOGY_0, topicsInfo),
                mkEntry(SUBTOPOLOGY_1, setupTopicInfoWithRepartitionTopicWithoutPartitionCount(repartitionTopicConfigWithoutPartitionCount))
            ));
        when(internalTopologyBuilder.copartitionGroups()).thenReturn(Collections.emptyList());
        when(internalTopicManager.makeReady(
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, repartitionTopicConfigWithoutPartitionCount)
            ))
        ).thenReturn(Collections.emptySet());
        setupCluster(true);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            new TopologyMetadata(internalTopologyBuilder, config),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            clusterMetadata,
            "[test] "
        );

        repartitionTopics.setup();

        final Map<TopicPartition, PartitionInfo> topicPartitionsInfo = repartitionTopics.topicPartitionsInfo();
        assertThat(topicPartitionsInfo.size(), is(10));
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 1);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 2);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME1, 3);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME2, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_TOPIC_NAME2, 1);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_WITHOUT_PARTITION_COUNT, 0);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_WITHOUT_PARTITION_COUNT, 1);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_WITHOUT_PARTITION_COUNT, 2);
        verifyRepartitionTopicPartitionInfo(topicPartitionsInfo, REPARTITION_WITHOUT_PARTITION_COUNT, 3);

        assertThat(repartitionTopics.topologiesWithMissingInputTopics().isEmpty(), is(true));
        assertThat(repartitionTopics.missingSourceTopicExceptions().isEmpty(), is(true));
    }

    @Test
    public void shouldNotSetupRepartitionTopicsWhenTopologyDoesNotContainAnyRepartitionTopics() {
        final TopicsInfo topicsInfo = new TopicsInfo(
            Set.of(SINK_TOPIC_NAME1),
            Set.of(SOURCE_TOPIC_NAME1),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        when(internalTopologyBuilder.subtopologyToTopicsInfo())
            .thenReturn(mkMap(mkEntry(SUBTOPOLOGY_0, topicsInfo)));
        setupCluster(false);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            new TopologyMetadata(internalTopologyBuilder, config),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            clusterMetadata,
            "[test] "
        );

        repartitionTopics.setup();

        final Map<TopicPartition, PartitionInfo> topicPartitionsInfo = repartitionTopics.topicPartitionsInfo();
        assertThat(topicPartitionsInfo, is(Collections.emptyMap()));

        assertThat(repartitionTopics.topologiesWithMissingInputTopics().isEmpty(), is(true));
        assertThat(repartitionTopics.missingSourceTopicExceptions().isEmpty(), is(true));
    }

    private void verifyRepartitionTopicPartitionInfo(final Map<TopicPartition, PartitionInfo> topicPartitionsInfo,
                                                     final String topic,
                                                     final int partition) {
        final TopicPartition repartitionTopicPartition = new TopicPartition(topic, partition);
        assertThat(topicPartitionsInfo.containsKey(repartitionTopicPartition), is(true));
        final PartitionInfo repartitionTopicInfo = topicPartitionsInfo.get(repartitionTopicPartition);
        assertThat(repartitionTopicInfo.topic(), is(topic));
        assertThat(repartitionTopicInfo.partition(), is(partition));
        assertThat(repartitionTopicInfo.inSyncReplicas(), is(new Node[0]));
        assertThat(repartitionTopicInfo.leader(), nullValue());
        assertThat(repartitionTopicInfo.offlineReplicas(), is(new Node[0]));
        assertThat(repartitionTopicInfo.replicas(), is(new Node[0]));
    }

    private void setupCluster(final boolean mockPartitionCount) {
        setupClusterWithMissingTopicsAndMissingPartitionCounts(Collections.emptySet(), Collections.emptySet(), mockPartitionCount);
    }

    private void setupClusterWithMissingTopics(final Set<String> missingTopics, final boolean mockPartitionCount) {
        setupClusterWithMissingTopicsAndMissingPartitionCounts(missingTopics, Collections.emptySet(), mockPartitionCount);
    }

    private void setupClusterWithMissingPartitionCounts(final Set<String> topicsWithMissingPartitionCounts, final boolean mockPartitionCount) {
        setupClusterWithMissingTopicsAndMissingPartitionCounts(Collections.emptySet(),
            topicsWithMissingPartitionCounts,
            mockPartitionCount);
    }

    private void setupClusterWithMissingTopicsAndMissingPartitionCounts(final Set<String> missingTopics,
                                                                        final Set<String> topicsWithMissingPartitionCounts,
                                                                        final boolean mockPartitionCount) {
        final Set<String> topics = new HashSet<>(List.of(
            SOURCE_TOPIC_NAME1,
            SOURCE_TOPIC_NAME2,
            SOURCE_TOPIC_NAME3,
            SINK_TOPIC_NAME1,
            SINK_TOPIC_NAME2,
            REPARTITION_TOPIC_NAME1,
            REPARTITION_TOPIC_NAME2,
            REPARTITION_TOPIC_NAME3,
            REPARTITION_TOPIC_NAME4,
            SOME_OTHER_TOPIC
        ));
        topics.removeAll(missingTopics);
        when(clusterMetadata.topics()).thenReturn(topics);
        if (mockPartitionCount) {
            when(clusterMetadata.partitionCountForTopic(SOURCE_TOPIC_NAME1))
                .thenReturn(topicsWithMissingPartitionCounts.contains(SOURCE_TOPIC_NAME1) ? null : 3);
        }
    }

    private TopicsInfo setupTopicInfoWithRepartitionTopicWithoutPartitionCount(final RepartitionTopicConfig repartitionTopicConfigWithoutPartitionCount) {
        return new TopicsInfo(
            Set.of(SINK_TOPIC_NAME2),
            Set.of(REPARTITION_TOPIC_NAME1, REPARTITION_WITHOUT_PARTITION_COUNT),
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, repartitionTopicConfigWithoutPartitionCount)
            ),
            Collections.emptyMap()
        );
    }
}
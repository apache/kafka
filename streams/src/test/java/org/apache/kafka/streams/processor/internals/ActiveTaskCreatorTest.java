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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockClientSupplier;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.util.Collections.emptySet;

@RunWith(EasyMockRunner.class)
public class ActiveTaskCreatorTest {

    @Mock(type = MockType.NICE)
    private InternalTopologyBuilder builder;
    @Mock(type = MockType.NICE)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;

    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "clientId", StreamsConfig.METRICS_LATEST, new MockTime());
    private final Map<String, Object> properties = mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    );

    // error handling
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void shouldCreateTasksCorrectly() {
        final TaskId task00 = new TaskId(0, 0);
        final TaskId task01 = new TaskId(0, 1);

        final ProcessorTopology topology = mock(ProcessorTopology.class);
        final SourceNode sourceNode = mock(SourceNode.class);

        reset(builder, stateDirectory);
        expect(builder.topologyConfigs()).andStubReturn(new TopologyConfig(new StreamsConfig(properties)));
        expect(builder.buildSubtopology(0)).andReturn(topology).anyTimes();
        expect(topology.sinkTopics()).andStubReturn(emptySet());
        expect(stateDirectory.getOrCreateDirectoryForTask(task00)).andReturn(mock(File.class));
        expect(stateDirectory.checkpointFileFor(task00)).andReturn(mock(File.class));
        expect(stateDirectory.getOrCreateDirectoryForTask(task01)).andReturn(mock(File.class));
        expect(stateDirectory.checkpointFileFor(task01)).andReturn(mock(File.class));
        expect(topology.storeToChangelogTopic()).andReturn(Collections.emptyMap()).anyTimes();
        expect(topology.source("topic")).andReturn(sourceNode).anyTimes();
        expect(sourceNode.getTimestampExtractor()).andReturn(mock(TimestampExtractor.class)).anyTimes();
        expect(topology.globalStateStores()).andReturn(Collections.emptyList()).anyTimes();
        expect(topology.terminalNodes()).andStubReturn(Collections.singleton(sourceNode.name()));
        expect(topology.sources()).andStubReturn(Collections.singleton(sourceNode));
        replay(builder, stateDirectory, topology, sourceNode);

        final LogContext lc = new LogContext();
        final String threadId = "clientId-StreamThread-0";
        final Time time = new MockTime();

        final StreamsConfig config = new StreamsConfig(properties);
        final ActiveTaskCreator activeTaskCreator = new ActiveTaskCreator(
            new TopologyMetadata(builder, config),
            config,
            streamsMetrics,
            stateDirectory,
            changeLogReader,
            new ThreadCache(new LogContext(), 0L, streamsMetrics),
            time,
            threadId,
            lc.logger(ActiveTaskCreator.class),
            false
        );

        assertThat(
            activeTaskCreator.createTasks(
                mockClientSupplier.consumer,
                new StreamsProducer(config, threadId, mockClientSupplier, UUID.randomUUID(), lc, time),
                mkMap(
                    mkEntry(task00, Collections.singleton(new TopicPartition("topic", 0))),
                    mkEntry(task01, Collections.singleton(new TopicPartition("topic", 1)))
                )
            ).stream().map(Task::id).collect(Collectors.toSet()),
            equalTo(mkSet(task00, task01))
        );
    }
}

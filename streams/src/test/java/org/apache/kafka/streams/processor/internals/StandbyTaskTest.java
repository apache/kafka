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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.MockRestoreConsumer;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class StandbyTaskTest {

    private final String threadName = "threadName";
    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0);

    private final String storeName1 = "store1";
    private final String storeName2 = "store2";
    private final String applicationId = "test-application";
    private final String storeChangelogTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
    private final String storeChangelogTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);

    private final TopicPartition partition = new TopicPartition(storeChangelogTopicName1, 0);
    private final MockKeyValueStore store1 = (MockKeyValueStore) new MockKeyValueStoreBuilder(storeName1, false).build();
    private final MockKeyValueStore store2 = (MockKeyValueStore) new MockKeyValueStoreBuilder(storeName2, true).build();

    private final ProcessorTopology topology = ProcessorTopologyFactories.withLocalStores(
        asList(store1, store2),
        mkMap(mkEntry(storeName1, storeChangelogTopicName1), mkEntry(storeName2, storeChangelogTopicName2))
    );
    private final StreamsMetricsImpl streamsMetrics =
        new StreamsMetricsImpl(new Metrics(), threadName, StreamsConfig.METRICS_LATEST);

    private File baseDir;
    private StreamsConfig config;
    private StateDirectory stateDirectory;
    private StandbyTask task;

    private StreamsConfig createConfig(final File baseDir) throws IOException {
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath()),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName())
        )));
    }

    private final MockRestoreConsumer<Integer, Integer> restoreStateConsumer = new MockRestoreConsumer<>(
        new IntegerSerializer(),
        new IntegerSerializer()
    );

    @Mock(type = MockType.NICE)
    private ProcessorStateManager stateManager;

    @Before
    public void setup() throws Exception {
        EasyMock.expect(stateManager.taskId()).andStubReturn(taskId);
        EasyMock.expect(stateManager.taskType()).andStubReturn(TaskType.STANDBY);

        restoreStateConsumer.reset();
        restoreStateConsumer.updatePartitions(storeChangelogTopicName1, asList(
            new PartitionInfo(storeChangelogTopicName1, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName1, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName1, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        restoreStateConsumer.updatePartitions(storeChangelogTopicName2, asList(
            new PartitionInfo(storeChangelogTopicName2, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName2, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName2, 2, Node.noNode(), new Node[0], new Node[0])
        ));
        baseDir = TestUtils.tempDirectory();
        config = createConfig(baseDir);
        stateDirectory = new StateDirectory(config, new MockTime(), true);
    }

    @After
    public void cleanup() throws IOException {
        if (task != null) {
            try {
                task.suspend();
            } catch (final IllegalStateException maybeSwallow) {
                if (!maybeSwallow.getMessage().startsWith("Invalid transition from CLOSED to SUSPENDED")) {
                    throw maybeSwallow;
                }
            }
            task.closeDirty();
            task = null;
        }
        Utils.delete(baseDir);
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectory() throws IOException {
        stateDirectory = EasyMock.createNiceMock(StateDirectory.class);
        EasyMock.expect(stateDirectory.lock(taskId)).andReturn(false);
        EasyMock.expect(stateManager.taskType()).andStubReturn(TaskType.STANDBY);

        EasyMock.replay(stateDirectory, stateManager);

        task = createStandbyTask();

        assertThrows(LockException.class, task::initializeIfNeeded);
        task = null;
    }

    @Test
    public void shouldTransitToRunningAfterInitialization() {
        stateManager.registerStateStores(EasyMock.anyObject(), EasyMock.anyObject());
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.replay(stateManager);

        task = createStandbyTask();

        assertEquals(Task.State.CREATED, task.state());

        task.initializeIfNeeded();

        assertEquals(Task.State.RUNNING, task.state());

        // initialize should be idempotent
        task.initializeIfNeeded();

        assertEquals(Task.State.RUNNING, task.state());

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldThrowIfCommittingOnIllegalState() {
        EasyMock.replay(stateManager);
        task = createStandbyTask();

        assertThrows(IllegalStateException.class, task::prepareCommit);
    }

    @Test
    public void shouldFlushAndCheckpointStateManagerOnCommit() {
        stateManager.flush();
        EasyMock.expectLastCall();
        stateManager.checkpoint(EasyMock.eq(Collections.emptyMap()));
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.singletonMap(partition, 50L));
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(partition)).anyTimes();
        EasyMock.replay(stateManager);

        task = createStandbyTask();
        task.initializeIfNeeded();
        task.prepareCommit();
        task.postCommit();

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldReturnStateManagerChangelogOffsets() {
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.singletonMap(partition, 50L));
        EasyMock.replay(stateManager);

        task = createStandbyTask();

        assertEquals(Collections.singletonMap(partition, 50L), task.changelogOffsets());

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldNotCommitAndThrowOnCloseDirty() {
        stateManager.close();
        EasyMock.expectLastCall().andThrow(new ProcessorStateException("KABOOM!")).anyTimes();
        stateManager.flush();
        EasyMock.expectLastCall().andThrow(new AssertionError("Flush should not be called")).anyTimes();
        stateManager.checkpoint(EasyMock.anyObject());
        EasyMock.expectLastCall().andThrow(new AssertionError("Checkpoint should not be called")).anyTimes();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.replay(stateManager);
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();
        task.suspend();
        task.closeDirty();

        assertEquals(Task.State.CLOSED, task.state());

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldNotThrowFromStateManagerCloseInCloseDirty() {
        stateManager.close();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!")).anyTimes();
        EasyMock.replay(stateManager);

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.suspend();
        task.closeDirty();

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldCommitOnCloseClean() {
        stateManager.close();
        EasyMock.expectLastCall();
        stateManager.checkpoint(EasyMock.eq(Collections.emptyMap()));
        EasyMock.expectLastCall();
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.singletonMap(partition, 50L));
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(partition)).anyTimes();
        EasyMock.replay(stateManager);
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();
        task.suspend();
        task.prepareCommit();
        task.postCommit();
        task.closeClean();

        assertEquals(Task.State.CLOSED, task.state());

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldOnlyNeedCommitWhenChangelogOffsetChanged() {
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(partition)).anyTimes();
        EasyMock.expect(stateManager.changelogOffsets())
            .andReturn(Collections.singletonMap(partition, 50L))
            .andReturn(Collections.singletonMap(partition, 50L))
            .andReturn(Collections.singletonMap(partition, 60L));
        stateManager.flush();
        EasyMock.expectLastCall();
        stateManager.checkpoint(EasyMock.eq(Collections.emptyMap()));
        EasyMock.expectLastCall();
        EasyMock.replay(stateManager);

        task = createStandbyTask();
        task.initializeIfNeeded();

        assertTrue(task.commitNeeded());

        task.prepareCommit();
        task.postCommit();

        // do not need to commit if there's no update
        assertFalse(task.commitNeeded());

        // could commit if the offset advanced
        assertTrue(task.commitNeeded());

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldThrowOnCloseCleanError() {
        stateManager.close();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!")).anyTimes();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(partition)).anyTimes();
        EasyMock.replay(stateManager);
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.suspend();
        assertThrows(RuntimeException.class, () -> task.closeClean());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);
        EasyMock.reset(stateManager);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.singleton(partition)).anyTimes();
        EasyMock.replay(stateManager);
    }

    @Test
    public void shouldThrowOnCloseCleanCheckpointError() {
        stateManager.checkpoint(EasyMock.anyObject());
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!")).anyTimes();
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.replay(stateManager);
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.prepareCommit();
        assertThrows(RuntimeException.class, task::postCommit);

        assertEquals(Task.State.RUNNING, task.state());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);
        EasyMock.reset(stateManager);
        EasyMock.expect(stateManager.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.replay(stateManager);
    }

    @Test
    public void shouldCloseStateManagerOnTaskCreated() {
        stateManager.close();
        EasyMock.expectLastCall();

        EasyMock.replay(stateManager);

        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();

        task.closeDirty();

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);

        assertEquals(Task.State.CLOSED, task.state());
    }

    @Test
    public void shouldDeleteStateDirOnTaskCreatedAndEosAlphaUncleanClose() {
        stateManager.close();
        EasyMock.expectLastCall();

        EasyMock.expect(stateManager.baseDir()).andReturn(baseDir);

        EasyMock.replay(stateManager);

        final MetricName metricName = setupCloseTaskMetric();

        config = new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        )));

        task = createStandbyTask();

        task.closeDirty();

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);

        assertEquals(Task.State.CLOSED, task.state());
    }

    @Test
    public void shouldDeleteStateDirOnTaskCreatedAndEosBetaUncleanClose() {
        stateManager.close();
        EasyMock.expectLastCall();

        EasyMock.expect(stateManager.baseDir()).andReturn(baseDir);

        EasyMock.replay(stateManager);

        final MetricName metricName = setupCloseTaskMetric();

        config = new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA)
        )));

        task = createStandbyTask();

        task.closeDirty();

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        EasyMock.verify(stateManager);

        assertEquals(Task.State.CLOSED, task.state());
    }

    @Test
    public void shouldRecycleTask() {
        stateManager.flush();
        EasyMock.expectLastCall();
        stateManager.recycle();
        EasyMock.expectLastCall();
        EasyMock.replay(stateManager);

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.closeAndRecycleState();

        EasyMock.verify(stateManager);
    }

    private StandbyTask createStandbyTask() {

        final ThreadCache cache = new ThreadCache(
            new LogContext(String.format("stream-thread [%s] ", Thread.currentThread().getName())),
            0,
            streamsMetrics
        );

        final InternalProcessorContext context = new ProcessorContextImpl(
            taskId,
            config,
            stateManager,
            streamsMetrics,
            cache
        );

        return new StandbyTask(
            taskId,
            topology,
            stateDirectory,
            stateManager,
            Collections.singleton(partition),
            config,
            context,
            cache,
            streamsMetrics
        );
    }

    private MetricName setupCloseTaskMetric() {
        final MetricName metricName = new MetricName("name", "group", "description", Collections.emptyMap());
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, "task-closed", Sensor.RecordingLevel.INFO);
        sensor.add(metricName, new CumulativeSum());
        return metricName;
    }

    private void verifyCloseTaskMetric(final double expected, final StreamsMetricsImpl streamsMetrics, final MetricName metricName) {
        final KafkaMetric metric = (KafkaMetric) streamsMetrics.metrics().get(metricName);
        final double totalCloses = metric.measurable().measure(metric.config(), System.currentTimeMillis());
        assertThat(totalCloses, equalTo(expected));
    }
}

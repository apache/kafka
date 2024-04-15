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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.MockRestoreConsumer;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.DEBUG;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.processor.internals.Task.State.CREATED;
import static org.apache.kafka.streams.processor.internals.Task.State.RUNNING;
import static org.apache.kafka.streams.processor.internals.Task.State.SUSPENDED;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class StandbyTaskTest {

    private final String threadName = "threadName";
    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0, "My-Topology");

    private final String storeName1 = "store1";
    private final String storeName2 = "store2";
    private final String applicationId = "test-application";
    private final String storeChangelogTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1, taskId.topologyName());
    private final String storeChangelogTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2, taskId.topologyName());

    private final TopicPartition partition = new TopicPartition(storeChangelogTopicName1, 0);
    private final MockKeyValueStore store1 = new MockKeyValueStoreBuilder(storeName1, false).build();
    private final MockKeyValueStore store2 = new MockKeyValueStoreBuilder(storeName2, true).build();

    private final ProcessorTopology topology = ProcessorTopologyFactories.withLocalStores(
        asList(store1, store2),
        mkMap(mkEntry(storeName1, storeChangelogTopicName1), mkEntry(storeName2, storeChangelogTopicName2))
    );

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG), time);
    private final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, threadName, StreamsConfig.METRICS_LATEST, time);

    private File baseDir;
    private StreamsConfig config;
    private StateDirectory stateDirectory;
    private StandbyTask task;

    private StreamsConfig createConfig(final File baseDir) throws IOException {
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, DEBUG.name),
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

    @Mock
    private ProcessorStateManager stateManager;

    @Before
    public void setup() throws Exception {
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.taskType()).thenReturn(TaskType.STANDBY);

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
        stateDirectory = new StateDirectory(config, new MockTime(), true, true);
    }

    @After
    public void cleanup() throws IOException {
        if (task != null) {
            try {
                task.suspend();
            } catch (final IllegalStateException maybeSwallow) {
                if (!maybeSwallow.getMessage().startsWith("Illegal state CLOSED while suspending standby task")) {
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
        stateDirectory = mock(StateDirectory.class);
        when(stateDirectory.lock(taskId)).thenReturn(false);
        when(stateManager.taskType()).thenReturn(TaskType.STANDBY);

        task = createStandbyTask();

        assertThrows(LockException.class, () -> task.initializeIfNeeded());
        task = null;
    }

    @Test
    public void shouldTransitToRunningAfterInitialization() {
        doNothing().when(stateManager).registerStateStores(any(), any());

        task = createStandbyTask();

        assertEquals(CREATED, task.state());

        task.initializeIfNeeded();

        assertEquals(RUNNING, task.state());

        // initialize should be idempotent
        task.initializeIfNeeded();

        assertEquals(RUNNING, task.state());
    }

    @Test
    public void shouldThrowIfCommittingOnIllegalState() {
        task = createStandbyTask();
        task.suspend();
        task.closeClean();

        assertThrows(IllegalStateException.class, task::prepareCommit);
    }

    @Test
    public void shouldAlwaysCheckpointStateIfEnforced() {
        when(stateManager.changelogOffsets()).thenReturn(Collections.emptyMap());

        task = createStandbyTask();

        task.initializeIfNeeded();
        task.maybeCheckpoint(true);

        verify(stateManager).flush();
        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldOnlyCheckpointStateWithBigAdvanceIfNotEnforced() {
        when(stateManager.changelogOffsets())
                .thenReturn(Collections.singletonMap(partition, 50L))
                .thenReturn(Collections.singletonMap(partition, 11000L))
                .thenReturn(Collections.singletonMap(partition, 12000L));

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.maybeCheckpoint(false);  // this should not checkpoint
        assertTrue(task.offsetSnapshotSinceLastFlush.isEmpty());
        task.maybeCheckpoint(false);  // this should checkpoint
        assertEquals(Collections.singletonMap(partition, 11000L), task.offsetSnapshotSinceLastFlush);
        task.maybeCheckpoint(false);  // this should not checkpoint
        assertEquals(Collections.singletonMap(partition, 11000L), task.offsetSnapshotSinceLastFlush);

        verify(stateManager).flush();
        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldFlushAndCheckpointStateManagerOnCommit() {
        when(stateManager.changelogOffsets()).thenReturn(Collections.emptyMap());
        doNothing().when(stateManager).flush();
        when(stateManager.changelogOffsets())
                .thenReturn(Collections.singletonMap(partition, 50L))
                .thenReturn(Collections.singletonMap(partition, 11000L))
                .thenReturn(Collections.singletonMap(partition, 11000L));

        task = createStandbyTask();
        task.initializeIfNeeded();
        task.prepareCommit();
        task.postCommit(false);  // this should not checkpoint

        task.prepareCommit();
        task.postCommit(false);  // this should checkpoint

        task.prepareCommit();
        task.postCommit(false);  // this should not checkpoint

        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldReturnStateManagerChangelogOffsets() {
        when(stateManager.changelogOffsets()).thenReturn(Collections.singletonMap(partition, 50L));

        task = createStandbyTask();

        assertEquals(Collections.singletonMap(partition, 50L), task.changelogOffsets());
    }

    @Test
    public void shouldNotFlushAndThrowOnCloseDirty() {
        doThrow(new ProcessorStateException("KABOOM!")).when(stateManager).close();
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();
        task.suspend();
        task.closeDirty();

        assertEquals(Task.State.CLOSED, task.state());

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        verify(stateManager, never()).flush();
        verify(stateManager, never()).checkpoint();
    }

    @Test
    public void shouldNotThrowFromStateManagerCloseInCloseDirty() {
        doThrow(new RuntimeException("KABOOM!")).when(stateManager).close();

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.suspend();
        task.closeDirty();
    }

    @Test
    public void shouldSuspendAndCommitBeforeCloseClean() {
        doNothing().when(stateManager).close();
        when(stateManager.changelogOffsets())
                .thenReturn(Collections.singletonMap(partition, 60L));
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();
        task.suspend();
        task.prepareCommit();
        task.postCommit(true);
        task.closeClean();

        assertEquals(Task.State.CLOSED, task.state());

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);
        verify(stateManager).checkpoint();
    }

    @Test
    public void shouldRequireSuspendingCreatedTasksBeforeClose() {
        task = createStandbyTask();
        assertThat(task.state(), equalTo(CREATED));
        assertThrows(IllegalStateException.class, () -> task.closeClean());

        task.suspend();
        task.closeClean();
    }

    @Test
    public void shouldOnlyNeedCommitWhenChangelogOffsetChanged() {
        when(stateManager.changelogOffsets())
            .thenReturn(Collections.singletonMap(partition, 50L))
            .thenReturn(Collections.singletonMap(partition, 10100L));
        doNothing().when(stateManager).flush();
        doNothing().when(stateManager).checkpoint();

        task = createStandbyTask();
        task.initializeIfNeeded();

        // no need to commit if we've just initialized and offset not advanced much
        assertFalse(task.commitNeeded());

        // could commit if the offset advanced beyond threshold
        assertTrue(task.commitNeeded());

        task.prepareCommit();
        task.postCommit(true);
    }

    @Test
    public void shouldThrowOnCloseCleanError() {
        doThrow(new RuntimeException("KABOOM!")).when(stateManager).close();
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.suspend();
        assertThrows(RuntimeException.class, () -> task.closeClean());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);
    }

    @Test
    public void shouldThrowOnCloseCleanCheckpointError() {
        when(stateManager.changelogOffsets())
            .thenReturn(Collections.singletonMap(partition, 50L));
        doThrow(new RuntimeException("KABOOM!")).when(stateManager).checkpoint();
        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.initializeIfNeeded();

        task.prepareCommit();
        assertThrows(RuntimeException.class, () -> task.postCommit(true));

        assertEquals(RUNNING, task.state());

        final double expectedCloseTaskMetric = 0.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);
    }

    @Test
    public void shouldUnregisterMetricsInCloseClean() {
        task = createStandbyTask();
        task.initializeIfNeeded();

        task.suspend();
        task.closeClean();
        // Currently, there are no metrics registered for standby tasks.
        // This is a regression test so that, if we add some, we will be sure to deregister them.
        assertThat(getTaskMetrics(), empty());
    }

    @Test
    public void shouldUnregisterMetricsInCloseDirty() {
        task = createStandbyTask();
        task.initializeIfNeeded();

        task.suspend();
        task.closeDirty();

        // Currently, there are no metrics registered for standby tasks.
        // This is a regression test so that, if we add some, we will be sure to deregister them.
        assertThat(getTaskMetrics(), empty());
    }

    @Test
    public void shouldCloseStateManagerOnTaskCreated() {
        doNothing().when(stateManager).close();

        final MetricName metricName = setupCloseTaskMetric();

        task = createStandbyTask();
        task.suspend();

        task.closeDirty();

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        assertEquals(Task.State.CLOSED, task.state());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDeleteStateDirOnTaskCreatedAndEosAlphaUncleanClose() {
        doNothing().when(stateManager).close();

        when(stateManager.baseDir()).thenReturn(baseDir);

        final MetricName metricName = setupCloseTaskMetric();

        config = new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        )));

        task = createStandbyTask();
        task.suspend();

        task.closeDirty();

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        assertEquals(Task.State.CLOSED, task.state());
    }

    @Test
    public void shouldDeleteStateDirOnTaskCreatedAndEosV2UncleanClose() {
        doNothing().when(stateManager).close();

        when(stateManager.baseDir()).thenReturn(baseDir);

        final MetricName metricName = setupCloseTaskMetric();

        config = new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
        )));

        task = createStandbyTask();

        task.suspend();
        task.closeDirty();

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);

        assertEquals(Task.State.CLOSED, task.state());
    }

    @Test
    public void shouldPrepareRecycleSuspendedTask() {
        task = createStandbyTask();
        assertThrows(IllegalStateException.class, () -> task.prepareRecycle()); // CREATED

        task.initializeIfNeeded();
        assertThrows(IllegalStateException.class, () -> task.prepareRecycle()); // RUNNING

        task.suspend();
        task.prepareRecycle(); // SUSPENDED
        assertThat(task.state(), is(Task.State.CLOSED));

        // Currently, there are no metrics registered for standby tasks.
        // This is a regression test so that, if we add some, we will be sure to deregister them.
        assertThat(getTaskMetrics(), empty());

        verify(stateManager).recycle();
    }

    @Test
    public void shouldAlwaysSuspendCreatedTasks() {
        task = createStandbyTask();
        assertThat(task.state(), equalTo(CREATED));
        task.suspend();
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldAlwaysSuspendRunningTasks() {
        task = createStandbyTask();
        task.initializeIfNeeded();
        assertThat(task.state(), equalTo(RUNNING));
        task.suspend();
        assertThat(task.state(), equalTo(SUSPENDED));
    }

    @Test
    public void shouldInitTaskTimeoutAndEventuallyThrow() {
        task = createStandbyTask();

        task.maybeInitTaskTimeoutOrThrow(0L, null);
        task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).toMillis(), null);

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).plus(Duration.ofMillis(1L)).toMillis(), null)
        );

        assertThat(thrown.getCause(), isA(TimeoutException.class));

    }

    @Test
    public void shouldClearTaskTimeout() {
        task = createStandbyTask();

        task.maybeInitTaskTimeoutOrThrow(0L, null);
        task.clearTaskTimeout();
        task.maybeInitTaskTimeoutOrThrow(Duration.ofMinutes(5).plus(Duration.ofMillis(1L)).toMillis(), null);
    }

    @Test
    public void shouldRecordRestoredRecords() {
        task = createStandbyTask();

        final KafkaMetric totalMetric = getMetric("update", "%s-total", task.id().toString());
        final KafkaMetric rateMetric = getMetric("update", "%s-rate", task.id().toString());

        assertThat(totalMetric.metricValue(), equalTo(0.0));
        assertThat(rateMetric.metricValue(), equalTo(0.0));

        task.recordRestoration(time, 25L, false);

        assertThat(totalMetric.metricValue(), equalTo(25.0));
        assertThat(rateMetric.metricValue(), not(0.0));

        task.recordRestoration(time, 50L, false);

        assertThat(totalMetric.metricValue(), equalTo(75.0));
        assertThat(rateMetric.metricValue(), not(0.0));
    }

    private KafkaMetric getMetric(final String operation,
                                  final String nameFormat,
                                  final String taskId) {
        final String descriptionIsNotVerified = "";
        return metrics.metrics().get(metrics.metricName(
            String.format(nameFormat, operation),
            "stream-task-metrics",
            descriptionIsNotVerified,
            mkMap(
                mkEntry("task-id", taskId),
                mkEntry(THREAD_ID_TAG, Thread.currentThread().getName())
            )
        ));
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
            Collections.singleton(partition),
            topology,
            new TopologyConfig(config).getTaskConfig(),
            streamsMetrics,
            stateManager,
            stateDirectory,
            cache,
            context);
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

    private List<MetricName> getTaskMetrics() {
        return streamsMetrics.metrics().keySet().stream().filter(m -> m.tags().containsKey("task-id")).collect(Collectors.toList());
    }
}

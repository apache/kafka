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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateUpdater.ExceptionAndTasks;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statelessTask;
import static org.apache.kafka.test.StreamsTestUtils.TopologyMetadataBuilder.unnamedTopology;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultStateUpdaterTest {

    private final static int COMMIT_INTERVAL = 100;
    private final static long CALL_TIMEOUT = 1000;
    private final static long VERIFICATION_TIMEOUT = 30000;
    private final static TopicPartition TOPIC_PARTITION_A_0 = new TopicPartition("topicA", 0);
    private final static TopicPartition TOPIC_PARTITION_A_1 = new TopicPartition("topicA", 1);
    private final static TopicPartition TOPIC_PARTITION_B_0 = new TopicPartition("topicB", 0);
    private final static TopicPartition TOPIC_PARTITION_B_1 = new TopicPartition("topicB", 1);
    private final static TopicPartition TOPIC_PARTITION_C_0 = new TopicPartition("topicC", 0);
    private final static TopicPartition TOPIC_PARTITION_D_0 = new TopicPartition("topicD", 0);
    private final static TaskId TASK_0_0 = new TaskId(0, 0);
    private final static TaskId TASK_0_1 = new TaskId(0, 1);
    private final static TaskId TASK_0_2 = new TaskId(0, 2);
    private final static TaskId TASK_1_0 = new TaskId(1, 0);
    private final static TaskId TASK_1_1 = new TaskId(1, 1);
    private final static TaskId TASK_A_0_0 = new TaskId(0, 0, "A");
    private final static TaskId TASK_A_0_1 = new TaskId(0, 1, "A");
    private final static TaskId TASK_B_0_0 = new TaskId(0, 0, "B");
    private final static TaskId TASK_B_0_1 = new TaskId(0, 1, "B");

    // need an auto-tick timer to work for draining with timeout
    private final Time time = new MockTime(1L);
    private final Metrics metrics = new Metrics(time);
    private final StreamsConfig config = new StreamsConfig(configProps(COMMIT_INTERVAL));
    private final ChangelogReader changelogReader = mock(ChangelogReader.class);
    private final TopologyMetadata topologyMetadata = unnamedTopology().build();
    private DefaultStateUpdater stateUpdater =
        new DefaultStateUpdater("test-state-updater", metrics, config, null, changelogReader, topologyMetadata, time);

    @AfterEach
    public void tearDown() {
        stateUpdater.shutdown(Duration.ofMinutes(1));
    }

    private Properties configProps(final int commitInterval) {
        return mkObjectProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitInterval),
            mkEntry(producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), commitInterval)
        ));
    }

    @Test
    public void shouldShutdownStateUpdater() {
        stateUpdater.start();

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verify(changelogReader).clear();
    }

    @Test
    public void shouldShutdownStateUpdaterAndRestart() {
        stateUpdater.start();

        stateUpdater.shutdown(Duration.ofMinutes(1));

        stateUpdater.start();

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verify(changelogReader, times(2)).clear();
    }

    @Test
    public void shouldRemoveTasksFromAndClearInputQueueOnShutdown() throws Exception {
        final StreamTask statelessTask = statelessTask(TASK_0_0).inState(State.RESTORING).build();
        final StreamTask statefulTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        stateUpdater.add(statelessTask);
        stateUpdater.add(statefulTask);
        stateUpdater.remove(TASK_1_1);
        stateUpdater.add(standbyTask);
        verifyRemovedTasks();

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verifyRemovedTasks(statelessTask, statefulTask, standbyTask);
    }

    @Test
    public void shouldRemoveUpdatingTasksOnShutdown() throws Exception {
        stateUpdater.shutdown(Duration.ofMillis(Long.MAX_VALUE));
        stateUpdater = new DefaultStateUpdater("test-state-updater", metrics, new StreamsConfig(configProps(Integer.MAX_VALUE)), null, changelogReader, topologyMetadata, time);
        final StreamTask activeTask = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(activeTask);
        stateUpdater.add(standbyTask);
        verifyUpdatingTasks(activeTask, standbyTask);
        verifyRemovedTasks();

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verifyRemovedTasks(activeTask, standbyTask);
        verify(activeTask).maybeCheckpoint(true);
        verify(standbyTask).maybeCheckpoint(true);
    }

    @Test
    public void shouldRemovePausedTasksOnShutdown() throws Exception {
        final StreamTask activeTask = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).inState(State.RUNNING).build();
        stateUpdater.start();
        stateUpdater.add(activeTask);
        stateUpdater.add(standbyTask);
        verifyUpdatingTasks(activeTask, standbyTask);
        when(topologyMetadata.isPaused(null)).thenReturn(true);
        verifyPausedTasks(activeTask, standbyTask);
        verifyRemovedTasks();

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verifyRemovedTasks(activeTask, standbyTask);
    }

    @Test
    public void shouldRemovePausedAndUpdatingTasksOnShutdown() throws Exception {
        final StreamTask activeTask = statefulTask(TASK_A_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(TASK_B_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();

        when(topologyMetadata.isPaused(standbyTask.id().topologyName())).thenReturn(false).thenReturn(true);

        stateUpdater.start();
        stateUpdater.add(activeTask);
        stateUpdater.add(standbyTask);
        verifyPausedTasks(standbyTask);
        verifyUpdatingTasks(activeTask);
        verifyRemovedTasks();

        stateUpdater.shutdown(Duration.ofMinutes(1));

        verifyRemovedTasks(activeTask, standbyTask);
    }

    @Test
    public void shouldThrowIfStatelessTaskNotInStateRestoring() {
        shouldThrowIfActiveTaskNotInStateRestoring(statelessTask(TASK_0_0).build());
    }

    @Test
    public void shouldThrowIfStatefulTaskNotInStateRestoring() {
        shouldThrowIfActiveTaskNotInStateRestoring(statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).build());
    }

    private void shouldThrowIfActiveTaskNotInStateRestoring(final StreamTask task) {
        shouldThrowIfTaskNotInGivenState(task, State.RESTORING);
    }

    @Test
    public void shouldThrowIfStandbyTaskNotInStateRunning() {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_B_0)).build();
        shouldThrowIfTaskNotInGivenState(task, State.RUNNING);
    }

    private void shouldThrowIfTaskNotInGivenState(final Task task, final State correctState) {
        for (final State state : State.values()) {
            if (state != correctState) {
                when(task.state()).thenReturn(state);
                assertThrows(IllegalStateException.class, () -> stateUpdater.add(task));
            }
        }
    }

    @Test
    public void shouldThrowIfAddingActiveTasksWithSameId() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask task2 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldThrowIfAddingTasksWithSameId(task1, task2);
    }

    @Test
    public void shouldThrowIfAddingStandbyTasksWithSameId() throws Exception {
        final StandbyTask task1 = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        final StandbyTask task2 = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        shouldThrowIfAddingTasksWithSameId(task1, task2);
    }

    @Test
    public void shouldThrowIfAddingActiveAndStandbyTaskWithSameId() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        shouldThrowIfAddingTasksWithSameId(task1, task2);
    }

    @Test
    public void shouldThrowIfAddingStandbyAndActiveTaskWithSameId() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        shouldThrowIfAddingTasksWithSameId(task2, task1);
    }

    private void shouldThrowIfAddingTasksWithSameId(final Task task1, final Task task2) throws Exception {
        stateUpdater.start();
        stateUpdater.add(task1);
        stateUpdater.add(task2);

        verifyFailedTasks(IllegalStateException.class, task1);
    }

    @Test
    public void shouldImmediatelyAddSingleStatelessTaskToRestoredTasks() throws Exception {
        final StreamTask task1 = statelessTask(TASK_0_0).inState(State.RESTORING).build();
        shouldImmediatelyAddStatelessTasksToRestoredTasks(task1);
    }

    @Test
    public void shouldImmediatelyAddMultipleStatelessTasksToRestoredTasks() throws Exception {
        final StreamTask task1 = statelessTask(TASK_0_0).inState(State.RESTORING).build();
        final StreamTask task2 = statelessTask(TASK_0_2).inState(State.RESTORING).build();
        final StreamTask task3 = statelessTask(TASK_1_0).inState(State.RESTORING).build();
        shouldImmediatelyAddStatelessTasksToRestoredTasks(task1, task2, task3);
    }

    private void shouldImmediatelyAddStatelessTasksToRestoredTasks(final StreamTask... tasks) throws Exception {
        stateUpdater.start();
        for (final StreamTask task : tasks) {
            stateUpdater.add(task);
        }

        verifyRestoredActiveTasks(tasks);
        verifyNeverCheckpointTasks(tasks);
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();
    }

    @Test
    public void shouldRestoreSingleActiveStatefulTask() throws Exception {
        final StreamTask task =
            statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);
        stateUpdater.start();

        stateUpdater.add(task);

        verifyRestoredActiveTasks(task);
        verifyCheckpointTasks(true, task);
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();
        verify(changelogReader).register(task.changelogPartitions(), task.stateManager());
        verify(changelogReader).unregister(task.changelogPartitions());
        verify(changelogReader).enforceRestoreActive();
        verify(changelogReader, atLeast(3)).restore(anyMap());
        verify(changelogReader, never()).transitToUpdateStandby();
    }

    @Test
    public void shouldRestoreMultipleActiveStatefulTasks() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask task2 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StreamTask task3 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_C_0))
            .thenReturn(mkSet(TOPIC_PARTITION_C_0, TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_C_0, TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);
        stateUpdater.start();

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        verifyRestoredActiveTasks(task3, task1, task2);
        verifyCheckpointTasks(true, task3, task1, task2);
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();
        verify(changelogReader).register(task1.changelogPartitions(), task1.stateManager());
        verify(changelogReader).register(task2.changelogPartitions(), task2.stateManager());
        verify(changelogReader).register(task3.changelogPartitions(), task3.stateManager());
        verify(changelogReader).unregister(task1.changelogPartitions());
        verify(changelogReader).unregister(task2.changelogPartitions());
        verify(changelogReader).unregister(task3.changelogPartitions());
        verify(changelogReader, times(3)).enforceRestoreActive();
        verify(changelogReader, atLeast(4)).restore(anyMap());
        verify(changelogReader, never()).transitToUpdateStandby();
    }

    @Test
    public void shouldReturnTrueForRestoreActiveTasksIfTaskAdded() {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RESTORING).build();
        stateUpdater.add(task);

        assertTrue(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldReturnTrueForRestoreActiveTasksIfTaskUpdating() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);
        verifyRestoredActiveTasks();
        verifyUpdatingTasks(task);
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();

        assertTrue(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldReturnTrueForRestoreActiveTasksIfTaskRestored() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(true);
        stateUpdater.start();
        stateUpdater.add(task);
        verifyRestoredActiveTasks(task);
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();

        assertTrue(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldReturnTrueForRestoreActiveTasksIfTaskRemoved() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);
        stateUpdater.remove(task.id());
        verifyRestoredActiveTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks(task);
        verifyPausedTasks();

        assertTrue(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldReturnTrueForRestoreActiveTasksIfTaskFailed() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        final TaskCorruptedException taskCorruptedException = new TaskCorruptedException(mkSet(task.id()));
        doThrow(taskCorruptedException).when(changelogReader).restore(mkMap(mkEntry(TASK_0_0, task)));
        stateUpdater.start();
        stateUpdater.add(task);
        verifyRestoredActiveTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks(new ExceptionAndTasks(mkSet(task), taskCorruptedException));
        verifyRemovedTasks();
        verifyPausedTasks();

        assertTrue(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldReturnTrueForRestoreActiveTasksIfTaskPaused() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);
        verifyUpdatingTasks(task);
        when(topologyMetadata.isPaused(null)).thenReturn(true);
        verifyRestoredActiveTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks(task);

        assertTrue(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldReturnFalseForRestoreActiveTasksIfTaskRemovedFromStateUpdater() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(true);
        stateUpdater.start();
        stateUpdater.add(task);
        stateUpdater.drainRestoredActiveTasks(Duration.ofMillis(VERIFICATION_TIMEOUT));
        verifyRestoredActiveTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();

        assertFalse(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldReturnTrueForRestoreActiveTasksIfStandbyTask() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted())
            .thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);
        verifyRestoredActiveTasks();
        verifyUpdatingTasks(task);
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();

        assertFalse(stateUpdater.restoresActiveTasks());
    }

    @Test
    public void shouldDrainRestoredActiveTasks() throws Exception {
        assertTrue(stateUpdater.drainRestoredActiveTasks(Duration.ZERO).isEmpty());

        final StreamTask task1 = statelessTask(TASK_0_0).inState(State.RESTORING).build();
        stateUpdater.start();
        stateUpdater.add(task1);

        verifyDrainingRestoredActiveTasks(task1);

        final StreamTask task2 = statelessTask(TASK_1_1).inState(State.RESTORING).build();
        final StreamTask task3 = statelessTask(TASK_1_0).inState(State.RESTORING).build();
        final StreamTask task4 = statelessTask(TASK_0_2).inState(State.RESTORING).build();
        stateUpdater.add(task2);
        stateUpdater.add(task3);
        stateUpdater.add(task4);

        verifyDrainingRestoredActiveTasks(task2, task3, task4);
    }

    @Test
    public void shouldUpdateSingleStandbyTask() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0))
            .inState(State.RUNNING).build();
        shouldUpdateStandbyTasks(task);
    }

    @Test
    public void shouldUpdateMultipleStandbyTasks() throws Exception {
        final StandbyTask task1 = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        final StandbyTask task2 = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        final StandbyTask task3 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        shouldUpdateStandbyTasks(task1, task2, task3);
    }

    private void shouldUpdateStandbyTasks(final StandbyTask... tasks) throws Exception {
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();

        for (final StandbyTask task : tasks) {
            stateUpdater.add(task);
        }

        verifyUpdatingStandbyTasks(tasks);
        verifyRestoredActiveTasks();
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();
        for (final StandbyTask task : tasks) {
            verify(changelogReader).register(task.changelogPartitions(), task.stateManager());
        }
        verify(changelogReader).transitToUpdateStandby();
        verify(changelogReader, timeout(VERIFICATION_TIMEOUT).atLeast(1)).restore(anyMap());
        verify(changelogReader, never()).enforceRestoreActive();
    }

    @Test
    public void shouldRestoreActiveStatefulTasksAndUpdateStandbyTasks() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask task2 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask task3 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final StandbyTask task4 = standbyTask(TASK_1_1, mkSet(TOPIC_PARTITION_D_0)).inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);
        stateUpdater.add(task4);

        verifyRestoredActiveTasks(task2, task1);
        verifyCheckpointTasks(true, task2, task1);
        verifyUpdatingStandbyTasks(task4, task3);
        verifyExceptionsAndFailedTasks();
        verifyRemovedTasks();
        verifyPausedTasks();
        verify(changelogReader).register(task1.changelogPartitions(), task1.stateManager());
        verify(changelogReader).register(task2.changelogPartitions(), task2.stateManager());
        verify(changelogReader).register(task3.changelogPartitions(), task3.stateManager());
        verify(changelogReader).register(task4.changelogPartitions(), task4.stateManager());
        verify(changelogReader, atLeast(3)).restore(anyMap());
        final InOrder orderVerifier = inOrder(changelogReader, task1, task2);
        orderVerifier.verify(changelogReader, times(2)).enforceRestoreActive();
        orderVerifier.verify(changelogReader).transitToUpdateStandby();
    }

    @Test
    public void shouldRestoreActiveStatefulTaskThenUpdateStandbyTaskAndAgainRestoreActiveStatefulTask() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final StreamTask task3 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs())
            .thenReturn(Collections.emptySet())
            .thenReturn(mkSet(TOPIC_PARTITION_A_0))
            .thenReturn(mkSet(TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        verifyRestoredActiveTasks(task1);
        verifyCheckpointTasks(true, task1);
        verifyUpdatingStandbyTasks(task2);
        final InOrder orderVerifier = inOrder(changelogReader);
        orderVerifier.verify(changelogReader, times(1)).enforceRestoreActive();
        orderVerifier.verify(changelogReader, times(1)).transitToUpdateStandby();

        stateUpdater.add(task3);

        verifyRestoredActiveTasks(task1, task3);
        verifyCheckpointTasks(true, task3);
        orderVerifier.verify(changelogReader, times(1)).enforceRestoreActive();
        orderVerifier.verify(changelogReader, times(1)).transitToUpdateStandby();
    }

    @Test
    public void shouldUpdateStandbyTaskAfterAllActiveStatefulTasksFailed() throws Exception {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask activeTask2 = statefulTask(TASK_0_1, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final TaskCorruptedException taskCorruptedException =
            new TaskCorruptedException(mkSet(activeTask1.id(), activeTask2.id()));
        final Map<TaskId, Task> updatingTasks1 = mkMap(
            mkEntry(activeTask1.id(), activeTask1),
            mkEntry(activeTask2.id(), activeTask2),
            mkEntry(standbyTask.id(), standbyTask)
        );
        doThrow(taskCorruptedException).doReturn(0L).when(changelogReader).restore(updatingTasks1);
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();

        stateUpdater.add(activeTask1);
        stateUpdater.add(activeTask2);
        stateUpdater.add(standbyTask);

        final ExceptionAndTasks expectedExceptionAndTasks =
            new ExceptionAndTasks(mkSet(activeTask1, activeTask2), taskCorruptedException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        final InOrder orderVerifier = inOrder(changelogReader);
        orderVerifier.verify(changelogReader, atLeast(1)).enforceRestoreActive();
        orderVerifier.verify(changelogReader).transitToUpdateStandby();
    }
    
    @Test
    public void shouldNotTransitToStandbyAgainAfterStandbyTaskFailed() throws Exception {
        final StandbyTask task1 = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        final StandbyTask task2 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        final Map<TaskId, Task> updatingTasks = mkMap(
                mkEntry(task1.id(), task1),
                mkEntry(task2.id(), task2)
        );
        final TaskCorruptedException taskCorruptedException = new TaskCorruptedException(mkSet(task1.id()));
        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task1), taskCorruptedException);
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        doThrow(taskCorruptedException).doReturn(0L).when(changelogReader).restore(updatingTasks);

        stateUpdater.start();
        stateUpdater.add(task1);
        stateUpdater.add(task2);

        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verify(changelogReader, times(1)).transitToUpdateStandby();
    }

    @Test
    public void shouldUpdateStandbyTaskAfterAllActiveStatefulTasksRemoved() throws Exception {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask activeTask2 = statefulTask(TASK_0_1, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(activeTask1);
        stateUpdater.add(activeTask2);
        stateUpdater.add(standbyTask);
        verifyUpdatingTasks(activeTask1, activeTask2, standbyTask);

        stateUpdater.remove(activeTask1.id());
        stateUpdater.remove(activeTask2.id());

        verifyRemovedTasks(activeTask1, activeTask2);
        final InOrder orderVerifier = inOrder(changelogReader);
        orderVerifier.verify(changelogReader, atLeast(1)).enforceRestoreActive();
        orderVerifier.verify(changelogReader).transitToUpdateStandby();
    }

    @Test
    public void shouldNotSwitchTwiceToUpdatingStandbyTaskIfStandbyTaskIsRemoved() throws Exception {
        final StandbyTask standbyTask1 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final StandbyTask standbyTask2 = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(standbyTask1);
        stateUpdater.add(standbyTask2);
        verifyUpdatingTasks(standbyTask1, standbyTask2);

        stateUpdater.remove(standbyTask2.id());

        verifyRemovedTasks(standbyTask2);
        verify(changelogReader).transitToUpdateStandby();
    }

    @Test
    public void shouldRemoveActiveStatefulTask() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldRemoveStatefulTask(task);
    }

    @Test
    public void shouldRemoveStandbyTask() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldRemoveStatefulTask(task);
    }

    private void shouldRemoveStatefulTask(final Task task) throws Exception {
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);

        stateUpdater.remove(task.id());

        verifyRemovedTasks(task);
        verifyCheckpointTasks(true, task);
        verifyRestoredActiveTasks();
        verifyUpdatingTasks();
        verifyPausedTasks();
        verifyExceptionsAndFailedTasks();
        verify(changelogReader).unregister(task.changelogPartitions());
    }

    @Test
    public void shouldRemovePausedTask() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();

        stateUpdater.start();
        stateUpdater.add(task1);
        stateUpdater.add(task2);
        verifyUpdatingTasks(task1, task2);

        when(topologyMetadata.isPaused(null)).thenReturn(true);
        verifyPausedTasks(task1, task2);
        verifyRemovedTasks();
        verifyUpdatingTasks();

        stateUpdater.remove(task1.id());
        stateUpdater.remove(task2.id());

        verifyRemovedTasks(task1, task2);
        verifyPausedTasks();
        verifyCheckpointTasks(true, task1, task2);
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
        verify(changelogReader).unregister(task1.changelogPartitions());
        verify(changelogReader).unregister(task2.changelogPartitions());
    }

    @Test
    public void shouldNotRemoveActiveStatefulTaskFromRestoredActiveTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldNotRemoveTaskFromRestoredActiveTasks(task);
    }

    @Test
    public void shouldNotRemoveStatelessTaskFromRestoredActiveTasks() throws Exception {
        final StreamTask task = statelessTask(TASK_0_0).inState(State.RESTORING).build();
        shouldNotRemoveTaskFromRestoredActiveTasks(task);
    }

    private void shouldNotRemoveTaskFromRestoredActiveTasks(final StreamTask task) throws Exception {
        final StreamTask controlTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs()).thenReturn(Collections.singleton(TOPIC_PARTITION_A_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);
        stateUpdater.add(controlTask);
        verifyRestoredActiveTasks(task);

        stateUpdater.remove(task.id());
        stateUpdater.remove(controlTask.id());

        verifyRemovedTasks(controlTask);
        verifyRestoredActiveTasks(task);
        verifyUpdatingTasks();
        verifyPausedTasks();
        verifyExceptionsAndFailedTasks();
    }

    @Test
    public void shouldNotRemoveActiveStatefulTaskFromFailedTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldNotRemoveTaskFromFailedTasks(task);
    }

    @Test
    public void shouldNotRemoveStandbyTaskFromFailedTasks() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldNotRemoveTaskFromFailedTasks(task);
    }

    private void shouldNotRemoveTaskFromFailedTasks(final Task task) throws Exception {
        final StreamTask controlTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StreamsException streamsException = new StreamsException("Something happened", task.id());
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task.id(), task),
            mkEntry(controlTask.id(), controlTask)
        );
        doThrow(streamsException)
            .doReturn(0L)
            .when(changelogReader).restore(updatingTasks);
        stateUpdater.start();

        stateUpdater.add(task);
        stateUpdater.add(controlTask);
        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task), streamsException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);

        stateUpdater.remove(task.id());
        stateUpdater.remove(controlTask.id());

        verifyRemovedTasks(controlTask);
        verifyPausedTasks();
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks();
        verifyRestoredActiveTasks();
    }

    @Test
    public void shouldPauseActiveStatefulTask() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldPauseStatefulTask(task);
        verify(changelogReader, never()).transitToUpdateStandby();
    }

    @Test
    public void shouldPauseStandbyTask() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldPauseStatefulTask(task);
        verify(changelogReader, times(1)).transitToUpdateStandby();
    }

    @Test
    public void shouldPauseActiveTaskAndTransitToUpdateStandby() throws Exception {
        final StreamTask task1 = statefulTask(TASK_A_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_B_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();

        when(topologyMetadata.isPaused(task1.id().topologyName())).thenReturn(false).thenReturn(true);

        stateUpdater.start();
        stateUpdater.add(task1);
        stateUpdater.add(task2);

        verifyPausedTasks(task1);
        verifyCheckpointTasks(true, task1);
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
        verifyUpdatingTasks(task2);
        verifyExceptionsAndFailedTasks();
        verify(changelogReader, times(1)).enforceRestoreActive();
        verify(changelogReader, times(1)).transitToUpdateStandby();
    }

    @Test
    public void shouldPauseStandbyTaskAndNotTransitToRestoreActive() throws Exception {
        final StandbyTask task1 = standbyTask(TASK_A_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        final StandbyTask task2 = standbyTask(TASK_B_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();

        when(topologyMetadata.isPaused(task1.id().topologyName())).thenReturn(false).thenReturn(true);

        stateUpdater.start();
        stateUpdater.add(task1);
        stateUpdater.add(task2);

        verifyPausedTasks(task1);
        verifyUpdatingTasks(task2);
        verifyCheckpointTasks(true, task1);
        verify(changelogReader, never()).enforceRestoreActive();
    }

    private void shouldPauseStatefulTask(final Task task) throws Exception {
        stateUpdater.start();
        stateUpdater.add(task);
        verifyUpdatingTasks(task);

        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyPausedTasks(task);
        verifyCheckpointTasks(true, task);
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
    }

    @Test
    public void shouldNotPausingNonExistTasks() throws Exception {
        stateUpdater.start();
        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyPausedTasks();
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
    }

    @Test
    public void shouldNotPauseActiveStatefulTaskInRestoredActiveTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask controlTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();

        when(changelogReader.completedChangelogs()).thenReturn(Collections.singleton(TOPIC_PARTITION_A_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);
        stateUpdater.add(controlTask);
        verifyRestoredActiveTasks(task);
        verifyUpdatingTasks(controlTask);

        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyPausedTasks(controlTask);
        verifyRestoredActiveTasks(task);
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
    }

    @Test
    public void shouldNotPauseActiveStatefulTaskInFailedTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldNotPauseTaskInFailedTasks(task);
    }

    @Test
    public void shouldNotPauseStandbyTaskInFailedTasks() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldNotPauseTaskInFailedTasks(task);
    }

    private void shouldNotPauseTaskInFailedTasks(final Task task) throws Exception {
        final StreamTask controlTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();

        final StreamsException streamsException = new StreamsException("Something happened", task.id());
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task.id(), task),
            mkEntry(controlTask.id(), controlTask)
        );
        doThrow(streamsException)
            .doReturn(0L)
            .when(changelogReader).restore(updatingTasks);
        stateUpdater.start();

        stateUpdater.add(task);
        stateUpdater.add(controlTask);
        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task), streamsException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks(controlTask);

        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyPausedTasks(controlTask);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks();
        verifyRestoredActiveTasks();
    }

    @Test
    public void shouldNotPauseActiveStatefulTaskInRemovedTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldNotPauseTaskInRemovedTasks(task);
    }

    @Test
    public void shouldNotPauseStandbyTaskInRemovedTasks() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldNotPauseTaskInRemovedTasks(task);
    }

    private void shouldNotPauseTaskInRemovedTasks(final Task task) throws Exception {
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);

        stateUpdater.remove(task.id());

        verifyRemovedTasks(task);
        verifyCheckpointTasks(true, task);
        verifyRestoredActiveTasks();
        verifyUpdatingTasks();
        verifyPausedTasks();
        verifyExceptionsAndFailedTasks();

        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyRemovedTasks(task);
        verifyUpdatingTasks();
        verifyPausedTasks();
    }

    @Test
    public void shouldResumeActiveStatefulTask() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldResumeStatefulTask(task);
        verify(changelogReader, times(2)).enforceRestoreActive();
    }

    @Test
    public void shouldIdleWhenAllTasksPaused() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        stateUpdater.start();
        stateUpdater.add(task);

        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyPausedTasks(task);
        verifyIdle();

        when(topologyMetadata.isPaused(null)).thenReturn(false);
        stateUpdater.signalResume();

        verifyPausedTasks();
        verifyUpdatingTasks(task);
    }

    @Test
    public void shouldResumeStandbyTask() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldResumeStatefulTask(task);
        verify(changelogReader, times(2)).transitToUpdateStandby();
    }

    private void shouldResumeStatefulTask(final Task task) throws Exception {
        stateUpdater.start();
        stateUpdater.add(task);
        verifyUpdatingTasks(task);

        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyPausedTasks(task);
        verifyUpdatingTasks();

        when(topologyMetadata.isPaused(null)).thenReturn(false);
        stateUpdater.signalResume();

        verifyPausedTasks();
        verifyUpdatingTasks(task);
    }

    @Test
    public void shouldNotResumeNonExistingTasks() throws Exception {
        stateUpdater.start();

        verifyPausedTasks();
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
        verifyUpdatingTasks();
        verifyExceptionsAndFailedTasks();
    }

    @Test
    public void shouldNotResumeActiveStatefulTaskInRestoredActiveTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask controlTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();

        when(changelogReader.completedChangelogs()).thenReturn(Collections.singleton(TOPIC_PARTITION_A_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);
        stateUpdater.add(controlTask);

        verifyRestoredActiveTasks(task);
        verifyPausedTasks();
        verifyRestoredActiveTasks(task);
        verifyUpdatingTasks(controlTask);
        verifyExceptionsAndFailedTasks();
    }

    @Test
    public void shouldNotResumeActiveStatefulTaskInRemovedTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldNotPauseTaskInRemovedTasks(task);
    }

    @Test
    public void shouldNotResumeStandbyTaskInRemovedTasks() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldNotResumeTaskInRemovedTasks(task);
    }

    private void shouldNotResumeTaskInRemovedTasks(final Task task) throws Exception {
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task);

        verifyUpdatingTasks(task);
        verifyExceptionsAndFailedTasks();

        stateUpdater.remove(task.id());

        verifyRemovedTasks(task);
        verifyUpdatingTasks();

        verifyUpdatingTasks();
    }

    @Test
    public void shouldNotResumeActiveStatefulTaskInFailedTasks() throws Exception {
        final StreamTask task = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        shouldNotPauseTaskInFailedTasks(task);
    }

    @Test
    public void shouldNotResumeStandbyTaskInFailedTasks() throws Exception {
        final StandbyTask task = standbyTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        shouldNotResumeTaskInFailedTasks(task);
    }

    private void shouldNotResumeTaskInFailedTasks(final Task task) throws Exception {
        final StreamTask controlTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StreamsException streamsException = new StreamsException("Something happened", task.id());
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        final Map<TaskId, Task> updatingTasks = mkMap(
                mkEntry(task.id(), task),
                mkEntry(controlTask.id(), controlTask)
        );
        doThrow(streamsException)
                .doReturn(0L)
                .when(changelogReader).restore(updatingTasks);
        stateUpdater.start();

        stateUpdater.add(task);
        stateUpdater.add(controlTask);
        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task), streamsException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks(controlTask);

        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks(controlTask);
    }

    @Test
    public void shouldDrainRemovedTasks() throws Exception {
        assertFalse(stateUpdater.hasRemovedTasks());
        assertTrue(stateUpdater.drainRemovedTasks().isEmpty());
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();

        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        stateUpdater.add(task1);
        stateUpdater.remove(task1.id());

        verifyDrainingRemovedTasks(task1);

        final StreamTask task2 = statefulTask(TASK_1_1, mkSet(TOPIC_PARTITION_C_0)).inState(State.RESTORING).build();
        final StreamTask task3 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask task4 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_D_0)).inState(State.RESTORING).build();
        stateUpdater.add(task2);
        stateUpdater.remove(task2.id());
        stateUpdater.add(task3);
        stateUpdater.remove(task3.id());
        stateUpdater.add(task4);
        stateUpdater.remove(task4.id());

        verifyDrainingRemovedTasks(task2, task3, task4);
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenRestoreThrowsStreamsExceptionWithoutTask() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        final String exceptionMessage = "The Streams were crossed!";
        final StreamsException streamsException = new StreamsException(exceptionMessage);
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2)
        );
        doReturn(0L).doThrow(streamsException).when(changelogReader).restore(updatingTasks);
        stateUpdater.start();

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task1, task2), streamsException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyRemovedTasks();
        verifyPausedTasks();
        verifyUpdatingTasks();
        verifyRestoredActiveTasks();
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenRestoreThrowsStreamsExceptionWithTask() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask task2 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask task3 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final String exceptionMessage = "The Streams were crossed!";
        final StreamsException streamsException1 = new StreamsException(exceptionMessage, task1.id());
        final StreamsException streamsException2 = new StreamsException(exceptionMessage, task3.id());
        final Map<TaskId, Task> updatingTasksBeforeFirstThrow = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3)
        );
        final Map<TaskId, Task> updatingTasksBeforeSecondThrow = mkMap(
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3)
        );
        doReturn(0L)
            .doThrow(streamsException1)
            .when(changelogReader).restore(updatingTasksBeforeFirstThrow);
        doReturn(0L)
            .doThrow(streamsException2)
            .when(changelogReader).restore(updatingTasksBeforeSecondThrow);
        stateUpdater.start();

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final ExceptionAndTasks expectedExceptionAndTasks1 = new ExceptionAndTasks(mkSet(task1), streamsException1);
        final ExceptionAndTasks expectedExceptionAndTasks2 = new ExceptionAndTasks(mkSet(task3), streamsException2);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks1, expectedExceptionAndTasks2);
        verifyUpdatingTasks(task2);
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
        verifyPausedTasks();
    }

    @Test
    public void shouldHandleTaskCorruptedExceptionAndAddFailedTasksToQueue() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        final StreamTask task3 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RESTORING).build();
        final Set<TaskId> expectedTaskIds = mkSet(task1.id(), task2.id());
        final TaskCorruptedException taskCorruptedException = new TaskCorruptedException(expectedTaskIds);
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3)
        );
        doReturn(0L).doThrow(taskCorruptedException).doReturn(0L).when(changelogReader).restore(updatingTasks);
        stateUpdater.start();

        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);

        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task1, task2), taskCorruptedException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks(task3);
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
        verify(changelogReader).unregister(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        verify(task1).markChangelogAsCorrupted(mkSet(TOPIC_PARTITION_A_0));
        verify(task2).markChangelogAsCorrupted(mkSet(TOPIC_PARTITION_B_0));
    }

    @Test
    public void shouldAddFailedTasksToQueueWhenUncaughtExceptionIsThrown() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask task2 = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RUNNING).build();
        final IllegalStateException illegalStateException = new IllegalStateException("Nobody expects the Spanish inquisition!");
        final Map<TaskId, Task> updatingTasks = mkMap(
            mkEntry(task1.id(), task1),
            mkEntry(task2.id(), task2)
        );
        doThrow(illegalStateException).when(changelogReader).restore(updatingTasks);
        stateUpdater.start();

        stateUpdater.add(task1);
        stateUpdater.add(task2);

        final ExceptionAndTasks expectedExceptionAndTasks = new ExceptionAndTasks(mkSet(task1, task2), illegalStateException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks);
        verifyUpdatingTasks();
        verifyRestoredActiveTasks();
        verifyRemovedTasks();
        verifyPausedTasks();
    }

    @Test
    public void shouldDrainFailedTasksAndExceptions() throws Exception {
        assertFalse(stateUpdater.hasExceptionsAndFailedTasks());
        assertTrue(stateUpdater.drainExceptionsAndFailedTasks().isEmpty());

        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StreamTask task2 = statefulTask(TASK_1_1, mkSet(TOPIC_PARTITION_C_0)).inState(State.RESTORING).build();
        final StreamTask task3 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask task4 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_D_0)).inState(State.RESTORING).build();
        final String exceptionMessage = "The Streams were crossed!";
        final StreamsException streamsException1 = new StreamsException(exceptionMessage, task1.id());
        final Map<TaskId, Task> updatingTasks1 = mkMap(
            mkEntry(task1.id(), task1)
        );
        doThrow(streamsException1)
            .when(changelogReader).restore(updatingTasks1);
        final StreamsException streamsException2 = new StreamsException(exceptionMessage, task2.id());
        final StreamsException streamsException3 = new StreamsException(exceptionMessage, task3.id());
        final StreamsException streamsException4 = new StreamsException(exceptionMessage, task4.id());
        final Map<TaskId, Task> updatingTasks2 = mkMap(
            mkEntry(task2.id(), task2),
            mkEntry(task3.id(), task3),
            mkEntry(task4.id(), task4)
        );
        doThrow(streamsException2).when(changelogReader).restore(updatingTasks2);
        final Map<TaskId, Task> updatingTasks3 = mkMap(
            mkEntry(task3.id(), task3),
            mkEntry(task4.id(), task4)
        );
        doThrow(streamsException3).when(changelogReader).restore(updatingTasks3);
        final Map<TaskId, Task> updatingTasks4 = mkMap(
            mkEntry(task4.id(), task4)
        );
        doThrow(streamsException4).when(changelogReader).restore(updatingTasks4);
        stateUpdater.start();

        stateUpdater.add(task1);

        final ExceptionAndTasks expectedExceptionAndTasks1 = new ExceptionAndTasks(mkSet(task1), streamsException1);
        verifyDrainingExceptionsAndFailedTasks(expectedExceptionAndTasks1);

        stateUpdater.add(task2);
        stateUpdater.add(task3);
        stateUpdater.add(task4);

        final ExceptionAndTasks expectedExceptionAndTasks2 = new ExceptionAndTasks(mkSet(task2), streamsException2);
        final ExceptionAndTasks expectedExceptionAndTasks3 = new ExceptionAndTasks(mkSet(task3), streamsException3);
        final ExceptionAndTasks expectedExceptionAndTasks4 = new ExceptionAndTasks(mkSet(task4), streamsException4);
        verifyDrainingExceptionsAndFailedTasks(expectedExceptionAndTasks2, expectedExceptionAndTasks3, expectedExceptionAndTasks4);
    }

    @Test
    public void shouldAutoCheckpointTasksOnInterval() throws Exception {
        final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask task2 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask task3 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final StandbyTask task4 = standbyTask(TASK_1_1, mkSet(TOPIC_PARTITION_D_0)).inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(task1);
        stateUpdater.add(task2);
        stateUpdater.add(task3);
        stateUpdater.add(task4);
        // wait for all tasks added to the thread before advance timer
        verifyUpdatingTasks(task1, task2, task3, task4);

        time.sleep(COMMIT_INTERVAL + 1);

        verifyExceptionsAndFailedTasks();
        verifyCheckpointTasks(false, task1, task2, task3, task4);
    }

    @Test
    public void shouldNotAutoCheckpointTasksIfIntervalNotElapsed() {
        // we need to use a non auto-ticking timer here to control how much time elapsed exactly
        final Time time = new MockTime();
        final DefaultStateUpdater stateUpdater = new DefaultStateUpdater("test-state-updater", metrics, config, null, changelogReader, topologyMetadata, time);
        try {
            final StreamTask task1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
            final StreamTask task2 = statefulTask(TASK_0_2, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
            final StandbyTask task3 = standbyTask(TASK_1_0, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
            final StandbyTask task4 = standbyTask(TASK_1_1, mkSet(TOPIC_PARTITION_D_0)).inState(State.RUNNING).build();
            when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
            when(changelogReader.allChangelogsCompleted()).thenReturn(false);
            stateUpdater.start();
            stateUpdater.add(task1);
            stateUpdater.add(task2);
            stateUpdater.add(task3);
            stateUpdater.add(task4);

            time.sleep(COMMIT_INTERVAL);

            verifyNeverCheckpointTasks(task1, task2, task3, task4);
        } finally {
            stateUpdater.shutdown(Duration.ofMinutes(1));
        }
    }

    private void verifyCheckpointTasks(final boolean enforceCheckpoint, final Task... tasks) {
        for (final Task task : tasks) {
            verify(task, timeout(VERIFICATION_TIMEOUT).atLeast(1)).maybeCheckpoint(enforceCheckpoint);
        }
    }

    private void verifyNeverCheckpointTasks(final Task... tasks) {
        for (final Task task : tasks) {
            verify(task, never()).maybeCheckpoint(anyBoolean());
        }
    }

    @Test
    public void shouldGetTasksFromInputQueue() {
        stateUpdater.shutdown(Duration.ofMillis(Long.MAX_VALUE));

        final StreamTask activeTask1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask activeTask2 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask1 = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final StandbyTask standbyTask2 = standbyTask(TASK_1_1, mkSet(TOPIC_PARTITION_D_0)).inState(State.RUNNING).build();
        final StandbyTask standbyTask3 = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).inState(State.RUNNING).build();
        stateUpdater.add(activeTask1);
        stateUpdater.add(standbyTask1);
        stateUpdater.add(standbyTask2);
        stateUpdater.remove(TASK_0_0);
        stateUpdater.add(activeTask2);
        stateUpdater.add(standbyTask3);

        verifyGetTasks(mkSet(activeTask1, activeTask2), mkSet(standbyTask1, standbyTask2, standbyTask3));
    }

    @Test
    public void shouldGetTasksFromUpdatingTasks() throws Exception {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask activeTask2 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask1 = standbyTask(TASK_0_2, mkSet(TOPIC_PARTITION_C_0)).inState(State.RUNNING).build();
        final StandbyTask standbyTask2 = standbyTask(TASK_1_1, mkSet(TOPIC_PARTITION_D_0)).inState(State.RUNNING).build();
        final StandbyTask standbyTask3 = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(activeTask1);
        stateUpdater.add(standbyTask1);
        stateUpdater.add(standbyTask2);
        stateUpdater.add(activeTask2);
        stateUpdater.add(standbyTask3);
        verifyUpdatingTasks(activeTask1, activeTask2, standbyTask1, standbyTask2, standbyTask3);

        verifyGetTasks(mkSet(activeTask1, activeTask2), mkSet(standbyTask1, standbyTask2, standbyTask3));
    }

    @Test
    public void shouldGetTasksFromRestoredActiveTasks() throws Exception {
        final StreamTask activeTask1 = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask activeTask2 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        when(changelogReader.completedChangelogs()).thenReturn(mkSet(TOPIC_PARTITION_A_0, TOPIC_PARTITION_B_0));
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(activeTask1);
        stateUpdater.add(activeTask2);
        verifyRestoredActiveTasks(activeTask1, activeTask2);

        verifyGetTasks(mkSet(activeTask1, activeTask2), mkSet());

        stateUpdater.drainRestoredActiveTasks(Duration.ofMinutes(1));

        verifyGetTasks(mkSet(), mkSet());
    }

    @Test
    public void shouldGetTasksFromExceptionsAndFailedTasks() throws Exception {
        final StreamTask activeTask1 = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask2 = standbyTask(TASK_1_1, mkSet(TOPIC_PARTITION_D_0)).inState(State.RUNNING).build();
        final StandbyTask standbyTask1 = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).inState(State.RUNNING).build();
        final TaskCorruptedException taskCorruptedException =
            new TaskCorruptedException(mkSet(standbyTask1.id(), standbyTask2.id()));
        final StreamsException streamsException = new StreamsException("The Streams were crossed!", activeTask1.id());
        final Map<TaskId, Task> updatingTasks1 = mkMap(
            mkEntry(activeTask1.id(), activeTask1),
            mkEntry(standbyTask1.id(), standbyTask1),
            mkEntry(standbyTask2.id(), standbyTask2)
        );
        doReturn(0L).doThrow(taskCorruptedException).doReturn(0L).when(changelogReader).restore(updatingTasks1);
        final Map<TaskId, Task> updatingTasks2 = mkMap(
            mkEntry(activeTask1.id(), activeTask1)
        );
        doReturn(0L).doThrow(streamsException).doReturn(0L).when(changelogReader).restore(updatingTasks2);
        stateUpdater.start();
        stateUpdater.add(standbyTask1);
        stateUpdater.add(activeTask1);
        stateUpdater.add(standbyTask2);
        final ExceptionAndTasks expectedExceptionAndTasks1 =
            new ExceptionAndTasks(mkSet(standbyTask1, standbyTask2), taskCorruptedException);
        final ExceptionAndTasks expectedExceptionAndTasks2 = new ExceptionAndTasks(mkSet(activeTask1), streamsException);
        verifyExceptionsAndFailedTasks(expectedExceptionAndTasks1, expectedExceptionAndTasks2);

        verifyGetTasks(mkSet(activeTask1), mkSet(standbyTask1, standbyTask2));

        stateUpdater.drainExceptionsAndFailedTasks();

        verifyGetTasks(mkSet(), mkSet());
    }

    @Test
    public void shouldGetTasksFromRemovedTasks() throws Exception {
        final StreamTask activeTask = statefulTask(TASK_1_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask2 = standbyTask(TASK_1_1, mkSet(TOPIC_PARTITION_D_0)).inState(State.RUNNING).build();
        final StandbyTask standbyTask1 = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_1)).inState(State.RUNNING).build();
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        stateUpdater.start();
        stateUpdater.add(standbyTask1);
        stateUpdater.add(activeTask);
        stateUpdater.add(standbyTask2);
        stateUpdater.remove(standbyTask1.id());
        stateUpdater.remove(standbyTask2.id());
        stateUpdater.remove(activeTask.id());
        verifyRemovedTasks(activeTask, standbyTask1, standbyTask2);

        verifyGetTasks(mkSet(activeTask), mkSet(standbyTask1, standbyTask2));

        stateUpdater.drainRemovedTasks();

        verifyGetTasks(mkSet(), mkSet());
    }

    @Test
    public void shouldGetTasksFromPausedTasks() throws Exception {
        final StreamTask activeTask = statefulTask(TASK_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask = standbyTask(TASK_0_1, mkSet(TOPIC_PARTITION_A_0)).inState(State.RUNNING).build();
        stateUpdater.start();
        stateUpdater.add(activeTask);
        stateUpdater.add(standbyTask);
        verifyUpdatingTasks(activeTask, standbyTask);

        when(topologyMetadata.isPaused(null)).thenReturn(true);

        verifyPausedTasks(activeTask, standbyTask);

        verifyGetTasks(mkSet(activeTask), mkSet(standbyTask));
    }

    @Test
    public void shouldRecordMetrics() throws Exception {
        final StreamTask activeTask1 = statefulTask(TASK_A_0_0, mkSet(TOPIC_PARTITION_A_0)).inState(State.RESTORING).build();
        final StreamTask activeTask2 = statefulTask(TASK_B_0_0, mkSet(TOPIC_PARTITION_B_0)).inState(State.RESTORING).build();
        final StandbyTask standbyTask3 = standbyTask(TASK_A_0_1, mkSet(TOPIC_PARTITION_A_1)).inState(State.RUNNING).build();
        final StandbyTask standbyTask4 = standbyTask(TASK_B_0_1, mkSet(TOPIC_PARTITION_B_1)).inState(State.RUNNING).build();
        final Map<TaskId, Task> tasks1234 = mkMap(
            mkEntry(activeTask1.id(), activeTask1),
            mkEntry(activeTask2.id(), activeTask2),
            mkEntry(standbyTask3.id(), standbyTask3),
            mkEntry(standbyTask4.id(), standbyTask4)
        );
        final Map<TaskId, Task> tasks13 = mkMap(
            mkEntry(activeTask1.id(), activeTask1),
            mkEntry(standbyTask3.id(), standbyTask3)
        );

        when(topologyMetadata.isPaused(activeTask2.id().topologyName())).thenReturn(true);
        when(topologyMetadata.isPaused(standbyTask4.id().topologyName())).thenReturn(true);
        when(changelogReader.completedChangelogs()).thenReturn(Collections.emptySet());
        when(changelogReader.allChangelogsCompleted()).thenReturn(false);
        when(changelogReader.restore(tasks1234)).thenReturn(1L);
        when(changelogReader.restore(tasks13)).thenReturn(1L);
        when(changelogReader.isRestoringActive()).thenReturn(true);

        stateUpdater.start();
        stateUpdater.add(activeTask1);
        stateUpdater.add(activeTask2);
        stateUpdater.add(standbyTask3);
        stateUpdater.add(standbyTask4);

        verifyPausedTasks(activeTask2, standbyTask4);
        assertThat(metrics.metrics().size(), is(11));

        final Map<String, String> tagMap = new LinkedHashMap<>();
        tagMap.put("thread-id", "test-state-updater");

        MetricName metricName = new MetricName("active-restoring-tasks",
            "stream-state-updater-metrics",
            "The number of active tasks currently undergoing restoration",
            tagMap);
        verifyMetric(metrics, metricName, is(1.0));

        metricName = new MetricName("standby-updating-tasks",
            "stream-state-updater-metrics",
            "The number of standby tasks currently undergoing state update",
            tagMap);
        verifyMetric(metrics, metricName, is(1.0));

        metricName = new MetricName("active-paused-tasks",
            "stream-state-updater-metrics",
            "The number of active tasks paused restoring",
            tagMap);
        verifyMetric(metrics, metricName, is(1.0));

        metricName = new MetricName("standby-paused-tasks",
            "stream-state-updater-metrics",
            "The number of standby tasks paused state update",
            tagMap);
        verifyMetric(metrics, metricName, is(1.0));

        metricName = new MetricName("idle-ratio",
            "stream-state-updater-metrics",
            "The fraction of time the thread spent on being idle",
            tagMap);
        verifyMetric(metrics, metricName, greaterThanOrEqualTo(0.0d));

        metricName = new MetricName("active-restore-ratio",
            "stream-state-updater-metrics",
            "The fraction of time the thread spent on restoring active tasks",
            tagMap);
        verifyMetric(metrics, metricName, greaterThanOrEqualTo(0.0d));

        metricName = new MetricName("standby-update-ratio",
            "stream-state-updater-metrics",
            "The fraction of time the thread spent on updating standby tasks",
            tagMap);
        verifyMetric(metrics, metricName, is(0.0d));

        metricName = new MetricName("checkpoint-ratio",
            "stream-state-updater-metrics",
            "The fraction of time the thread spent on checkpointing tasks restored progress",
            tagMap);
        verifyMetric(metrics, metricName, greaterThanOrEqualTo(0.0d));

        metricName = new MetricName("restore-records-rate",
            "stream-state-updater-metrics",
            "The average per-second number of records restored",
            tagMap);
        verifyMetric(metrics, metricName, not(0.0d));

        metricName = new MetricName("restore-call-rate",
            "stream-state-updater-metrics",
            "The average per-second number of restore calls triggered",
            tagMap);
        verifyMetric(metrics, metricName, not(0.0d));

        stateUpdater.shutdown(Duration.ofMinutes(1));
        assertThat(metrics.metrics().size(), is(1));
    }

    @SuppressWarnings("unchecked")
    private static <T> void verifyMetric(final Metrics metrics,
                                         final MetricName metricName,
                                         final Matcher<T> matcher) {
        assertThat(metrics.metrics().get(metricName).metricName().description(), is(metricName.description()));
        assertThat(metrics.metrics().get(metricName).metricName().tags(), is(metricName.tags()));
        assertThat((T) metrics.metrics().get(metricName).metricValue(), matcher);
    }

    private void verifyGetTasks(final Set<StreamTask> expectedActiveTasks,
                                final Set<StandbyTask> expectedStandbyTasks) {
        final Set<Task> tasks = stateUpdater.getTasks();

        assertEquals(expectedActiveTasks.size() + expectedStandbyTasks.size(), tasks.size());
        tasks.forEach(task -> assertTrue(task instanceof ReadOnlyTask));
        final Set<TaskId> actualTaskIds = tasks.stream().map(Task::id).collect(Collectors.toSet());
        final Set<Task> expectedTasks = new HashSet<>(expectedActiveTasks);
        expectedTasks.addAll(expectedStandbyTasks);
        final Set<TaskId> expectedTaskIds = expectedTasks.stream().map(Task::id).collect(Collectors.toSet());
        assertTrue(actualTaskIds.containsAll(expectedTaskIds));

        final Set<StreamTask> activeTasks = stateUpdater.getActiveTasks();
        assertEquals(expectedActiveTasks.size(), activeTasks.size());
        assertTrue(activeTasks.containsAll(expectedActiveTasks));

        final Set<StandbyTask> standbyTasks = stateUpdater.getStandbyTasks();
        assertEquals(expectedStandbyTasks.size(), standbyTasks.size());
        assertTrue(standbyTasks.containsAll(expectedStandbyTasks));
    }

    private void verifyRestoredActiveTasks(final StreamTask... tasks) throws Exception {
        if (tasks.length == 0) {
            waitForCondition(
                () -> stateUpdater.getRestoredActiveTasks().isEmpty(),
                VERIFICATION_TIMEOUT,
                "Did not get empty restored active task within the given timeout!"
            );
        } else {
            final Set<StreamTask> expectedRestoredTasks = mkSet(tasks);
            final Set<StreamTask> restoredTasks = new HashSet<>();
            waitForCondition(
                () -> {
                    restoredTasks.addAll(stateUpdater.getRestoredActiveTasks());
                    return restoredTasks.containsAll(expectedRestoredTasks)
                        && restoredTasks.size() == expectedRestoredTasks.size();
                },
                VERIFICATION_TIMEOUT,
                "Did not get all restored active task within the given timeout!"
            );
        }
    }

    private void verifyDrainingRestoredActiveTasks(final StreamTask... tasks) throws Exception {
        final Set<StreamTask> expectedRestoredTasks = mkSet(tasks);
        final Set<StreamTask> restoredTasks = new HashSet<>();
        waitForCondition(
            () -> {
                restoredTasks.addAll(stateUpdater.drainRestoredActiveTasks(Duration.ofMillis(CALL_TIMEOUT)));
                return restoredTasks.containsAll(expectedRestoredTasks)
                    && restoredTasks.size() == expectedRestoredTasks.size();
            },
            VERIFICATION_TIMEOUT,
            "Did not get all restored active task within the given timeout!"
        );
        assertTrue(stateUpdater.drainRestoredActiveTasks(Duration.ZERO).isEmpty());
    }

    private void verifyUpdatingTasks(final Task... tasks) throws Exception {
        if (tasks.length == 0) {
            waitForCondition(
                () -> stateUpdater.getUpdatingTasks().isEmpty(),
                VERIFICATION_TIMEOUT,
                "Did not get empty updating task within the given timeout!"
            );
        } else {
            final Set<Task> expectedUpdatingTasks = mkSet(tasks);
            final Set<Task> updatingTasks = new HashSet<>();
            waitForCondition(
                () -> {
                    updatingTasks.addAll(stateUpdater.getUpdatingTasks());
                    return updatingTasks.containsAll(expectedUpdatingTasks)
                        && updatingTasks.size() == expectedUpdatingTasks.size();
                },
                VERIFICATION_TIMEOUT,
                "Did not get all updating task within the given timeout!"
            );
        }
    }

    private void verifyUpdatingStandbyTasks(final StandbyTask... tasks) throws Exception {
        final Set<StandbyTask> expectedStandbyTasks = mkSet(tasks);
        final Set<StandbyTask> standbyTasks = new HashSet<>();
        waitForCondition(
            () -> {
                standbyTasks.addAll(stateUpdater.getUpdatingStandbyTasks());
                return standbyTasks.containsAll(expectedStandbyTasks)
                    && standbyTasks.size() == expectedStandbyTasks.size();
            },
            VERIFICATION_TIMEOUT,
            "Did not see all standby task within the given timeout!"
        );
    }

    private void verifyRemovedTasks(final Task... tasks) throws Exception {
        if (tasks.length == 0) {
            waitForCondition(
                () -> stateUpdater.getRemovedTasks().isEmpty(),
                VERIFICATION_TIMEOUT,
                "Did not get empty removed task within the given timeout!"
            );
        } else {
            final Set<Task> expectedRemovedTasks = mkSet(tasks);
            final Set<Task> removedTasks = new HashSet<>();
            waitForCondition(
                () -> {
                    removedTasks.addAll(stateUpdater.getRemovedTasks());
                    return removedTasks.containsAll(expectedRemovedTasks)
                        && removedTasks.size() == expectedRemovedTasks.size();
                },
                VERIFICATION_TIMEOUT,
                "Did not get all removed task within the given timeout!"
            );
        }
    }

    private void verifyIdle() throws Exception {
        waitForCondition(
            () -> stateUpdater.isIdle(),
            VERIFICATION_TIMEOUT,
            "State updater did not enter an idling state!"
        );
    }

    private void verifyPausedTasks(final Task... tasks) throws Exception {
        if (tasks.length == 0) {
            waitForCondition(
                () -> stateUpdater.getPausedTasks().isEmpty(),
                VERIFICATION_TIMEOUT,
                "Did not get empty paused task within the given timeout!"
            );
        } else {
            final Set<Task> expectedPausedTasks = mkSet(tasks);
            final Set<Task> pausedTasks = new HashSet<>();
            waitForCondition(
                () -> {
                    pausedTasks.addAll(stateUpdater.getPausedTasks());
                    return pausedTasks.containsAll(expectedPausedTasks)
                        && pausedTasks.size() == expectedPausedTasks.size();
                },
                VERIFICATION_TIMEOUT,
                "Did not get all paused task within the given timeout!"
            );
        }
    }

    private void verifyDrainingRemovedTasks(final Task... tasks) throws Exception {
        final Set<Task> expectedRemovedTasks = mkSet(tasks);
        final Set<Task> removedTasks = new HashSet<>();
        waitForCondition(
            () -> {
                if (stateUpdater.hasRemovedTasks()) {
                    final Set<Task> drainedTasks = stateUpdater.drainRemovedTasks();
                    assertFalse(drainedTasks.isEmpty());
                    removedTasks.addAll(drainedTasks);
                }
                return removedTasks.containsAll(mkSet(tasks))
                    && removedTasks.size() == expectedRemovedTasks.size();
            },
            VERIFICATION_TIMEOUT,
            "Did not get all restored active task within the given timeout!"
        );
        assertFalse(stateUpdater.hasRemovedTasks());
        assertTrue(stateUpdater.drainRemovedTasks().isEmpty());
    }

    private void verifyExceptionsAndFailedTasks(final ExceptionAndTasks... exceptionsAndTasks) throws Exception {
        final List<ExceptionAndTasks> expectedExceptionAndTasks = Arrays.asList(exceptionsAndTasks);
        final Set<ExceptionAndTasks> failedTasks = new HashSet<>();
        waitForCondition(
            () -> {
                failedTasks.addAll(stateUpdater.getExceptionsAndFailedTasks());
                return failedTasks.containsAll(expectedExceptionAndTasks)
                    && failedTasks.size() == expectedExceptionAndTasks.size();
            },
            VERIFICATION_TIMEOUT,
            "Did not get all exceptions and failed tasks within the given timeout!"
        );
    }

    private void verifyFailedTasks(final Class<? extends RuntimeException> clazz, final Task... tasks) throws Exception {
        final List<Task> expectedFailedTasks = Arrays.asList(tasks);
        final Set<Task> failedTasks = new HashSet<>();
        waitForCondition(
                () -> {
                    for (final ExceptionAndTasks exceptionsAndTasks : stateUpdater.getExceptionsAndFailedTasks()) {
                        if (clazz.isInstance(exceptionsAndTasks.exception())) {
                            failedTasks.addAll(exceptionsAndTasks.getTasks());
                        }
                    }
                    return failedTasks.containsAll(expectedFailedTasks)
                            && failedTasks.size() == expectedFailedTasks.size();
                },
                VERIFICATION_TIMEOUT,
                "Did not get all exceptions and failed tasks within the given timeout!"
        );
    }

    private void verifyDrainingExceptionsAndFailedTasks(final ExceptionAndTasks... exceptionsAndTasks) throws Exception {
        final List<ExceptionAndTasks> expectedExceptionAndTasks = Arrays.asList(exceptionsAndTasks);
        final List<ExceptionAndTasks> failedTasks = new ArrayList<>();
        waitForCondition(
            () -> {
                if (stateUpdater.hasExceptionsAndFailedTasks()) {
                    final List<ExceptionAndTasks> exceptionAndTasks = stateUpdater.drainExceptionsAndFailedTasks();
                    assertFalse(exceptionAndTasks.isEmpty());
                    failedTasks.addAll(exceptionAndTasks);
                }
                return failedTasks.containsAll(expectedExceptionAndTasks)
                    && failedTasks.size() == expectedExceptionAndTasks.size();
            },
            VERIFICATION_TIMEOUT,
            "Did not get all exceptions and failed tasks within the given timeout!"
        );
        assertFalse(stateUpdater.hasExceptionsAndFailedTasks());
        assertTrue(stateUpdater.drainExceptionsAndFailedTasks().isEmpty());
    }
}
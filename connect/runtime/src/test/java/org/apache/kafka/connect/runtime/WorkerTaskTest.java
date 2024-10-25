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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.TaskStatus.Listener;
import org.apache.kafka.connect.runtime.WorkerTask.TaskMetricsGroup;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.reporter.ErrorRecordReporter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class WorkerTaskTest {

    private static final Map<String, String> TASK_PROPS = new HashMap<>();
    static {
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }
    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    @Mock private TaskStatus.Listener statusListener;
    @Mock private ClassLoader loader;
    @Mock private StatusBackingStore statusBackingStore;
    private ConnectMetrics metrics;
    @Mock private ErrorHandlingMetrics errorHandlingMetrics;
    @Mock private RetryWithToleranceOperator<Object> retryWithToleranceOperator;
    @Mock private TransformationChain<Object, SourceRecord> transformationChain;
    @Mock private Supplier<List<ErrorReporter<Object>>> errorReportersSupplier;
    @Mock private Supplier<List<ErrorRecordReporter<Object>>> errorRecordReportersSupplier;

    @BeforeEach
    public void setup() {
        metrics = new MockConnectMetrics();
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void standardStartup() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        WorkerTask<Object, SourceRecord> workerTask = new TestWorkerTask(taskId, statusListener, TargetState.STARTED, loader, metrics, errorHandlingMetrics,
                retryWithToleranceOperator, transformationChain, errorReportersSupplier, errorRecordReportersSupplier, Time.SYSTEM, statusBackingStore);
        workerTask.initialize(TASK_CONFIG);
        workerTask.run();
        workerTask.stop();
        workerTask.awaitStop(1000L);

        verify(statusListener).onStartup(taskId);
        verify(statusListener).onShutdown(taskId);
    }

    @Test
    public void stopBeforeStarting() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        WorkerTask<Object, SourceRecord> workerTask = new TestWorkerTask(taskId, statusListener, TargetState.STARTED, loader, metrics, errorHandlingMetrics,
                retryWithToleranceOperator, transformationChain, errorReportersSupplier, errorRecordReportersSupplier, Time.SYSTEM, statusBackingStore) {

            @Override
            public void initializeAndStart() {
                fail("This method is expected to not be invoked");
            }

            @Override
            public void execute() {
                fail("This method is expected to not be invoked");
            }
        };

        workerTask.initialize(TASK_CONFIG);
        workerTask.stop();
        workerTask.awaitStop(1000L);

        // now run should not do anything
        workerTask.run();
    }

    @Test
    public void cancelBeforeStopping() throws Exception {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);
        final CountDownLatch stopped = new CountDownLatch(1);

        WorkerTask<Object, SourceRecord> workerTask = new TestWorkerTask(taskId, statusListener, TargetState.STARTED, loader, metrics, errorHandlingMetrics,
                retryWithToleranceOperator, transformationChain, errorReportersSupplier, errorRecordReportersSupplier, Time.SYSTEM, statusBackingStore) {

            @Override
            public void execute() {
                try {
                    stopped.await();
                } catch (InterruptedException e) {
                    fail("Unexpected interrupt");
                }
            }

            // Trigger task shutdown immediately after start. The task will block in its execute() method
            // until the stopped latch is counted down (i.e. it doesn't actually stop after stop is triggered).
            @Override
            public void initializeAndStart() {
                stop();
            }
        };

        workerTask.initialize(TASK_CONFIG);
        Thread t = new Thread(workerTask);
        t.start();

        workerTask.cancel();
        stopped.countDown();
        t.join();

        verify(statusListener).onStartup(taskId);
        // there should be no other status updates, including shutdown
        verifyNoMoreInteractions(statusListener);
    }

    @Test
    public void testErrorReportersConfigured() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        WorkerTask<Object, SourceRecord> workerTask = new TestWorkerTask(taskId, statusListener, TargetState.STARTED, loader, metrics, errorHandlingMetrics,
                retryWithToleranceOperator, transformationChain, errorReportersSupplier, errorRecordReportersSupplier, Time.SYSTEM, statusBackingStore);

        List<ErrorReporter<Object>> errorReporters = new ArrayList<>();
        when(errorReportersSupplier.get()).thenReturn(errorReporters);

        workerTask.doStart();
        verify(retryWithToleranceOperator).reporters(errorReporters);
    }

    @Test
    public void testErrorReporterConfigurationExceptionPropagation() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        WorkerTask<Object, SourceRecord> workerTask = new TestWorkerTask(taskId, statusListener, TargetState.STARTED, loader, metrics, errorHandlingMetrics,
                retryWithToleranceOperator, transformationChain, errorReportersSupplier, errorRecordReportersSupplier, Time.SYSTEM, statusBackingStore);
        when(errorReportersSupplier.get()).thenThrow(new ConnectException("Failed to create error reporters"));

        assertThrows(ConnectException.class, workerTask::doStart);
    }

    @Test
    public void testCloseClosesManagedResources() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        WorkerTask<Object, SourceRecord> workerTask = new TestWorkerTask(taskId, statusListener, TargetState.STARTED, loader, metrics, errorHandlingMetrics,
                retryWithToleranceOperator, transformationChain, errorReportersSupplier, errorRecordReportersSupplier, Time.SYSTEM, statusBackingStore);

        workerTask.doClose();

        verify(retryWithToleranceOperator).close();
        verify(transformationChain).close();
    }

    @Test
    public void testCloseClosesManagedResourcesIfSubclassThrows() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);

        WorkerTask<Object, SourceRecord> workerTask = new TestWorkerTask(taskId, statusListener, TargetState.STARTED, loader, metrics, errorHandlingMetrics,
                retryWithToleranceOperator, transformationChain, errorReportersSupplier, errorRecordReportersSupplier, Time.SYSTEM, statusBackingStore) {
            @Override
            protected void close() {
                throw new ConnectException("Failure during close");
            }
        };

        assertThrows(ConnectException.class, workerTask::doClose);

        verify(retryWithToleranceOperator).close();
        verify(transformationChain).close();
    }

    @Test
    public void updateMetricsOnListenerEventsForStartupPauseResumeAndShutdown() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);
        ConnectMetrics metrics = new MockConnectMetrics();
        TaskMetricsGroup group = new TaskMetricsGroup(taskId, metrics, statusListener);

        group.onStartup(taskId);
        assertRunningMetric(group);
        group.onPause(taskId);
        assertPausedMetric(group);
        group.onResume(taskId);
        assertRunningMetric(group);
        group.onShutdown(taskId);
        assertStoppedMetric(group);

        verify(statusListener).onStartup(taskId);
        verify(statusListener).onPause(taskId);
        verify(statusListener).onResume(taskId);
        verify(statusListener).onShutdown(taskId);
    }

    @Test
    public void updateMetricsOnListenerEventsForStartupPauseResumeAndFailure() {
        ConnectorTaskId taskId = new ConnectorTaskId("foo", 0);
        MockConnectMetrics metrics = new MockConnectMetrics();
        MockTime time = metrics.time();
        ConnectException error = new ConnectException("error");
        TaskMetricsGroup group = new TaskMetricsGroup(taskId, metrics, statusListener);

        time.sleep(1000L);
        group.onStartup(taskId);
        assertRunningMetric(group);

        time.sleep(2000L);
        group.onPause(taskId);
        assertPausedMetric(group);

        time.sleep(3000L);
        group.onResume(taskId);
        assertRunningMetric(group);

        time.sleep(4000L);
        group.onPause(taskId);
        assertPausedMetric(group);

        time.sleep(5000L);
        group.onResume(taskId);
        assertRunningMetric(group);

        time.sleep(6000L);
        group.onFailure(taskId, error);
        assertFailedMetric(group);

        time.sleep(7000L);
        group.onShutdown(taskId);
        assertStoppedMetric(group);

        verify(statusListener).onStartup(taskId);
        verify(statusListener, times(2)).onPause(taskId);
        verify(statusListener, times(2)).onResume(taskId);
        verify(statusListener).onFailure(taskId, error);
        verify(statusListener).onShutdown(taskId);

        long totalTime = 27000L;
        double pauseTimeRatio = (double) (3000L + 5000L) / totalTime;
        double runningTimeRatio = (double) (2000L + 4000L + 6000L) / totalTime;
        assertEquals(pauseTimeRatio, metrics.currentMetricValueAsDouble(group.metricGroup(), "pause-ratio"), 0.000001d);
        assertEquals(runningTimeRatio, metrics.currentMetricValueAsDouble(group.metricGroup(), "running-ratio"), 0.000001d);
    }

    private abstract static class TestSinkTask extends SinkTask {
    }

    private static class TestWorkerTask extends WorkerTask<Object, SourceRecord> {

        public TestWorkerTask(ConnectorTaskId id, Listener statusListener, TargetState initialState, ClassLoader loader,
                              ConnectMetrics connectMetrics, ErrorHandlingMetrics errorHandlingMetrics,
                              RetryWithToleranceOperator<Object> retryWithToleranceOperator,
                              TransformationChain<Object, SourceRecord> transformationChain,
                              Supplier<List<ErrorReporter<Object>>> errorReporterSupplier,
                              Supplier<List<ErrorRecordReporter<Object>>> errorRecordReporterSupplier,
                              Time time, StatusBackingStore statusBackingStore) {
            super(id, statusListener, initialState, loader, connectMetrics, errorHandlingMetrics,
                    retryWithToleranceOperator, transformationChain, errorReporterSupplier, errorRecordReporterSupplier, time, statusBackingStore);
        }

        @Override
        public void initialize(TaskConfig taskConfig) {
        }

        @Override
        protected void initializeAndStart() {
        }

        @Override
        protected void execute() {
        }

        @Override
        protected void close() {
        }
    }

    protected void assertFailedMetric(TaskMetricsGroup metricsGroup) {
        assertEquals(AbstractStatus.State.FAILED, metricsGroup.state());
    }

    protected void assertPausedMetric(TaskMetricsGroup metricsGroup) {
        assertEquals(AbstractStatus.State.PAUSED, metricsGroup.state());
    }

    protected void assertRunningMetric(TaskMetricsGroup metricsGroup) {
        assertEquals(AbstractStatus.State.RUNNING, metricsGroup.state());
    }

    protected void assertStoppedMetric(TaskMetricsGroup metricsGroup) {
        assertEquals(AbstractStatus.State.UNASSIGNED, metricsGroup.state());
    }
}

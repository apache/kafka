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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Frequencies;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.AbstractStatus.State;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.LoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Handles processing for an individual task. This interface only provides the basic methods
 * used by {@link Worker} to manage the tasks. Implementations combine a user-specified Task with
 * Kafka to create a data flow.
 *
 * Note on locking: since the task runs in its own thread, special care must be taken to ensure
 * that state transitions are reported correctly, in particular since some state transitions are
 * asynchronous (e.g. pause/resume). For example, changing the state to paused could cause a race
 * if the task fails at the same time. To protect from these cases, we synchronize status updates
 * using the WorkerTask's monitor.
 */
abstract class WorkerTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WorkerTask.class);
    private static final String THREAD_NAME_PREFIX = "task-thread-";

    protected final ConnectorTaskId id;
    private final TaskStatus.Listener statusListener;
    protected final ClassLoader loader;
    protected final StatusBackingStore statusBackingStore;
    protected final Time time;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final TaskMetricsGroup taskMetricsGroup;
    private volatile TargetState targetState;
    private volatile boolean stopping;   // indicates whether the Worker has asked the task to stop
    private volatile boolean cancelled;  // indicates whether the Worker has cancelled the task (e.g. because of slow shutdown)

    protected final RetryWithToleranceOperator retryWithToleranceOperator;

    public WorkerTask(ConnectorTaskId id,
                      TaskStatus.Listener statusListener,
                      TargetState initialState,
                      ClassLoader loader,
                      ConnectMetrics connectMetrics,
                      RetryWithToleranceOperator retryWithToleranceOperator,
                      Time time,
                      StatusBackingStore statusBackingStore) {
        this.id = id;
        this.taskMetricsGroup = new TaskMetricsGroup(this.id, connectMetrics, statusListener);
        this.statusListener = taskMetricsGroup;
        this.loader = loader;
        this.targetState = initialState;
        this.stopping = false;
        this.cancelled = false;
        this.taskMetricsGroup.recordState(this.targetState);
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.time = time;
        this.statusBackingStore = statusBackingStore;
    }

    public ConnectorTaskId id() {
        return id;
    }

    public ClassLoader loader() {
        return loader;
    }

    /**
     * Initialize the task for execution.
     *
     * @param taskConfig initial configuration
     */
    public abstract void initialize(TaskConfig taskConfig);


    private void triggerStop() {
        synchronized (this) {
            stopping = true;

            // wakeup any threads that are waiting for unpause
            this.notifyAll();
        }
    }

    /**
     * Stop this task from processing messages. This method does not block, it only triggers
     * shutdown. Use #{@link #awaitStop} to block until completion.
     */
    public void stop() {
        triggerStop();
    }

    /**
     * Cancel this task. This won't actually stop it, but it will prevent the state from being
     * updated when it eventually does shutdown.
     */
    public void cancel() {
        cancelled = true;
    }

    /**
     * Wait for this task to finish stopping.
     *
     * @param timeoutMs time in milliseconds to await stop
     * @return true if successful, false if the timeout was reached
     */
    public boolean awaitStop(long timeoutMs) {
        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    /**
     * Remove all metrics published by this task.
     */
    public void removeMetrics() {
        taskMetricsGroup.close();
    }

    protected abstract void execute();

    protected abstract void close();

    protected boolean isStopping() {
        return stopping;
    }

    protected boolean isCancelled() {
        return cancelled;
    }

    private void doClose() {
        try {
            close();
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception during shutdown", this, t);
            throw t;
        }
    }

    private void doRun() throws InterruptedException {
        try {
            synchronized (this) {
                if (stopping)
                    return;

                if (targetState == TargetState.PAUSED) {
                    onPause();
                    if (!awaitUnpause()) return;
                }

                statusListener.onStartup(id);
            }

            execute();
        } catch (Throwable t) {
            if (cancelled) {
                log.warn("{} After being scheduled for shutdown, the orphan task threw an uncaught exception. A newer instance of this task might be already running", this, t);
            } else if (stopping) {
                log.warn("{} After being scheduled for shutdown, task threw an uncaught exception.", this, t);
            } else {
                log.error("{} Task threw an uncaught and unrecoverable exception. Task is being killed and will not recover until manually restarted", this, t);
                throw t;
            }
        } finally {
            doClose();
        }
    }

    private void onShutdown() {
        synchronized (this) {
            triggerStop();

            // if we were cancelled, skip the status update since the task may have already been
            // started somewhere else
            if (!cancelled)
                statusListener.onShutdown(id);
        }
    }

    protected void onFailure(Throwable t) {
        synchronized (this) {
            triggerStop();

            // if we were cancelled, skip the status update since the task may have already been
            // started somewhere else
            if (!cancelled)
                statusListener.onFailure(id, t);
        }
    }

    protected synchronized void onPause() {
        statusListener.onPause(id);
    }

    protected synchronized void onResume() {
        statusListener.onResume(id);
    }

    @Override
    public void run() {
        // Clear all MDC parameters, in case this thread is being reused
        LoggingContext.clear();

        try (LoggingContext loggingContext = LoggingContext.forTask(id())) {
            ClassLoader savedLoader = Plugins.compareAndSwapLoaders(loader);
            String savedName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(THREAD_NAME_PREFIX + id);
                doRun();
                onShutdown();
            } catch (Throwable t) {
                onFailure(t);

                if (t instanceof Error)
                    throw (Error) t;
            } finally {
                Thread.currentThread().setName(savedName);
                Plugins.compareAndSwapLoaders(savedLoader);
                shutdownLatch.countDown();
            }
        }
    }

    public boolean shouldPause() {
        return this.targetState == TargetState.PAUSED;
    }

    /**
     * Await task resumption.
     *
     * @return true if the task's target state is not paused, false if the task is shutdown before resumption
     * @throws InterruptedException
     */
    protected boolean awaitUnpause() throws InterruptedException {
        synchronized (this) {
            while (targetState == TargetState.PAUSED) {
                if (stopping)
                    return false;
                this.wait();
            }
            return true;
        }
    }

    public void transitionTo(TargetState state) {
        synchronized (this) {
            // ignore the state change if we are stopping
            if (stopping)
                return;

            this.targetState = state;
            this.notifyAll();
        }
    }

    /**
     * Include this topic to the set of active topics for the connector that this worker task
     * is running. This information is persisted in the status backing store used by this worker.
     *
     * @param topic the topic to mark as active for this connector
     */
    protected void recordActiveTopic(String topic) {
        if (statusBackingStore.getTopic(id.connector(), topic) != null) {
            // The topic is already recorded as active. No further action is required.
            return;
        }
        statusBackingStore.put(new TopicStatus(topic, id, time.milliseconds()));
    }

    /**
     * Record that offsets have been committed.
     *
     * @param duration the length of time in milliseconds for the commit attempt to complete
     */
    protected void recordCommitSuccess(long duration) {
        taskMetricsGroup.recordCommit(duration, null);
    }

    /**
     * Record that offsets have been committed.
     *
     * @param duration the length of time in milliseconds for the commit attempt to complete
     * @param error the unexpected error that occurred; may be null in the case of timeouts or interruptions
     */
    protected void recordCommitFailure(long duration, Throwable error) {
        taskMetricsGroup.recordCommit(duration, error);
    }

    /**
     * Record that a batch of records has been processed.
     *
     * @param size the number of records in the batch
     */
    protected void recordBatch(int size) {
        taskMetricsGroup.recordBatch(size);
    }

    TaskMetricsGroup taskMetricsGroup() {
        return taskMetricsGroup;
    }

    static class TaskMetricsGroup implements TaskStatus.Listener {
        private final TaskStatus.Listener delegateListener;
        private final MetricGroup metricGroup;
        private final Time time;
        private final StateTracker taskStateTimer;
        private final Sensor commitTime;
        private final Sensor batchSize;
        private final Sensor commitAttempts;

        public TaskMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics, TaskStatus.Listener statusListener) {
            delegateListener = statusListener;
            time = connectMetrics.time();
            taskStateTimer = new StateTracker();
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.taskGroupName(),
                    registry.connectorTagName(), id.connector(),
                    registry.taskTagName(), Integer.toString(id.task()));
            // prevent collisions by removing any previously created metrics in this group.
            metricGroup.close();

            metricGroup.addValueMetric(registry.taskStatus, now ->
                taskStateTimer.currentState().toString().toLowerCase(Locale.getDefault())
            );

            addRatioMetric(State.RUNNING, registry.taskRunningRatio);
            addRatioMetric(State.PAUSED, registry.taskPauseRatio);

            commitTime = metricGroup.sensor("commit-time");
            commitTime.add(metricGroup.metricName(registry.taskCommitTimeMax), new Max());
            commitTime.add(metricGroup.metricName(registry.taskCommitTimeAvg), new Avg());

            batchSize = metricGroup.sensor("batch-size");
            batchSize.add(metricGroup.metricName(registry.taskBatchSizeMax), new Max());
            batchSize.add(metricGroup.metricName(registry.taskBatchSizeAvg), new Avg());

            MetricName offsetCommitFailures = metricGroup.metricName(registry.taskCommitFailurePercentage);
            MetricName offsetCommitSucceeds = metricGroup.metricName(registry.taskCommitSuccessPercentage);
            Frequencies commitFrequencies = Frequencies.forBooleanValues(offsetCommitFailures, offsetCommitSucceeds);
            commitAttempts = metricGroup.sensor("offset-commit-completion");
            commitAttempts.add(commitFrequencies);
        }

        private void addRatioMetric(final State matchingState, MetricNameTemplate template) {
            MetricName metricName = metricGroup.metricName(template);
            if (metricGroup.metrics().metric(metricName) == null) {
                metricGroup.metrics().addMetric(metricName, (config, now) ->
                    taskStateTimer.durationRatio(matchingState, now));
            }
        }

        void close() {
            metricGroup.close();
        }

        void recordCommit(long duration, Throwable error) {
            if (error == null) {
                commitTime.record(duration);
                commitAttempts.record(1.0d);
            } else {
                commitAttempts.record(0.0d);
            }
        }

        void recordBatch(int size) {
            batchSize.record(size);
        }

        @Override
        public void onStartup(ConnectorTaskId id) {
            taskStateTimer.changeState(State.RUNNING, time.milliseconds());
            delegateListener.onStartup(id);
        }

        @Override
        public void onFailure(ConnectorTaskId id, Throwable cause) {
            taskStateTimer.changeState(State.FAILED, time.milliseconds());
            delegateListener.onFailure(id, cause);
        }

        @Override
        public void onPause(ConnectorTaskId id) {
            taskStateTimer.changeState(State.PAUSED, time.milliseconds());
            delegateListener.onPause(id);
        }

        @Override
        public void onResume(ConnectorTaskId id) {
            taskStateTimer.changeState(State.RUNNING, time.milliseconds());
            delegateListener.onResume(id);
        }

        @Override
        public void onShutdown(ConnectorTaskId id) {
            taskStateTimer.changeState(State.UNASSIGNED, time.milliseconds());
            delegateListener.onShutdown(id);
        }

        @Override
        public void onDeletion(ConnectorTaskId id) {
            taskStateTimer.changeState(State.DESTROYED, time.milliseconds());
            delegateListener.onDeletion(id);
        }

        public void recordState(TargetState state) {
            switch (state) {
                case STARTED:
                    taskStateTimer.changeState(State.RUNNING, time.milliseconds());
                    break;
                case PAUSED:
                    taskStateTimer.changeState(State.PAUSED, time.milliseconds());
                    break;
                default:
                    break;
            }
        }

        public State state() {
            return taskStateTimer.currentState();
        }

        protected MetricGroup metricGroup() {
            return metricGroup;
        }
    }
}

package org.apache.kafka.streams.processor.internals.tasks;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.DefaultStateUpdater;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultTaskExecutor implements TaskExecutor {

    private class TaskExecutorThread extends Thread {

        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final AtomicReference<KafkaFutureImpl<StreamTask>> pauseRequested = new AtomicReference<>(null);

        private final Logger log;

        public TaskExecutorThread(final String name) {
            super(name);
            final String logPrefix = String.format("%s ", name);
            final LogContext logContext = new LogContext(logPrefix);
            log = logContext.logger(DefaultStateUpdater.class);
        }

        @Override
        public void run() {
            log.info("Task executor thread started");
            try {
                while (isRunning.get()) {
                    runOnce(time.milliseconds());
                }
            } finally {
                shutdownGate.countDown();
                log.info("Task executor thread shutdown");
            }
        }

        private void runOnce(final long nowMs) {
            KafkaFutureImpl<StreamTask> pauseFuture;
            if ((pauseFuture = pauseRequested.getAndSet(null)) != null) {
                taskManager.unassignTask(currentTask, DefaultTaskExecutor.this);
                pauseFuture.complete(currentTask);
                currentTask = null;
            }

            if (currentTask == null) {
                currentTask = taskManager.assignNextTask(DefaultTaskExecutor.this);
            }

            if (currentTask.isProcessable(nowMs)) {
                currentTask.process(nowMs);
            } else {
                taskManager.unassignTask(currentTask, DefaultTaskExecutor.this);
                currentTask = null;
            }
        }
    }

    private final Time time;
    private final TaskManager taskManager;

    private StreamTask currentTask = null;
    private TaskExecutorThread taskExecutorThread = null;
    private CountDownLatch shutdownGate;

    public DefaultTaskExecutor(final TaskManager taskManager,
                               final Time time) {
        this.time = time;
        this.taskManager = taskManager;
    }

    @Override
    public void start() {
        if (taskExecutorThread == null) {
            taskExecutorThread = new TaskExecutorThread("task-executor");
            taskExecutorThread.start();
            shutdownGate = new CountDownLatch(1);
        }
    }

    @Override
    public void shutdown(final Duration timeout) {
        if (taskExecutorThread != null) {
            taskExecutorThread.isRunning.set(false);
            taskExecutorThread.interrupt();
            try {
                if (!shutdownGate.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new StreamsException("State updater thread did not shutdown within the timeout");
                }
                taskExecutorThread = null;
            } catch (final InterruptedException ignored) {
            }
        }
    }

    @Override
    public ReadOnlyTask currentTask() {
        return new ReadOnlyTask(currentTask);
    }

    @Override
    public KafkaFuture<StreamTask> unassign() {
        final KafkaFutureImpl<StreamTask> future = new KafkaFutureImpl<>();

        if (taskExecutorThread != null) {
            taskExecutorThread.pauseRequested.set(future);
        } else {
            future.complete(null);
        }

        return future;
    }
}

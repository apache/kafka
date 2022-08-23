package org.apache.kafka.streams.processor.internals.tasks;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;

import java.time.Duration;

public interface TaskExecutor {

    /**
     * Starts the task processor.
     */
    void start();

    /**
     * Shuts down the task processor updater.
     *
     * @param timeout duration how long to wait until the state updater is shut down
     *
     * @throws
     *     org.apache.kafka.streams.errors.StreamsException if the state updater thread cannot shutdown within the timeout
     */
    void shutdown(final Duration timeout);

    /**
     * Get the current assigned processing task. The task returned is read-only and cannot be modified.
     *
     * @return the current processing task
     */
    ReadOnlyTask currentTask();

    /**
     * Unassign the current processing task from the task processor and give it back to the state manager.
     *
     * The paused task must be flushed since it may be committed or closed by the task manager next.
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<StreamTask> unassign();
}

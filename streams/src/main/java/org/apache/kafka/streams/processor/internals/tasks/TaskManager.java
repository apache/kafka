package org.apache.kafka.streams.processor.internals.tasks;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ReadOnlyTask;
import org.apache.kafka.streams.processor.internals.StreamTask;
import java.util.Set;

public interface TaskManager {

    /**
     * Lock the next processible active task for the requested executor. Once the task is locked by
     * the requested task executor, it should not be locked to any other executors until it was
     * returned to the task manager.
     *
     * @param executor the requesting {@link TaskExecutor}
     */
    StreamTask lockNextTask(final TaskExecutor executor);

    /**
     * Unlock the stream task so that it can be processed by other executors later
     * or be removed from the task manager. The requested executor must have locked
     * the task already, otherwise an exception would be thrown.
     *
     * @param executor the requesting {@link TaskExecutor}
     */
    void unlockTask(final StreamTask task, final TaskExecutor executor);

    /**
     * Lock a set of active tasks from the task manager.
     *
     * This function is needed when we need to 1) commit these tasks, 2) remove these tasks.
     *
     * The requested tasks may be locked by {@link TaskExecutor}s at the time, and in that case
     * the task manager need to ask these executors to unlock the tasks first
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<Set<TaskId>> lockTasks(final Set<TaskId> taskIds);

    /**
     * Lock all of the managed active tasks from the task manager.
     *
     * This function is needed when we need to 1) commit these tasks, 2) remove these tasks.
     *
     * The requested tasks may be locked by {@link TaskExecutor}s at the time, and in that case
     * the task manager need to ask these executors to unlock the tasks first
     *
     * This method does not block, instead a future is returned.
     */
    KafkaFuture<Set<TaskId>> lockAllTasks();

    /**
     * Add a new active task to the task manager.
     *
     * @param tasks task to add
     */
    void add(final Set<StreamTask> tasks);

    /**
     * Remove an active task from the task manager.
     *
     * The task to remove must be locked.
     *
     * @param taskId ID of the task to remove
     */
    void remove(final TaskId taskId);

    /**
     * Gets all active tasks that are managed by this manager. The returned tasks are read-only
     * and cannot be manipulated.
     *
     * @return set of all managed active tasks
     */
    Set<ReadOnlyTask> getTasks();
}

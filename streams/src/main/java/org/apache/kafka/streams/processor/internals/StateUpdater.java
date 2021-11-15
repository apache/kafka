package org.apache.kafka.streams.processor.internals;

import java.time.Duration;
import java.util.List;

public interface StateUpdater {

    /**
     * Adds a task (active or standby) to the state updater.
     *
     * The state of the task will be updated.
     *
     * @param task task
     */
    void add(final Task task);

    /**
     * Removes a task (active and standby) from the state updater.
     *
     * A task is removed from the state updater irrespective of whether its state is up-to-date or not.
     *
     * @param task tasks to remove
     */
    void remove(final Task task);

    /**
     * Gets restored active tasks from state restoration/update
     *
     * @param timeout duration how long the calling thread should wait for restored active tasks
     *
     * @return list of active tasks with up-to-date states
     */
    List<StreamTask> getRestoredActiveTasks(final Duration timeout);

    /**
     * Gets a list of tasks that failed during restoration.
     *
     * The exception that caused the failure can be retrieved by {@link Task#getException()}
     *
     * @return failed tasks
     */
    List<Task> getFailedTasks();

    /**
     * Shuts down the state updater.
     *
     * @param timeout duration how long to wait until the state updater is shut down
     */
    void shutdown(final Duration timeout);
}

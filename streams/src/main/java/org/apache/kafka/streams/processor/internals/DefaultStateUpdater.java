package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.TaskId;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultStateUpdater implements StateUpdater {

    private final static String BUG_ERROR_MESSAGE = "This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA/issues or to the dev-mailing list (https://kafka.apache.org/contact).";

    private class StateUpdaterThread extends Thread {

        private final ChangelogReader changelogReader;
        private final ConcurrentMap<TaskId, Task> updatingTasks = new ConcurrentHashMap<>();
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final java.util.function.Consumer<Set<TopicPartition>> offsetResetter;

        public StateUpdaterThread(final String name,
                                  final ChangelogReader changelogReader,
                                  final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
            super(name);
            this.changelogReader = changelogReader;
            this.offsetResetter = offsetResetter;
        }

        @Override
        public void run() {
            try {
                while (isRunning.get()) {
                    try {
                        performActionsOnTasks();
                        restoreTasks();
                        waitIfAllChangelogsCompletelyRead();
                    } catch (final InterruptedException interruptedException) {
                        return;
                    }
                }
            } catch (final Throwable anyOtherError) {
                // ToDo: log that the thread died unexpectedly
            } finally{
                clear();
            }
        }

        private void performActionsOnTasks() throws InterruptedException {
            tasksAndActionsLock.lock();
            try {
                for (final TaskAndAction taskAndAction : getTasksAndActions()) {
                    final Task task = taskAndAction.task;
                    final Action action = taskAndAction.action;
                    switch (action) {
                        case ADD:
                            addTask(task);
                            break;
                        case REMOVE:
                            removeTask(task);
                            break;
                        case RECYCLE:
                            ;
                            break;

                    }
                }
            } finally {
                tasksAndActionsLock.unlock();
            }
        }

        private void restoreTasks() throws InterruptedException {
            try {
                // ToDo: Prioritize restoration of active tasks over standby tasks
                //                changelogReader.enforceRestoreActive();
                changelogReader.restore(updatingTasks);
            } catch (final TaskCorruptedException taskCorruptedException) {
                handleTaskCorruptedException(taskCorruptedException);
            } catch (final StreamsException streamsException) {
                handleStreamsException(streamsException);
            }
            final Set<TopicPartition> completedChangelogs = changelogReader.completedChangelogs();
            final List<Task> activeTasks = updatingTasks.values().stream().filter(Task::isActive).collect(Collectors.toList());
            for (final Task task : activeTasks) {
                endRestorationIfChangelogsCompletelyRead(task, completedChangelogs);
            }
        }

        private void endRestorationIfChangelogsCompletelyRead(final Task task,
                                                              final Set<TopicPartition> restoredChangelogs) {
            catchException(() -> {
                final Collection<TopicPartition> taskChangelogPartitions = task.changelogPartitions();
                if (restoredChangelogs.containsAll(taskChangelogPartitions)) {
                    task.completeRestoration(offsetResetter);
                    addTaskToRestoredTasks((StreamTask) task);
                    updatingTasks.remove(task.id());
                }
                return null;
            }, task);
        }

        private void waitIfAllChangelogsCompletelyRead() throws InterruptedException {
            if (isRunning.get() && changelogReader.allChangelogsCompleted()) {
                tasksAndActionsLock.lock();
                try {
                    while (tasksAndActions.isEmpty()) {
                        tasksAndActionsCondition.await();
                    }
                } finally {
                    tasksAndActionsLock.unlock();
                }
            }
        }

        private List<TaskAndAction> getTasksAndActions() {
            final List<TaskAndAction> tasksAndActionsToProcess = new ArrayList<>(tasksAndActions);
            tasksAndActions.clear();
            return tasksAndActionsToProcess;
        }

        private void addTask(final Task task) {
            catchException(() -> {
                final Task.State state = task.state();
                switch (state) {

                    case CREATED:
                        task.initializeIfNeeded();
                        // Todo: catch exceptions and clear task timeout
                        if (isStateless(task)) {
                            addTaskToRestoredTasks((StreamTask) task);
                        } else {
                            updatingTasks.put(task.id(), task);
                        }
                        break;

                    case SUSPENDED:
                        task.resume();
                        break;

                    default:
                        throw new IllegalStateException("Illegal state " + state + " while adding to the state updater. "
                            + BUG_ERROR_MESSAGE);
                }
                return null;
            }, task);
        }

        private boolean isStateless(final Task task) {
            return task.changelogPartitions().isEmpty() && task.isActive();
        }

        private void removeTask(final Task task) {
            catchException(() -> {
                final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
                if (!changelogPartitions.isEmpty()) {
                    updatingTasks.remove(task.id());
                    task.stateManager().checkpoint();
                    changelogReader.unregister(changelogPartitions);
                }
                if (task.isActive()) {
                    removeTaskFromRestoredTasks((StreamTask) task);
                }
                task.suspend();
                task.closeClean();
                return null;
            }, task);
        }

        private void catchException(final Supplier<Void> codeToCheck, final Task task) {
            try {
                codeToCheck.get();
            } catch (final RuntimeException exception) {
                exceptions.add(exception);
                updatingTasks.remove(task.id());
            }
        }

        private void handleTaskCorruptedException(final TaskCorruptedException taskCorruptedException) {
            exceptions.add(taskCorruptedException);
            final Set<TaskId> corruptedTaskIds = taskCorruptedException.corruptedTasks();
            for (final TaskId taskId : corruptedTaskIds) {
                updatingTasks.remove(taskId);
            }
        }

        private void handleStreamsException(final StreamsException streamsException) {
            if (streamsException.taskId().isPresent()) {
                final Task task = updatingTasks.get(streamsException.taskId().get());
                task.stateManager().checkpoint();
                exceptions.add(streamsException);
                updatingTasks.remove(task.id());
            } else {
                exceptions.add(streamsException);
            }
        }

        private void addTaskToRestoredTasks(final StreamTask task) {
            restoredActiveTasksLock.lock();
            try {
                restoredActiveTasks.add(task);
                restoredActiveTasksCondition.signalAll();
            } finally {
                restoredActiveTasksLock.unlock();
            }
        }

        private void removeTaskFromRestoredTasks(final StreamTask task) {
            restoredActiveTasksLock.lock();
            try {
                restoredActiveTasks.remove(task);
            } finally {
                restoredActiveTasksLock.unlock();
            }
        }

        public Collection<Task> getAllUpdatingTasks() {
            return updatingTasks.values();
        }

        private void clear() {
            tasksAndActionsLock.lock();
            restoredActiveTasksLock.lock();
            try {
                tasksAndActions.clear();
                restoredActiveTasks.clear();
            } finally {
                tasksAndActionsLock.unlock();
                restoredActiveTasksLock.unlock();
            }
            changelogReader.clear();
            updatingTasks.clear();
        }
    }

    enum Action {
        ADD,
        REMOVE,
        RECYCLE
    }

    private static class TaskAndAction {
        public final Task task;
        public final Action action;

        public TaskAndAction(final Task task, final Action action) {
            this.task = task;
            this.action = action;
        }
    }

    private final Time time;
    private final Queue<TaskAndAction> tasksAndActions = new LinkedList<>();
    private final Lock tasksAndActionsLock = new ReentrantLock();
    private final Condition tasksAndActionsCondition = tasksAndActionsLock.newCondition();
    private final Queue<StreamTask> restoredActiveTasks = new LinkedList<>();
    private final Lock restoredActiveTasksLock = new ReentrantLock();
    private final Condition restoredActiveTasksCondition = restoredActiveTasksLock.newCondition();
    private final BlockingQueue<RuntimeException> exceptions = new LinkedBlockingQueue<>();
    private final ChangelogReader changelogReader;
    private final java.util.function.Consumer<Set<TopicPartition>> offsetResetter;
    private StateUpdaterThread stateUpdaterThread = null;

    public DefaultStateUpdater(final ChangelogReader changelogReader,
                               final java.util.function.Consumer<Set<TopicPartition>> offsetResetter,
                               final Time time) {
        this.changelogReader = changelogReader;
        this.offsetResetter = offsetResetter;
        this.time = time;
    }

    @Override
    public void add(final Task task) {
        if (stateUpdaterThread == null) {
            stateUpdaterThread = new StateUpdaterThread("state-updater", changelogReader, offsetResetter);
            stateUpdaterThread.start();
        }
//        checkStateOfTask()
        // Todo: verify that active tasks are in state RESTORING and standby tasks are in state RUNNING
        tasksAndActionsLock.lock();
        try {
            tasksAndActions.add(new TaskAndAction(task, Action.ADD));
            tasksAndActionsCondition.signalAll();
        } finally {
            tasksAndActionsLock.unlock();
        }
    }


    @Override
    public void remove(final Task task) {
        tasksAndActionsLock.lock();
        try {
            tasksAndActions.add(new TaskAndAction(task, Action.REMOVE));
            tasksAndActionsCondition.signalAll();
        } finally {
            tasksAndActionsLock.unlock();
        }
    }

    @Override
    public List<StreamTask> getRestoredActiveTasks(final Duration timeout) {
        final long timeoutMs = timeout.toMillis();
        final long startTime = time.milliseconds();
        final long deadline = startTime + timeoutMs;
        long now = startTime;
        final List<StreamTask> result = new LinkedList<>();
        try {
            while (now <= deadline && result.isEmpty()) {
                restoredActiveTasksLock.lock();
                try {
                    while (restoredActiveTasks.isEmpty() && now <= deadline) {
                        restoredActiveTasksCondition.await(deadline - now, TimeUnit.MILLISECONDS);
                        now = time.milliseconds();
                    }
                    while (!restoredActiveTasks.isEmpty()) {
                        result.add(restoredActiveTasks.poll());
                    }
                } finally {
                    restoredActiveTasksLock.unlock();
                }
                now = time.milliseconds();
            }
            return result;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public List<RuntimeException> getExceptions() {
        final List<RuntimeException> result = new ArrayList<>();
        exceptions.drainTo(result);
        return result;
    }

    public List<Task> getAllTasks() {
        tasksAndActionsLock.lock();
        restoredActiveTasksLock.lock();
        try {
            final Set<Task> allTasks = new HashSet<>();
            allTasks.addAll(tasksAndActions.stream()
                .filter(t -> t.action == Action.ADD)
                .map(t -> t.task)
                .collect(Collectors.toList())
            );
            allTasks.addAll(stateUpdaterThread.getAllUpdatingTasks());
            allTasks.addAll(restoredActiveTasks);
            return Collections.unmodifiableList(new ArrayList<>(allTasks));
        } finally {
            tasksAndActionsLock.unlock();
            restoredActiveTasksLock.unlock();
        }
    }

    @Override
    public void shutdown(final Duration timeout) {
        if (stateUpdaterThread != null) {
            stateUpdaterThread.isRunning.set(false);
            stateUpdaterThread.interrupt();
            try {
                stateUpdaterThread.join(timeout.toMillis());
                stateUpdaterThread = null;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

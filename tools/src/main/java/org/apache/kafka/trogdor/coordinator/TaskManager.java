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

package org.apache.kafka.trogdor.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.rest.RequestConflictException;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskPending;
import org.apache.kafka.trogdor.rest.TaskRequest;
import org.apache.kafka.trogdor.rest.TaskRunning;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TaskStateType;
import org.apache.kafka.trogdor.rest.TaskStopping;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.rest.WorkerDone;
import org.apache.kafka.trogdor.rest.WorkerReceiving;
import org.apache.kafka.trogdor.rest.WorkerState;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The TaskManager is responsible for managing tasks inside the Trogdor coordinator.
 *
 * The task manager has a single thread, managed by the executor.  We start, stop,
 * and handle state changes to tasks by adding requests to the executor queue.
 * Because the executor is single threaded, no locks are needed when accessing
 * TaskManager data structures.
 *
 * The TaskManager maintains a state machine for each task.  Tasks begin in the
 * PENDING state, waiting for their designated start time to arrive.
 * When their time arrives, they transition to the RUNNING state.  In this state,
 * the NodeManager will start them, and monitor them.
 *
 * The TaskManager does not handle communication with the agents.  This is handled
 * by the NodeManagers.  There is one NodeManager per node being managed.
 * See {org.apache.kafka.trogdor.coordinator.NodeManager} for details.
 */
public final class TaskManager {
    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    /**
     * The platform.
     */
    private final Platform platform;

    /**
     * The scheduler to use for this coordinator.
     */
    private final Scheduler scheduler;

    /**
     * The clock to use for this coordinator.
     */
    private final Time time;

    /**
     * A map of task IDs to Task objects.
     */
    private final Map<String, ManagedTask> tasks;

    /**
     * The executor used for handling Task state changes.
     */
    private final ScheduledExecutorService executor;

    /**
     * Maps node names to node managers.
     */
    private final Map<String, NodeManager> nodeManagers;

    /**
     * The states of all workers.
     */
    private final Map<Long, WorkerState> workerStates = new HashMap<>();

    /**
     * True if the TaskManager is shut down.
     */
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * The ID to use for the next worker.  Only accessed by the state change thread.
     */
    private long nextWorkerId;

    TaskManager(Platform platform, Scheduler scheduler, long firstWorkerId) {
        this.platform = platform;
        this.scheduler = scheduler;
        this.time = scheduler.time();
        this.tasks = new HashMap<>();
        this.executor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("TaskManagerStateThread", false));
        this.nodeManagers = new HashMap<>();
        this.nextWorkerId = firstWorkerId;
        for (Node node : platform.topology().nodes().values()) {
            if (Node.Util.getTrogdorAgentPort(node) > 0) {
                this.nodeManagers.put(node.name(), new NodeManager(node, this));
            }
        }
        log.info("Created TaskManager for agent(s) on: {}",
            Utils.join(nodeManagers.keySet(), ", "));
    }

    class ManagedTask {
        /**
         * The task id.
         */
        final private String id;

        /**
         * The original task specification as submitted when the task was created.
         */
        final private TaskSpec originalSpec;

        /**
         * The effective task specification.
         * The start time will be adjusted to reflect the time when the task was submitted.
         */
        final private TaskSpec spec;

        /**
         * The task controller.
         */
        final private TaskController controller;

        /**
         * The task state.
         */
        private TaskStateType state;

        /**
         * The time when the task was started, or -1 if the task has not been started.
         */
        private long startedMs = -1;

        /**
         * The time when the task was finished, or -1 if the task has not been finished.
         */
        private long doneMs = -1;

        /**
         * True if the task was cancelled by a stop request.
         */
        boolean cancelled = false;

        /**
         * If there is a task start scheduled, this is a future which can
         * be used to cancel it.
         */
        private Future<?> startFuture = null;

        /**
         * Maps node names to worker IDs.
         */
        public TreeMap<String, Long> workerIds = new TreeMap<>();

        /**
         * If this is non-empty, a message describing how this task failed.
         */
        private String error = "";

        ManagedTask(String id, TaskSpec originalSpec, TaskSpec spec,
                    TaskController controller, TaskStateType state) {
            this.id = id;
            this.originalSpec = originalSpec;
            this.spec = spec;
            this.controller = controller;
            this.state = state;
        }

        void clearStartFuture() {
            if (startFuture != null) {
                startFuture.cancel(false);
                startFuture = null;
            }
        }

        long startDelayMs(long now) {
            if (now > spec.startMs()) {
                return 0;
            }
            return spec.startMs() - now;
        }

        TreeSet<String> findNodeNames() {
            Set<String> nodeNames = controller.targetNodes(platform.topology());
            TreeSet<String> validNodeNames = new TreeSet<>();
            TreeSet<String> nonExistentNodeNames = new TreeSet<>();
            for (String nodeName : nodeNames) {
                if (nodeManagers.containsKey(nodeName)) {
                    validNodeNames.add(nodeName);
                } else {
                    nonExistentNodeNames.add(nodeName);
                }
            }
            if (!nonExistentNodeNames.isEmpty()) {
                throw new KafkaException("Unknown node names: " +
                        Utils.join(nonExistentNodeNames, ", "));
            }
            if (validNodeNames.isEmpty()) {
                throw new KafkaException("No node names specified.");
            }
            return validNodeNames;
        }

        void maybeSetError(String newError) {
            if (error.isEmpty()) {
                error = newError;
            }
        }

        TaskState taskState() {
            switch (state) {
                case PENDING:
                    return new TaskPending(spec);
                case RUNNING:
                    return new TaskRunning(spec, startedMs, getCombinedStatus());
                case STOPPING:
                    return new TaskStopping(spec, startedMs, getCombinedStatus());
                case DONE:
                    return new TaskDone(spec, startedMs, doneMs, error, cancelled, getCombinedStatus());
            }
            throw new RuntimeException("unreachable");
        }

        private JsonNode getCombinedStatus() {
            if (workerIds.size() == 1) {
                return workerStates.get(workerIds.values().iterator().next()).status();
            } else {
                ObjectNode objectNode = new ObjectNode(JsonNodeFactory.instance);
                for (Map.Entry<String, Long> entry : workerIds.entrySet()) {
                    String nodeName = entry.getKey();
                    Long workerId = entry.getValue();
                    WorkerState state = workerStates.get(workerId);
                    JsonNode node = state.status();
                    if (node != null) {
                        objectNode.set(nodeName, node);
                    }
                }
                return objectNode;
            }
        }

        TreeMap<String, Long> activeWorkerIds() {
            TreeMap<String, Long> activeWorkerIds = new TreeMap<>();
            for (Map.Entry<String, Long> entry : workerIds.entrySet()) {
                WorkerState workerState = workerStates.get(entry.getValue());
                if (!workerState.done()) {
                    activeWorkerIds.put(entry.getKey(), entry.getValue());
                }
            }
            return activeWorkerIds;
        }
    }

    /**
     * Create a task.
     *
     * @param id                    The ID of the task to create.
     * @param spec                  The specification of the task to create.
     * @throws RequestConflictException - if a task with the same ID but different spec exists
     */
    public void createTask(final String id, TaskSpec spec)
            throws Throwable {
        try {
            executor.submit(new CreateTask(id, spec)).get();
        } catch (ExecutionException | JsonProcessingException e) {
            log.info("createTask(id={}, spec={}) error", id, spec, e);
            throw e.getCause();
        }
    }

    /**
     * Handles a request to create a new task.  Processed by the state change thread.
     */
    class CreateTask implements Callable<Void> {
        private final String id;
        private final TaskSpec originalSpec;
        private final TaskSpec spec;

        CreateTask(String id, TaskSpec spec) throws JsonProcessingException {
            this.id = id;
            this.originalSpec = spec;
            ObjectNode node = JsonUtil.JSON_SERDE.valueToTree(originalSpec);
            node.set("startMs", new LongNode(Math.max(time.milliseconds(), originalSpec.startMs())));
            this.spec = JsonUtil.JSON_SERDE.treeToValue(node, TaskSpec.class);
        }

        @Override
        public Void call() throws Exception {
            if (id.isEmpty()) {
                throw new InvalidRequestException("Invalid empty ID in createTask request.");
            }
            ManagedTask task = tasks.get(id);
            if (task != null) {
                if (!task.originalSpec.equals(originalSpec)) {
                    throw new RequestConflictException("Task ID " + id + " already " +
                        "exists, and has a different spec " + task.originalSpec);
                }
                log.info("Task {} already exists with spec {}", id, originalSpec);
                return null;
            }
            TaskController controller = null;
            String failure = null;
            try {
                controller = spec.newController(id);
            } catch (Throwable t) {
                failure = "Failed to create TaskController: " + t.getMessage();
            }
            if (failure != null) {
                log.info("Failed to create a new task {} with spec {}: {}",
                    id, spec, failure);
                task = new ManagedTask(id, originalSpec, spec, null, TaskStateType.DONE);
                task.doneMs = time.milliseconds();
                task.maybeSetError(failure);
                tasks.put(id, task);
                return null;
            }
            task = new ManagedTask(id, originalSpec, spec, controller, TaskStateType.PENDING);
            tasks.put(id, task);
            long delayMs = task.startDelayMs(time.milliseconds());
            task.startFuture = scheduler.schedule(executor, new RunTask(task), delayMs);
            log.info("Created a new task {} with spec {}, scheduled to start {} ms from now.",
                    id, spec, delayMs);
            return null;
        }
    }

    /**
     * Handles starting a task.  Processed by the state change thread.
     */
    class RunTask implements Callable<Void> {
        private final ManagedTask task;

        RunTask(ManagedTask task) {
            this.task = task;
        }

        @Override
        public Void call() throws Exception {
            task.clearStartFuture();
            if (task.state != TaskStateType.PENDING) {
                log.info("Can't start task {}, because it is already in state {}.",
                    task.id, task.state);
                return null;
            }
            TreeSet<String> nodeNames;
            try {
                nodeNames = task.findNodeNames();
            } catch (Exception e) {
                log.error("Unable to find nodes for task {}", task.id, e);
                task.doneMs = time.milliseconds();
                task.state = TaskStateType.DONE;
                task.maybeSetError("Unable to find nodes for task: " + e.getMessage());
                return null;
            }
            log.info("Running task {} on node(s): {}", task.id, Utils.join(nodeNames, ", "));
            task.state = TaskStateType.RUNNING;
            task.startedMs = time.milliseconds();
            for (String workerName : nodeNames) {
                long workerId = nextWorkerId++;
                task.workerIds.put(workerName, workerId);
                workerStates.put(workerId, new WorkerReceiving(task.id, task.spec));
                nodeManagers.get(workerName).createWorker(workerId, task.id, task.spec);
            }
            return null;
        }
    }

    /**
     * Stop a task.
     *
     * @param id                    The ID of the task to stop.
     */
    public void stopTask(final String id) throws Throwable {
        try {
            executor.submit(new CancelTask(id)).get();
        } catch (ExecutionException e) {
            log.info("stopTask(id={}) error", id, e);
            throw e.getCause();
        }
    }

    /**
     * Handles cancelling a task.  Processed by the state change thread.
     */
    class CancelTask implements Callable<Void> {
        private final String id;

        CancelTask(String id) {
            this.id = id;
        }

        @Override
        public Void call() throws Exception {
            if (id.isEmpty()) {
                throw new InvalidRequestException("Invalid empty ID in stopTask request.");
            }
            ManagedTask task = tasks.get(id);
            if (task == null) {
                log.info("Can't cancel non-existent task {}.", id);
                return null;
            }
            switch (task.state) {
                case PENDING:
                    task.cancelled = true;
                    task.clearStartFuture();
                    task.doneMs = time.milliseconds();
                    task.state = TaskStateType.DONE;
                    log.info("Stopped pending task {}.", id);
                    break;
                case RUNNING:
                    task.cancelled = true;
                    TreeMap<String, Long> activeWorkerIds = task.activeWorkerIds();
                    if (activeWorkerIds.isEmpty()) {
                        if (task.error.isEmpty()) {
                            log.info("Task {} is now complete with no errors.", id);
                        } else {
                            log.info("Task {} is now complete with error: {}", id, task.error);
                        }
                        task.doneMs = time.milliseconds();
                        task.state = TaskStateType.DONE;
                    } else {
                        for (Map.Entry<String, Long> entry : activeWorkerIds.entrySet()) {
                            nodeManagers.get(entry.getKey()).stopWorker(entry.getValue());
                        }
                        log.info("Cancelling task {} with worker(s) {}",
                            id, Utils.mkString(activeWorkerIds, "", "", " = ", ", "));
                        task.state = TaskStateType.STOPPING;
                    }
                    break;
                case STOPPING:
                    log.info("Can't cancel task {} because it is already stopping.", id);
                    break;
                case DONE:
                    log.info("Can't cancel task {} because it is already done.", id);
                    break;
            }
            return null;
        }
    }

    public void destroyTask(String id) throws Throwable {
        try {
            executor.submit(new DestroyTask(id)).get();
        } catch (ExecutionException e) {
            log.info("destroyTask(id={}) error", id, e);
            throw e.getCause();
        }
    }

    /**
     * Handles destroying a task.  Processed by the state change thread.
     */
    class DestroyTask implements Callable<Void> {
        private final String id;

        DestroyTask(String id) {
            this.id = id;
        }

        @Override
        public Void call() throws Exception {
            if (id.isEmpty()) {
                throw new InvalidRequestException("Invalid empty ID in destroyTask request.");
            }
            ManagedTask task = tasks.remove(id);
            if (task == null) {
                log.info("Can't destroy task {}: no such task found.", id);
                return null;
            }
            log.info("Destroying task {}.", id);
            task.clearStartFuture();
            for (Map.Entry<String, Long> entry : task.workerIds.entrySet()) {
                long workerId = entry.getValue();
                workerStates.remove(workerId);
                String nodeName = entry.getKey();
                nodeManagers.get(nodeName).destroyWorker(workerId);
            }
            return null;
        }
    }

    /**
     * Update the state of a particular agent's worker.
     *
     * @param nodeName      The node where the agent is running.
     * @param workerId      The worker ID.
     * @param state         The worker state.
     */
    public void updateWorkerState(String nodeName, long workerId, WorkerState state) {
        executor.submit(new UpdateWorkerState(nodeName, workerId, state));
    }

    /**
     * Updates the state of a worker.  Process by the state change thread.
     */
    class UpdateWorkerState implements Callable<Void> {
        private final String nodeName;
        private final long workerId;
        private final WorkerState nextState;

        UpdateWorkerState(String nodeName, long workerId, WorkerState nextState) {
            this.nodeName = nodeName;
            this.workerId = workerId;
            this.nextState = nextState;
        }

        @Override
        public Void call() throws Exception {
            try {
                WorkerState prevState = workerStates.get(workerId);
                if (prevState == null) {
                    throw new RuntimeException("Unable to find workerId " + workerId);
                }
                ManagedTask task = tasks.get(prevState.taskId());
                if (task == null) {
                    throw new RuntimeException("Unable to find taskId " + prevState.taskId());
                }
                log.debug("Task {}: Updating worker state for {} on {} from {} to {}.",
                    task.id, workerId, nodeName, prevState, nextState);
                workerStates.put(workerId, nextState);
                if (nextState.done() && (!prevState.done())) {
                    handleWorkerCompletion(task, nodeName, (WorkerDone) nextState);
                }
            } catch (Exception e) {
                log.error("Error updating worker state for {} on {}.  Stopping worker.",
                    workerId, nodeName, e);
                nodeManagers.get(nodeName).stopWorker(workerId);
            }
            return null;
        }
    }

    /**
     * Handle a worker being completed.
     *
     * @param task      The task that owns the worker.
     * @param nodeName  The name of the node on which the worker is running.
     * @param state     The worker state.
     */
    private void handleWorkerCompletion(ManagedTask task, String nodeName, WorkerDone state) {
        if (state.error().isEmpty()) {
            log.info("{}: Worker {} finished with status '{}'",
                nodeName, task.id, JsonUtil.toJsonString(state.status()));
        } else {
            log.warn("{}: Worker {} finished with error '{}' and status '{}'",
                nodeName, task.id, state.error(), JsonUtil.toJsonString(state.status()));
            task.maybeSetError(state.error());
        }
        TreeMap<String, Long> activeWorkerIds = task.activeWorkerIds();
        if (activeWorkerIds.isEmpty()) {
            task.doneMs = time.milliseconds();
            task.state = TaskStateType.DONE;
            log.info("{}: Task {} is now complete on {} with error: {}",
                nodeName, task.id, Utils.join(task.workerIds.keySet(), ", "),
                task.error.isEmpty() ? "(none)" : task.error);
        } else if ((task.state == TaskStateType.RUNNING) && (!task.error.isEmpty())) {
            log.info("{}: task {} stopped with error {}.  Stopping worker(s): {}",
                nodeName, task.id, task.error, Utils.mkString(activeWorkerIds, "{", "}", ": ", ", "));
            task.state = TaskStateType.STOPPING;
            for (Map.Entry<String, Long> entry : activeWorkerIds.entrySet()) {
                nodeManagers.get(entry.getKey()).stopWorker(entry.getValue());
            }
        }
    }

    /**
     * Get information about the tasks being managed.
     */
    public TasksResponse tasks(TasksRequest request) throws ExecutionException, InterruptedException {
        return executor.submit(new GetTasksResponse(request)).get();
    }

    /**
     * Gets information about the tasks being managed.  Processed by the state change thread.
     */
    class GetTasksResponse implements Callable<TasksResponse> {
        private final TasksRequest request;

        GetTasksResponse(TasksRequest request) {
            this.request = request;
        }

        @Override
        public TasksResponse call() throws Exception {
            TreeMap<String, TaskState> states = new TreeMap<>();
            for (ManagedTask task : tasks.values()) {
                if (request.matches(task.id, task.startedMs, task.doneMs, task.state)) {
                    states.put(task.id, task.taskState());
                }
            }
            return new TasksResponse(states);
        }
    }

    /**
     * Get information about a single task being managed.
     *
     * Returns #{@code null} if the task does not exist
     */
    public TaskState task(TaskRequest request) throws ExecutionException, InterruptedException {
        return executor.submit(new GetTaskState(request)).get();
    }

    /**
     * Gets information about the tasks being managed.  Processed by the state change thread.
     */
    class GetTaskState implements Callable<TaskState> {
        private final TaskRequest request;

        GetTaskState(TaskRequest request) {
            this.request = request;
        }

        @Override
        public TaskState call() throws Exception {
            ManagedTask task = tasks.get(request.taskId());
            if (task == null) {
                return null;
            }

            return task.taskState();
        }
    }

    /**
     * Initiate shutdown, but do not wait for it to complete.
     */
    public void beginShutdown(boolean stopAgents) throws ExecutionException, InterruptedException {
        if (shutdown.compareAndSet(false, true)) {
            executor.submit(new Shutdown(stopAgents));
        }
    }

    /**
     * Wait for shutdown to complete.  May be called prior to beginShutdown.
     */
    public void waitForShutdown() throws ExecutionException, InterruptedException {
        while (!executor.awaitTermination(1, TimeUnit.DAYS)) { }
    }

    class Shutdown implements Callable<Void> {
        private final boolean stopAgents;

        Shutdown(boolean stopAgents) {
            this.stopAgents = stopAgents;
        }

        @Override
        public Void call() throws Exception {
            log.info("Shutting down TaskManager{}.", stopAgents ? " and agents" : "");
            for (NodeManager nodeManager : nodeManagers.values()) {
                nodeManager.beginShutdown(stopAgents);
            }
            for (NodeManager nodeManager : nodeManagers.values()) {
                nodeManager.waitForShutdown();
            }
            executor.shutdown();
            return null;
        }
    }
};

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

/*
 * So, when a task comes in, it happens via createTask (the RPC backend).
 * This starts a CreateTask on the main state change thread, and waits for it.
 * That task checks the main task hash map, and returns back the existing task spec
 * if there is something there.  If there is nothing there, it creates
 * something new, and returns null.
 * It also schedules a RunTask some time in the future on the main state change thread.
 * We save the future from this in case we need to cancel it later, in a StopTask.
 * If we can't create the TaskController for the task, we transition to DONE with an
 * appropriate error message.
 *
 * RunTask actually starts the task which was created earlier.  This could
 * happen an arbitrary amount of time after task creation (it is based on the
 * task spec).  RunTask must operate only on PENDING tasks... if the task has been
 * stopped, then we have nothing to do here.
 * RunTask asks the TaskController for a list of all the names of nodes
 * affected by this task.
 * If this list contains nodes we don't know about, or zero nodes, we
 * transition directly to DONE state with an appropriate error set.
 * RunTask schedules CreateWorker Callables on all the affected worker nodes.
 * These callables run in the context of the relevant NodeManager.
 *
 * CreateWorker calls the RPC of the same name for the agent.
 * There is some complexity here due to retries.
 */

package org.apache.kafka.trogdor.coordinator;

import org.apache.kafka.trogdor.agent.AgentClient;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.apache.kafka.trogdor.rest.WorkerReceiving;
import org.apache.kafka.trogdor.rest.WorkerRunning;
import org.apache.kafka.trogdor.rest.WorkerStarting;
import org.apache.kafka.trogdor.rest.WorkerState;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The NodeManager handles communicating with a specific agent node.
 * Each NodeManager has its own ExecutorService which runs in a dedicated thread.
 */
public final class NodeManager {
    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);

    /**
     * The normal amount of seconds between heartbeats sent to the agent.
     */
    private static final long HEARTBEAT_DELAY_MS = 1000L;

    class ManagedWorker {
        private final String id;
        private final TaskSpec spec;
        private boolean shouldRun;
        private WorkerState state;

        ManagedWorker(String id, TaskSpec spec, boolean shouldRun, WorkerState state) {
            this.id = id;
            this.spec = spec;
            this.shouldRun = shouldRun;
            this.state = state;
        }

        void tryCreate() {
            try {
                client.createWorker(new CreateWorkerRequest(id, spec));
            } catch (Throwable e) {
                log.error("{}: error creating worker {}.", node.name(), id, e);
            }
        }

        void tryStop() {
            try {
                client.stopWorker(new StopWorkerRequest(id));
            } catch (Throwable e) {
                log.error("{}: error stopping worker {}.", node.name(), id, e);
            }
        }
    }

    /**
     * The node which we are managing.
     */
    private final Node node;

    /**
     * The task manager.
     */
    private final TaskManager taskManager;

    /**
     * A client for the Node's Agent.
     */
    private final AgentClient client;

    /**
     * Maps task IDs to worker structures.
     */
    private final Map<String, ManagedWorker> workers;

    /**
     * An executor service which manages the thread dedicated to this node.
     */
    private final ScheduledExecutorService executor;

    /**
     * The heartbeat runnable.
     */
    private final NodeHeartbeat heartbeat;

    /**
     * A future which can be used to cancel the periodic hearbeat task.
     */
    private ScheduledFuture<?> heartbeatFuture;

    NodeManager(Node node, TaskManager taskManager) {
        this.node = node;
        this.taskManager = taskManager;
        this.client = new AgentClient.Builder().
            maxTries(1).
            target(node.hostname(), Node.Util.getTrogdorAgentPort(node)).
            build();
        this.workers = new HashMap<>();
        this.executor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("NodeManager(" + node.name() + ")",
                false));
        this.heartbeat = new NodeHeartbeat();
        rescheduleNextHeartbeat(HEARTBEAT_DELAY_MS);
    }

    /**
     * Reschedule the heartbeat runnable.
     *
     * @param initialDelayMs        The initial delay to use.
     */
    void rescheduleNextHeartbeat(long initialDelayMs) {
        if (this.heartbeatFuture != null) {
            this.heartbeatFuture.cancel(false);
        }
        this.heartbeatFuture = this.executor.scheduleAtFixedRate(heartbeat,
            initialDelayMs, HEARTBEAT_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * The heartbeat runnable.
     */
    class NodeHeartbeat implements Runnable {
        @Override
        public void run() {
            rescheduleNextHeartbeat(HEARTBEAT_DELAY_MS);
            try {
                AgentStatusResponse agentStatus = null;
                try {
                    agentStatus = client.status();
                } catch (ConnectException e) {
                    log.error("{}: failed to get agent status: ConnectException {}", node.name(), e.getMessage());
                    return;
                } catch (Exception e) {
                    log.error("{}: failed to get agent status", node.name(), e);
                    // TODO: eventually think about putting tasks into a bad state as a result of
                    // agents going down?
                    return;
                }
                if (log.isTraceEnabled()) {
                    log.trace("{}: got heartbeat status {}", node.name(), agentStatus);
                }
                // Identify workers which we think should be running, but which do not appear
                // in the agent's response.  We need to send startWorker requests for these.
                for (Map.Entry<String, ManagedWorker> entry : workers.entrySet()) {
                    String id = entry.getKey();
                    if (!agentStatus.workers().containsKey(id)) {
                        ManagedWorker worker = entry.getValue();
                        if (worker.shouldRun) {
                            worker.tryCreate();
                        }
                    }
                }
                for (Map.Entry<String, WorkerState> entry : agentStatus.workers().entrySet()) {
                    String id = entry.getKey();
                    WorkerState state = entry.getValue();
                    ManagedWorker worker = workers.get(id);
                    if (worker == null) {
                        // Identify tasks which are running, but which we don't know about.
                        // Add these to the NodeManager as tasks that should not be running.
                        log.warn("{}: scheduling unknown worker {} for stopping.", node.name(), id);
                        workers.put(id, new ManagedWorker(id, state.spec(), false, state));
                    } else {
                        // Handle workers which need to be stopped.
                        if (state instanceof WorkerStarting || state instanceof WorkerRunning) {
                            if (!worker.shouldRun) {
                                worker.tryStop();
                            }
                        }
                        // Notify the TaskManager if the worker state has changed.
                        if (worker.state.equals(state)) {
                            log.debug("{}: worker state is still {}", node.name(), worker.state);
                        } else {
                            log.info("{}: worker state changed from {} to {}", node.name(), worker.state, state);
                            worker.state = state;
                            taskManager.updateWorkerState(node.name(), worker.id, state);
                        }
                    }
                }
            } catch (Throwable e) {
                log.error("{}: Unhandled exception in NodeHeartbeatRunnable", node.name(), e);
            }
        }
    }

    /**
     * Create a new worker.
     *
     * @param id                    The new worker id.
     * @param spec                  The task specification to use with the new worker.
     */
    public void createWorker(String id, TaskSpec spec) {
        executor.submit(new CreateWorker(id, spec));
    }

    /**
     * Starts a worker.
     */
    class CreateWorker implements Callable<Void> {
        private final String id;
        private final TaskSpec spec;

        CreateWorker(String id, TaskSpec spec) {
            this.id = id;
            this.spec = spec;
        }

        @Override
        public Void call() throws Exception {
            ManagedWorker worker = workers.get(id);
            if (worker != null) {
                log.error("{}: there is already a worker for task {}.", node.name(), id);
                return null;
            }
            log.info("{}: scheduling worker {} to start.", node.name(), id);
            workers.put(id, new ManagedWorker(id, spec, true, new WorkerReceiving(spec)));
            rescheduleNextHeartbeat(0);
            return null;
        }
    }

    /**
     * Stop a worker.
     *
     * @param id                    The id of the worker to stop.
     */
    public void stopWorker(String id) {
        executor.submit(new StopWorker(id));
    }

    /**
     * Stops a worker.
     */
    class StopWorker implements Callable<Void> {
        private final String id;

        StopWorker(String id) {
            this.id = id;
        }

        @Override
        public Void call() throws Exception {
            ManagedWorker worker = workers.get(id);
            if (worker == null) {
                log.error("{}: can't stop non-existent worker {}.", node.name(), id);
                return null;
            }
            if (!worker.shouldRun) {
                log.error("{}: The worker for task {} is already scheduled to stop.",
                    node.name(), id);
                return null;
            }
            log.info("{}: scheduling worker {} on {} to stop.", node.name(), id);
            worker.shouldRun = false;
            rescheduleNextHeartbeat(0);
            return null;
        }
    }

    public void beginShutdown(boolean stopNode) {
        executor.shutdownNow();
        if (stopNode) {
            try {
                client.invokeShutdown();
            } catch (Exception e) {
                log.error("{}: Failed to send shutdown request", node.name(), e);
            }
        }
    }

    public void waitForShutdown() throws InterruptedException {
        executor.awaitTermination(1, TimeUnit.DAYS);
    }
};

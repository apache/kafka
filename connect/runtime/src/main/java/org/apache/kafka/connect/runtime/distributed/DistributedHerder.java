/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.HerderConnectorContext;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.SinkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 *     Distributed "herder" that coordinates with other workers to spread work across multiple processes.
 * </p>
 * <p>
 *     Under the hood, this is implemented as a group managed by Kafka's group membership facilities (i.e. the generalized
 *     group/consumer coordinator). Each instance of DistributedHerder joins the group and indicates what it's current
 *     configuration state is (where it is in the configuration log). The group coordinator selects one member to take
 *     this information and assign each instance a subset of the active connectors & tasks to execute. This assignment
 *     is currently performed in a simple round-robin fashion, but this is not guaranteed -- the herder may also choose
 *     to, e.g., use a sticky assignment to avoid the usual start/stop costs associated with connectors and tasks. Once
 *     an assignment is received, the DistributedHerder simply runs its assigned connectors and tasks in a Worker.
 * </p>
 * <p>
 *     In addition to distributing work, the DistributedHerder uses the leader determined during the work assignment
 *     to select a leader for this generation of the group who is responsible for other tasks that can only be performed
 *     by a single node at a time. Most importantly, this includes writing updated configurations for connectors and tasks,
 *     (and therefore, also for creating, destroy, and scaling up/down connectors).
 * </p>
 * <p>
 *     The DistributedHerder uses a single thread for most of its processing. This includes processing
 *     config changes, handling task rebalances and serving requests from the HTTP layer. The latter are pushed
 *     into a queue until the thread has time to handle them. A consequence of this is that requests can get blocked
 *     behind a worker rebalance. When the herder knows that a rebalance is expected, it typically returns an error
 *     immediately to the request, but this is not always possible (in particular when another worker has requested
 *     the rebalance). Similar to handling HTTP requests, config changes which are observed asynchronously by polling
 *     the config log are batched for handling in the work thread.
 * </p>
 */
public class DistributedHerder extends AbstractHerder implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(DistributedHerder.class);

    private static final long RECONFIGURE_CONNECTOR_TASKS_BACKOFF_MS = 250;
    private static final int START_STOP_THREAD_POOL_SIZE = 8;

    private final AtomicLong requestSeqNum = new AtomicLong();

    private final Time time;

    private final String workerGroupId;
    private final int workerSyncTimeoutMs;
    private final long workerTasksShutdownTimeoutMs;
    private final int workerUnsyncBackoffMs;

    private final ExecutorService herderExecutor;
    private final ExecutorService forwardRequestExecutor;
    private final ExecutorService startAndStopExecutor;
    private final WorkerGroupMember member;
    private final AtomicBoolean stopping;

    // Track enough information about the current membership state to be able to determine which requests via the API
    // and the from other nodes are safe to process
    private boolean rebalanceResolved;
    private ConnectProtocol.Assignment assignment;
    private boolean canReadConfigs;
    private ClusterConfigState configState;

    // To handle most external requests, like creating or destroying a connector, we can use a generic request where
    // the caller specifies all the code that should be executed.
    final NavigableSet<HerderRequest> requests = new ConcurrentSkipListSet<>();
    // Config updates can be collected and applied together when possible. Also, we need to take care to rebalance when
    // needed (e.g. task reconfiguration, which requires everyone to coordinate offset commits).
    private Set<String> connectorConfigUpdates = new HashSet<>();
    // Similarly collect target state changes (when observed by the config storage listener) for handling in the
    // herder's main thread.
    private Set<String> connectorTargetStateChanges = new HashSet<>();
    private boolean needsReconfigRebalance;
    private volatile int generation;

    public DistributedHerder(DistributedConfig config,
                             Time time,
                             Worker worker,
                             StatusBackingStore statusBackingStore,
                             ConfigBackingStore configBackingStore,
                             String restUrl) {
        this(config, worker, worker.workerId(), statusBackingStore, configBackingStore, null, restUrl, time);
        configBackingStore.setUpdateListener(new ConfigUpdateListener());
    }

    // visible for testing
    DistributedHerder(DistributedConfig config,
                      Worker worker,
                      String workerId,
                      StatusBackingStore statusBackingStore,
                      ConfigBackingStore configBackingStore,
                      WorkerGroupMember member,
                      String restUrl,
                      Time time) {
        super(worker, workerId, statusBackingStore, configBackingStore);

        this.time = time;
        this.workerGroupId = config.getString(DistributedConfig.GROUP_ID_CONFIG);
        this.workerSyncTimeoutMs = config.getInt(DistributedConfig.WORKER_SYNC_TIMEOUT_MS_CONFIG);
        this.workerTasksShutdownTimeoutMs = config.getLong(DistributedConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG);
        this.workerUnsyncBackoffMs = config.getInt(DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG);
        this.member = member != null ? member : new WorkerGroupMember(config, restUrl, this.configBackingStore, new RebalanceListener(), time);
        this.herderExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(1),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable herder) {
                        return new Thread(herder, "DistributedHerder");
                    }
                });
        this.forwardRequestExecutor = Executors.newSingleThreadExecutor();
        this.startAndStopExecutor = Executors.newFixedThreadPool(START_STOP_THREAD_POOL_SIZE);

        stopping = new AtomicBoolean(false);
        configState = ClusterConfigState.EMPTY;
        rebalanceResolved = true; // If we still need to follow up after a rebalance occurred, starting up tasks
        needsReconfigRebalance = false;
        canReadConfigs = true; // We didn't try yet, but Configs are readable until proven otherwise
    }

    @Override
    public void start() {
        this.herderExecutor.submit(this);
    }

    @Override
    public void run() {
        try {
            log.info("Herder starting");

            startServices();

            log.info("Herder started");

            while (!stopping.get()) {
                tick();
            }

            halt();

            log.info("Herder stopped");
        } catch (Throwable t) {
            log.error("Uncaught exception in herder work thread, exiting: ", t);
            Exit.exit(1);
        }
    }

    // public for testing
    public void tick() {
        // The main loop does two primary things: 1) drive the group membership protocol, responding to rebalance events
        // as they occur, and 2) handle external requests targeted at the leader. All the "real" work of the herder is
        // performed in this thread, which keeps synchronization straightforward at the cost of some operations possibly
        // blocking up this thread (especially those in callbacks due to rebalance events).

        try {
            // if we failed to read to end of log before, we need to make sure the issue was resolved before joining group
            // Joining and immediately leaving for failure to read configs is exceedingly impolite
            if (!canReadConfigs && !readConfigToEnd(workerSyncTimeoutMs))
                return; // Safe to return and tick immediately because readConfigToEnd will do the backoff for us

            member.ensureActive();
            // Ensure we're in a good state in our group. If not restart and everything should be setup to rejoin
            if (!handleRebalanceCompleted()) return;
        } catch (WakeupException e) {
            // May be due to a request from another thread, or might be stopping. If the latter, we need to check the
            // flag immediately. If the former, we need to re-run the ensureActive call since we can't handle requests
            // unless we're in the group.
            return;
        }

        // Process any external requests
        final long now = time.milliseconds();
        long nextRequestTimeoutMs = Long.MAX_VALUE;
        while (true) {
            final HerderRequest next = peekWithoutException();
            if (next == null) {
                break;
            } else if (now >= next.at) {
                requests.pollFirst();
            } else {
                nextRequestTimeoutMs = next.at - now;
                break;
            }

            try {
                next.action().call();
                next.callback().onCompletion(null, null);
            } catch (Throwable t) {
                next.callback().onCompletion(t, null);
            }
        }

        // Process any configuration updates
        Set<String> connectorConfigUpdatesCopy = null;
        Set<String> connectorTargetStateChangesCopy = null;
        synchronized (this) {
            if (needsReconfigRebalance || !connectorConfigUpdates.isEmpty() || !connectorTargetStateChanges.isEmpty()) {
                // Connector reconfigs only need local updates since there is no coordination between workers required.
                // However, if connectors were added or removed, work needs to be rebalanced since we have more work
                // items to distribute among workers.
                configState = configBackingStore.snapshot();

                if (needsReconfigRebalance) {
                    // Task reconfigs require a rebalance. Request the rebalance, clean out state, and then restart
                    // this loop, which will then ensure the rebalance occurs without any other requests being
                    // processed until it completes.
                    member.requestRejoin();
                    // Any connector config updates or target state changes will be addressed during the rebalance too
                    connectorConfigUpdates.clear();
                    connectorTargetStateChanges.clear();
                    needsReconfigRebalance = false;
                    return;
                } else {
                    if (!connectorConfigUpdates.isEmpty()) {
                        // We can't start/stop while locked since starting connectors can cause task updates that will
                        // require writing configs, which in turn make callbacks into this class from another thread that
                        // require acquiring a lock. This leads to deadlock. Instead, just copy the info we need and process
                        // the updates after unlocking.
                        connectorConfigUpdatesCopy = connectorConfigUpdates;
                        connectorConfigUpdates = new HashSet<>();
                    }

                    if (!connectorTargetStateChanges.isEmpty()) {
                        // Similarly for target state changes which can cause connectors to be restarted
                        connectorTargetStateChangesCopy = connectorTargetStateChanges;
                        connectorTargetStateChanges = new HashSet<>();
                    }
                }
            }
        }

        if (connectorConfigUpdatesCopy != null)
            processConnectorConfigUpdates(connectorConfigUpdatesCopy);

        if (connectorTargetStateChangesCopy != null)
            processTargetStateChanges(connectorTargetStateChangesCopy);

        // Let the group take any actions it needs to
        try {
            member.poll(nextRequestTimeoutMs);
            // Ensure we're in a good state in our group. If not restart and everything should be setup to rejoin
            handleRebalanceCompleted();
        } catch (WakeupException e) { // FIXME should not be WakeupException
            // Ignore. Just indicates we need to check the exit flag, for requested actions, etc.
        }
    }

    private void processConnectorConfigUpdates(Set<String> connectorConfigUpdates) {
        // If we only have connector config updates, we can just bounce the updated connectors that are
        // currently assigned to this worker.
        Set<String> localConnectors = assignment == null ? Collections.<String>emptySet() : new HashSet<>(assignment.connectors());
        for (String connectorName : connectorConfigUpdates) {
            if (!localConnectors.contains(connectorName))
                continue;
            boolean remains = configState.contains(connectorName);
            log.info("Handling connector-only config update by {} connector {}",
                    remains ? "restarting" : "stopping", connectorName);
            worker.stopConnector(connectorName);
            // The update may be a deletion, so verify we actually need to restart the connector
            if (remains)
                startConnector(connectorName);
        }
    }

    private void processTargetStateChanges(Set<String> connectorTargetStateChanges) {
        for (String connector : connectorTargetStateChanges) {
            TargetState targetState = configState.targetState(connector);
            if (!configState.connectors().contains(connector)) {
                log.debug("Received target state change for unknown connector: {}", connector);
                continue;
            }

            // we must propagate the state change to the worker so that the connector's
            // tasks can transition to the new target state
            worker.setTargetState(connector, targetState);

            // additionally, if the worker is running the connector itself, then we need to
            // request reconfiguration to ensure that config changes while paused take effect
            if (targetState == TargetState.STARTED)
                reconfigureConnectorTasksWithRetry(connector);
        }
    }

    // public for testing
    public void halt() {
        synchronized (this) {
            // Clean up any connectors and tasks that are still running.
            log.info("Stopping connectors and tasks that are still assigned to this worker.");
            List<Callable<Void>> callables = new ArrayList<>();
            for (String connectorName : new ArrayList<>(worker.connectorNames())) {
                callables.add(getConnectorStoppingCallable(connectorName));
            }
            for (ConnectorTaskId taskId : new ArrayList<>(worker.taskIds())) {
                callables.add(getTaskStoppingCallable(taskId));
            }
            startAndStop(callables);

            member.stop();

            // Explicitly fail any outstanding requests so they actually get a response and get an
            // understandable reason for their failure.
            HerderRequest request = requests.pollFirst();
            while (request != null) {
                request.callback().onCompletion(new ConnectException("Worker is shutting down"), null);
                request = requests.pollFirst();
            }

            stopServices();
        }
    }

    @Override
    public void stop() {
        log.info("Herder stopping");

        stopping.set(true);
        member.wakeup();
        herderExecutor.shutdown();
        try {
            if (!herderExecutor.awaitTermination(workerTasksShutdownTimeoutMs, TimeUnit.MILLISECONDS))
                herderExecutor.shutdownNow();

            forwardRequestExecutor.shutdown();
            startAndStopExecutor.shutdown();

            if (!forwardRequestExecutor.awaitTermination(10000L, TimeUnit.MILLISECONDS))
                forwardRequestExecutor.shutdownNow();
            if (!startAndStopExecutor.awaitTermination(1000L, TimeUnit.MILLISECONDS))
                startAndStopExecutor.shutdownNow();
        } catch (InterruptedException e) {
            // ignore
        }

        log.info("Herder stopped");
    }

    @Override
    public void connectors(final Callback<Collection<String>> callback) {
        log.trace("Submitting connector listing request");

        addRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (checkRebalanceNeeded(callback))
                            return null;

                        callback.onCompletion(null, configState.connectors());
                        return null;
                    }
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    public void connectorInfo(final String connName, final Callback<ConnectorInfo> callback) {
        log.trace("Submitting connector info request {}", connName);

        addRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (checkRebalanceNeeded(callback))
                            return null;

                        if (!configState.contains(connName)) {
                            callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                        } else {
                            callback.onCompletion(null, new ConnectorInfo(connName, configState.connectorConfig(connName), configState.tasks(connName)));
                        }
                        return null;
                    }
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    public void connectorConfig(String connName, final Callback<Map<String, String>> callback) {
        // Subset of connectorInfo, so piggy back on that implementation
        log.trace("Submitting connector config read request {}", connName);
        connectorInfo(connName, new Callback<ConnectorInfo>() {
            @Override
            public void onCompletion(Throwable error, ConnectorInfo result) {
                if (error != null)
                    callback.onCompletion(error, null);
                else
                    callback.onCompletion(null, result.config());
            }
        });
    }

    @Override
    public void deleteConnectorConfig(final String connName, final Callback<Created<ConnectorInfo>> callback) {
        addRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        log.trace("Handling connector config request {}", connName);
                        if (!isLeader()) {
                            callback.onCompletion(new NotLeaderException("Only the leader can set connector configs.", leaderUrl()), null);
                            return null;
                        }

                        if (!configState.contains(connName)) {
                            callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                        } else {
                            log.trace("Removing connector config {} {}", connName, configState.connectors());
                            configBackingStore.removeConnectorConfig(connName);
                            callback.onCompletion(null, new Created<ConnectorInfo>(false, null));
                        }
                        return null;
                    }
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    protected Map<String, ConfigValue> validateBasicConnectorConfig(Connector connector,
                                                                    ConfigDef configDef,
                                                                    Map<String, String> config) {
        Map<String, ConfigValue> validatedConfig = super.validateBasicConnectorConfig(connector, configDef, config);
        if (connector instanceof SinkConnector) {
            ConfigValue validatedName = validatedConfig.get(ConnectorConfig.NAME_CONFIG);
            String name = (String) validatedName.value();

            if (workerGroupId.equals(SinkUtils.consumerGroupId(name))) {
                validatedName.addErrorMessage("Consumer group for sink connector named " + name +
                        " conflicts with Connect worker group " + workerGroupId);
            }
        }
        return validatedConfig;
    }


    @Override
    public void putConnectorConfig(final String connName, final Map<String, String> config, final boolean allowReplace,
                                   final Callback<Created<ConnectorInfo>> callback) {
        log.trace("Submitting connector config write request {}", connName);
        addRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        ConfigInfos validatedConfig = validateConnectorConfig(config);
                        if (validatedConfig.errorCount() > 0) {
                            callback.onCompletion(new BadRequestException("Connector configuration is invalid " +
                                    "(use the endpoint `/{connectorType}/config/validate` to get a full list of errors)"), null);
                            return null;
                        }

                        log.trace("Handling connector config request {}", connName);
                        if (!isLeader()) {
                            callback.onCompletion(new NotLeaderException("Only the leader can set connector configs.", leaderUrl()), null);
                            return null;
                        }

                        boolean exists = configState.contains(connName);
                        if (!allowReplace && exists) {
                            callback.onCompletion(new AlreadyExistsException("Connector " + connName + " already exists"), null);
                            return null;
                        }

                        log.trace("Submitting connector config {} {} {}", connName, allowReplace, configState.connectors());
                        configBackingStore.putConnectorConfig(connName, config);

                        // Note that we use the updated connector config despite the fact that we don't have an updated
                        // snapshot yet. The existing task info should still be accurate.
                        ConnectorInfo info = new ConnectorInfo(connName, config, configState.tasks(connName));
                        callback.onCompletion(null, new Created<>(!exists, info));
                        return null;
                    }
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    public void requestTaskReconfiguration(final String connName) {
        log.trace("Submitting connector task reconfiguration request {}", connName);

        addRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        reconfigureConnectorTasksWithRetry(connName);
                        return null;
                    }
                },
                new Callback<Void>() {
                    @Override
                    public void onCompletion(Throwable error, Void result) {
                        if (error != null) {
                            log.error("Unexpected error during task reconfiguration: ", error);
                            log.error("Task reconfiguration for {} failed unexpectedly, this connector will not be properly reconfigured unless manually triggered.", connName);
                        }
                    }
                }
        );
    }

    @Override
    public void taskConfigs(final String connName, final Callback<List<TaskInfo>> callback) {
        log.trace("Submitting get task configuration request {}", connName);

        addRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (checkRebalanceNeeded(callback))
                            return null;

                        if (!configState.contains(connName)) {
                            callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                        } else {
                            List<TaskInfo> result = new ArrayList<>();
                            for (int i = 0; i < configState.taskCount(connName); i++) {
                                ConnectorTaskId id = new ConnectorTaskId(connName, i);
                                result.add(new TaskInfo(id, configState.taskConfig(id)));
                            }
                            callback.onCompletion(null, result);
                        }
                        return null;
                    }
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    public void putTaskConfigs(final String connName, final List<Map<String, String>> configs, final Callback<Void> callback) {
        log.trace("Submitting put task configuration request {}", connName);

        addRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (!isLeader())
                            callback.onCompletion(new NotLeaderException("Only the leader may write task configurations.", leaderUrl()), null);
                        else if (!configState.contains(connName))
                            callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                        else {
                            configBackingStore.putTaskConfigs(connName, configs);
                            callback.onCompletion(null, null);
                        }
                        return null;
                    }
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    public void restartConnector(final String connName, final Callback<Void> callback) {
        addRequest(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (checkRebalanceNeeded(callback))
                    return null;

                if (!configState.connectors().contains(connName)) {
                    callback.onCompletion(new NotFoundException("Unknown connector: " + connName), null);
                    return null;
                }

                if (assignment.connectors().contains(connName)) {
                    try {
                        worker.stopConnector(connName);
                        if (startConnector(connName))
                            callback.onCompletion(null, null);
                        else
                            callback.onCompletion(new ConnectException("Failed to start connector: " + connName), null);
                    } catch (Throwable t) {
                        callback.onCompletion(t, null);
                    }
                } else if (isLeader()) {
                    callback.onCompletion(new NotAssignedException("Cannot restart connector since it is not assigned to this member", member.ownerUrl(connName)), null);
                } else {
                    callback.onCompletion(new NotLeaderException("Cannot restart connector since it is not assigned to this member", leaderUrl()), null);
                }
                return null;
            }
        }, forwardErrorCallback(callback));
    }

    @Override
    public void restartTask(final ConnectorTaskId id, final Callback<Void> callback) {
        addRequest(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (checkRebalanceNeeded(callback))
                    return null;

                if (!configState.connectors().contains(id.connector())) {
                    callback.onCompletion(new NotFoundException("Unknown connector: " + id.connector()), null);
                    return null;
                }

                if (configState.taskConfig(id) == null) {
                    callback.onCompletion(new NotFoundException("Unknown task: " + id), null);
                    return null;
                }

                if (assignment.tasks().contains(id)) {
                    try {
                        worker.stopAndAwaitTask(id);
                        if (startTask(id))
                            callback.onCompletion(null, null);
                        else
                            callback.onCompletion(new ConnectException("Failed to start task: " + id), null);
                    } catch (Throwable t) {
                        callback.onCompletion(t, null);
                    }
                } else if (isLeader()) {
                    callback.onCompletion(new NotAssignedException("Cannot restart task since it is not assigned to this member", member.ownerUrl(id)), null);
                } else {
                    callback.onCompletion(new NotLeaderException("Cannot restart task since it is not assigned to this member", leaderUrl()), null);
                }
                return null;
            }
        }, forwardErrorCallback(callback));
    }

    @Override
    public int generation() {
        return generation;
    }


    // Should only be called from work thread, so synchronization should not be needed
    private boolean isLeader() {
        return assignment != null && member.memberId().equals(assignment.leader());
    }

    /**
     * Get the URL for the leader's REST interface, or null if we do not have the leader's URL yet.
     */
    private String leaderUrl() {
        if (assignment == null)
            return null;
        return assignment.leaderUrl();
    }

    /**
     * Handle post-assignment operations, either trying to resolve issues that kept assignment from completing, getting
     * this node into sync and its work started. Since
     *
     * @return false if we couldn't finish
     */
    private boolean handleRebalanceCompleted() {
        if (this.rebalanceResolved)
            return true;

        // We need to handle a variety of cases after a rebalance:
        // 1. Assignment failed
        //  1a. We are the leader for the round. We will be leader again if we rejoin now, so we need to catch up before
        //      even attempting to. If we can't we should drop out of the group because we will block everyone from making
        //      progress. We can backoff and try rejoining later.
        //  1b. We are not the leader. We might need to catch up. If we're already caught up we can rejoin immediately,
        //      otherwise, we just want to wait reasonable amount of time to catch up and rejoin if we are ready.
        // 2. Assignment succeeded.
        //  2a. We are caught up on configs. Awesome! We can proceed to run our assigned work.
        //  2b. We need to try to catch up - try reading configs for reasonable amount of time.

        boolean needsReadToEnd = false;
        boolean needsRejoin = false;
        if (assignment.failed()) {
            needsRejoin = true;
            if (isLeader()) {
                log.warn("Join group completed, but assignment failed and we are the leader. Reading to end of config and retrying.");
                needsReadToEnd = true;
            } else if (configState.offset() < assignment.offset()) {
                log.warn("Join group completed, but assignment failed and we lagging. Reading to end of config and retrying.");
                needsReadToEnd = true;
            } else {
                log.warn("Join group completed, but assignment failed. We were up to date, so just retrying.");
            }
        } else {
            if (configState.offset() < assignment.offset()) {
                log.warn("Catching up to assignment's config offset.");
                needsReadToEnd = true;
            }
        }

        if (needsReadToEnd) {
            // Force exiting this method to avoid creating any connectors/tasks and require immediate rejoining if
            // we timed out. This should only happen if we failed to read configuration for long enough,
            // in which case giving back control to the main loop will prevent hanging around indefinitely after getting kicked out of the group.
            // We also indicate to the main loop that we failed to readConfigs so it will check that the issue was resolved before trying to join the group
            if (!readConfigToEnd(workerSyncTimeoutMs)) {
                canReadConfigs = false;
                needsRejoin = true;
            }
        }

        if (needsRejoin) {
            member.requestRejoin();
            return false;
        }

        // Should still validate that they match since we may have gone *past* the required offset, in which case we
        // should *not* start any tasks and rejoin
        if (configState.offset() != assignment.offset()) {
            log.info("Current config state offset {} does not match group assignment {}. Forcing rebalance.", configState.offset(), assignment.offset());
            member.requestRejoin();
            return false;
        }

        startWork();

        // We only mark this as resolved once we've actually started work, which allows us to correctly track whether
        // what work is currently active and running. If we bail early, the main tick loop + having requested rejoin
        // guarantees we'll attempt to rejoin before executing this method again.
        rebalanceResolved = true;
        return true;
    }

    /**
     * Try to read to the end of the config log within the given timeout
     * @param timeoutMs maximum time to wait to sync to the end of the log
     * @return true if successful, false if timed out
     */
    private boolean readConfigToEnd(long timeoutMs) {
        log.info("Current config state offset {} is behind group assignment {}, reading to end of config log", configState.offset(), assignment.offset());
        try {
            configBackingStore.refresh(timeoutMs, TimeUnit.MILLISECONDS);
            configState = configBackingStore.snapshot();
            log.info("Finished reading to end of log and updated config snapshot, new config log offset: {}", configState.offset());
            return true;
        } catch (TimeoutException e) {
            // in case reading the log takes too long, leave the group to ensure a quick rebalance (although by default we should be out of the group already)
            // and back off to avoid a tight loop of rejoin-attempt-to-catch-up-leave
            log.warn("Didn't reach end of config log quickly enough", e);
            member.maybeLeaveGroup();
            backoff(workerUnsyncBackoffMs);
            return false;
        }
    }

    private void backoff(long ms) {
        Utils.sleep(ms);
    }

    private void startAndStop(Collection<Callable<Void>> callables) {
        try {
            startAndStopExecutor.invokeAll(callables);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private void startWork() {
        // Start assigned connectors and tasks
        log.info("Starting connectors and tasks using config offset {}", assignment.offset());
        List<Callable<Void>> callables = new ArrayList<>();
        for (String connectorName : assignment.connectors()) {
            callables.add(getConnectorStartingCallable(connectorName));
        }

        for (ConnectorTaskId taskId : assignment.tasks()) {
            callables.add(getTaskStartingCallable(taskId));
        }
        startAndStop(callables);
        log.info("Finished starting connectors and tasks");
    }

    private boolean startTask(ConnectorTaskId taskId) {
        log.info("Starting task {}", taskId);
        return worker.startTask(
                taskId,
                configState.connectorConfig(taskId.connector()),
                configState.taskConfig(taskId),
                this,
                configState.targetState(taskId.connector())
        );
    }

    private Callable<Void> getTaskStartingCallable(final ConnectorTaskId taskId) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    startTask(taskId);
                } catch (Throwable t) {
                    log.error("Couldn't instantiate task {} because it has an invalid task configuration. This task will not execute until reconfigured.",
                            taskId, t);
                    onFailure(taskId, t);
                }
                return null;
            }
        };
    }

    private Callable<Void> getTaskStoppingCallable(final ConnectorTaskId taskId) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                worker.stopAndAwaitTask(taskId);
                return null;
            }
        };
    }

    // Helper for starting a connector with the given name, which will extract & parse the config, generate connector
    // context and add to the worker. This needs to be called from within the main worker thread for this herder.
    private boolean startConnector(String connectorName) {
        log.info("Starting connector {}", connectorName);
        final Map<String, String> configProps = configState.connectorConfig(connectorName);
        final ConnectorContext ctx = new HerderConnectorContext(this, connectorName);
        final TargetState initialState = configState.targetState(connectorName);
        boolean started = worker.startConnector(connectorName, configProps, ctx, this, initialState);

        // Immediately request configuration since this could be a brand new connector. However, also only update those
        // task configs if they are actually different from the existing ones to avoid unnecessary updates when this is
        // just restoring an existing connector.
        if (started && initialState == TargetState.STARTED)
            reconfigureConnectorTasksWithRetry(connectorName);

        return started;
    }

    private Callable<Void> getConnectorStartingCallable(final String connectorName) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    startConnector(connectorName);
                } catch (Throwable t) {
                    log.error("Couldn't instantiate connector " + connectorName + " because it has an invalid connector " +
                            "configuration. This connector will not execute until reconfigured.", t);
                    onFailure(connectorName, t);
                }
                return null;
            }
        };
    }

    private Callable<Void> getConnectorStoppingCallable(final String connectorName) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    worker.stopConnector(connectorName);
                } catch (Throwable t) {
                    log.error("Failed to shut down connector " + connectorName, t);
                }
                return null;
            }
        };
    }

    private void reconfigureConnectorTasksWithRetry(final String connName) {
        reconfigureConnector(connName, new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                // If we encountered an error, we don't have much choice but to just retry. If we don't, we could get
                // stuck with a connector that thinks it has generated tasks, but wasn't actually successful and therefore
                // never makes progress. The retry has to run through a HerderRequest since this callback could be happening
                // from the HTTP request forwarding thread.
                if (error != null) {
                    log.error("Failed to reconfigure connector's tasks, retrying after backoff:", error);
                    addRequest(RECONFIGURE_CONNECTOR_TASKS_BACKOFF_MS,
                            new Callable<Void>() {
                                @Override
                                public Void call() throws Exception {
                                    reconfigureConnectorTasksWithRetry(connName);
                                    return null;
                                }
                            }, new Callback<Void>() {
                                @Override
                                public void onCompletion(Throwable error, Void result) {
                                    log.error("Unexpected error during connector task reconfiguration: ", error);
                                    log.error("Task reconfiguration for {} failed unexpectedly, this connector will not be properly reconfigured unless manually triggered.", connName);
                                }
                            }
                    );
                }
            }
        });
    }

    // Updates configurations for a connector by requesting them from the connector, filling in parameters provided
    // by the system, then checks whether any configs have actually changed before submitting the new configs to storage
    private void reconfigureConnector(final String connName, final Callback<Void> cb) {
        try {
            if (!worker.isRunning(connName)) {
                log.info("Skipping reconfiguration of connector {} since it is not running", connName);
                return;
            }

            Map<String, String> configs = configState.connectorConfig(connName);

            ConnectorConfig connConfig;
            List<String> sinkTopics = null;
            if (worker.isSinkConnector(connName)) {
                connConfig = new SinkConnectorConfig(configs);
                sinkTopics = connConfig.getList(SinkConnectorConfig.TOPICS_CONFIG);
            } else {
                connConfig = new SourceConnectorConfig(configs);
            }

            final List<Map<String, String>> taskProps
                    = worker.connectorTaskConfigs(connName, connConfig.getInt(ConnectorConfig.TASKS_MAX_CONFIG), sinkTopics);
            boolean changed = false;
            int currentNumTasks = configState.taskCount(connName);
            if (taskProps.size() != currentNumTasks) {
                log.debug("Change in connector task count from {} to {}, writing updated task configurations", currentNumTasks, taskProps.size());
                changed = true;
            } else {
                int index = 0;
                for (Map<String, String> taskConfig : taskProps) {
                    if (!taskConfig.equals(configState.taskConfig(new ConnectorTaskId(connName, index)))) {
                        log.debug("Change in task configurations, writing updated task configurations");
                        changed = true;
                        break;
                    }
                    index++;
                }
            }
            if (changed) {
                if (isLeader()) {
                    configBackingStore.putTaskConfigs(connName, taskProps);
                    cb.onCompletion(null, null);
                } else {
                    // We cannot forward the request on the same thread because this reconfiguration can happen as a result of connector
                    // addition or removal. If we blocked waiting for the response from leader, we may be kicked out of the worker group.
                    forwardRequestExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                String reconfigUrl = RestServer.urlJoin(leaderUrl(), "/connectors/" + connName + "/tasks");
                                RestServer.httpRequest(reconfigUrl, "POST", taskProps, null);
                                cb.onCompletion(null, null);
                            } catch (ConnectException e) {
                                log.error("Request to leader to reconfigure connector tasks failed", e);
                                cb.onCompletion(e, null);
                            }
                        }
                    });
                }
            }
        } catch (Throwable t) {
            cb.onCompletion(t, null);
        }
    }

    private boolean checkRebalanceNeeded(Callback<?> callback) {
        // Raise an error if we are expecting a rebalance to begin. This prevents us from forwarding requests
        // based on stale leadership or assignment information
        if (needsReconfigRebalance) {
            callback.onCompletion(new RebalanceNeededException("Request cannot be completed because a rebalance is expected"), null);
            return true;
        }
        return false;
    }

    HerderRequest addRequest(Callable<Void> action, Callback<Void> callback) {
        return addRequest(0, action, callback);
    }

    HerderRequest addRequest(long delayMs, Callable<Void> action, Callback<Void> callback) {
        HerderRequest req = new HerderRequest(time.milliseconds() + delayMs, requestSeqNum.incrementAndGet(), action, callback);
        requests.add(req);
        if (peekWithoutException() == req)
            member.wakeup();
        return req;
    }

    private HerderRequest peekWithoutException() {
        try {
            return requests.isEmpty() ? null : requests.first();
        } catch (NoSuchElementException e) {
            // Ignore exception. Should be rare. Means that the collection became empty between
            // checking the size and retrieving the first element.
        }
        return null;
    }

    public class ConfigUpdateListener implements ConfigBackingStore.UpdateListener {
        @Override
        public void onConnectorConfigRemove(String connector) {
            log.info("Connector {} config removed", connector);

            synchronized (DistributedHerder.this) {
                // rebalance after connector removal to ensure that existing tasks are balanced among workers
                if (configState.contains(connector))
                    needsReconfigRebalance = true;
                connectorConfigUpdates.add(connector);
            }
            member.wakeup();
        }

        @Override
        public void onConnectorConfigUpdate(String connector) {
            log.info("Connector {} config updated", connector);

            // Stage the update and wake up the work thread. Connector config *changes* only need the one connector
            // to be bounced. However, this callback may also indicate a connector *addition*, which does require
            // a rebalance, so we need to be careful about what operation we request.
            synchronized (DistributedHerder.this) {
                if (!configState.contains(connector))
                    needsReconfigRebalance = true;
                connectorConfigUpdates.add(connector);
            }
            member.wakeup();
        }

        @Override
        public void onTaskConfigUpdate(Collection<ConnectorTaskId> tasks) {
            log.info("Tasks {} configs updated", tasks);

            // Stage the update and wake up the work thread. No need to record the set of tasks here because task reconfigs
            // always need a rebalance to ensure offsets get committed.
            // TODO: As an optimization, some task config updates could avoid a rebalance. In particular, single-task
            // connectors clearly don't need any coordination.
            synchronized (DistributedHerder.this) {
                needsReconfigRebalance = true;
            }
            member.wakeup();
        }

        @Override
        public void onConnectorTargetStateChange(String connector) {
            log.info("Connector {} target state change", connector);

            synchronized (DistributedHerder.this) {
                connectorTargetStateChanges.add(connector);
            }
            member.wakeup();
        }
    }

    static class HerderRequest implements Comparable<HerderRequest> {
        private final long at;
        private final long seq;
        private final Callable<Void> action;
        private final Callback<Void> callback;

        public HerderRequest(long at, long seq, Callable<Void> action, Callback<Void> callback) {
            this.at = at;
            this.seq = seq;
            this.action = action;
            this.callback = callback;
        }

        public Callable<Void> action() {
            return action;
        }

        public Callback<Void> callback() {
            return callback;
        }

        @Override
        public int compareTo(HerderRequest o) {
            final int cmp = Long.compare(at, o.at);
            return cmp == 0 ? Long.compare(seq, o.seq) : cmp;
        }
    }

    private static final Callback<Void> forwardErrorCallback(final Callback<?> callback) {
        return new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                if (error != null)
                    callback.onCompletion(error, null);
            }
        };
    }

    private void updateDeletedConnectorStatus() {
        ClusterConfigState snapshot = configBackingStore.snapshot();
        Set<String> connectors = snapshot.connectors();
        for (String connector : statusBackingStore.connectors()) {
            if (!connectors.contains(connector)) {
                log.debug("Cleaning status information for connector {}", connector);
                onDeletion(connector);
            }
        }
    }

    // Rebalances are triggered internally from the group member, so these are always executed in the work thread.
    public class RebalanceListener implements WorkerRebalanceListener {
        @Override
        public void onAssigned(ConnectProtocol.Assignment assignment, int generation) {
            // This callback just logs the info and saves it. The actual response is handled in the main loop, which
            // ensures the group member's logic for rebalancing can complete, potentially long-running steps to
            // catch up (or backoff if we fail) not executed in a callback, and so we'll be able to invoke other
            // group membership actions (e.g., we may need to explicitly leave the group if we cannot handle the
            // assigned tasks).
            log.info("Joined group and got assignment: {}", assignment);
            synchronized (DistributedHerder.this) {
                DistributedHerder.this.assignment = assignment;
                DistributedHerder.this.generation = generation;
                rebalanceResolved = false;
            }

            // Delete the statuses of all connectors removed prior to the start of this rebalance. This has to
            // be done after the rebalance completes to avoid race conditions as the previous generation attempts
            // to change the state to UNASSIGNED after tasks have been stopped.
            if (isLeader())
                updateDeletedConnectorStatus();

            // We *must* interrupt any poll() call since this could occur when the poll starts, and we might then
            // sleep in the poll() for a long time. Forcing a wakeup ensures we'll get to process this event in the
            // main thread.
            member.wakeup();
        }

        @Override
        public void onRevoked(String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
            log.info("Rebalance started");

            // Note that since we don't reset the assignment, we we don't revoke leadership here. During a rebalance,
            // it is still important to have a leader that can write configs, offsets, etc.

            if (rebalanceResolved) {
                // TODO: Technically we don't have to stop connectors at all until we know they've really been removed from
                // this worker. Instead, we can let them continue to run but buffer any update requests (which should be
                // rare anyway). This would avoid a steady stream of start/stop, which probably also includes lots of
                // unnecessary repeated connections to the source/sink system.
                List<Callable<Void>> callables = new ArrayList<>();
                for (final String connectorName : connectors) {
                    callables.add(getConnectorStoppingCallable(connectorName));
                }

                // TODO: We need to at least commit task offsets, but if we could commit offsets & pause them instead of
                // stopping them then state could continue to be reused when the task remains on this worker. For example,
                // this would avoid having to close a connection and then reopen it when the task is assigned back to this
                // worker again.
                for (final ConnectorTaskId taskId : tasks) {
                    callables.add(getTaskStoppingCallable(taskId));
                }

                // The actual timeout for graceful task stop is applied in worker's stopAndAwaitTask method.
                startAndStop(callables);

                // Ensure that all status updates have been pushed to the storage system before rebalancing.
                // Otherwise, we may inadvertently overwrite the state with a stale value after the rebalance
                // completes.
                statusBackingStore.flush();
                log.info("Finished stopping tasks in preparation for rebalance");
            } else {
                log.info("Wasn't unable to resume work after last rebalance, can skip stopping connectors and tasks");
            }
        }
    }

}

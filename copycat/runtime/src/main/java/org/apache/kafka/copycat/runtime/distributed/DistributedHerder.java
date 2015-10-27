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

package org.apache.kafka.copycat.runtime.distributed;

import org.apache.kafka.clients.consumer.WakeupException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.connector.ConnectorContext;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Herder;
import org.apache.kafka.copycat.runtime.HerderConnectorContext;
import org.apache.kafka.copycat.runtime.TaskConfig;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.apache.kafka.copycat.storage.KafkaConfigStorage;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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
 */
public class DistributedHerder implements Herder, Runnable {
    private static final Logger log = LoggerFactory.getLogger(DistributedHerder.class);

    private final Worker worker;
    private final KafkaConfigStorage configStorage;
    private ClusterConfigState configState;

    private final int workerSyncTimeoutMs;
    private final int workerUnsyncBackoffMs;

    private final WorkerGroupMember member;
    private final AtomicBoolean stopping;
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    // Track enough information about the current membership state to be able to determine which requests via the API
    // and the from other nodes are safe to process
    private boolean rebalanceResolved;
    private CopycatProtocol.Assignment assignment;

    // To handle most external requests, like creating or destroying a connector, we can use a generic request where
    // the caller specifies all the code that should be executed.
    private final Queue<HerderRequest> requests = new LinkedBlockingDeque<>();
    // Config updates can be collected and applied together when possible. Also, we need to take care to rebalance when
    // needed (e.g. task reconfiguration, which requires everyone to coordinate offset commits).
    private final Set<String> connectorConfigUpdates = new HashSet<>();
    private boolean needsReconfigRebalance;

    public DistributedHerder(Worker worker, Map<String, ?> configs) {
        this(worker, configs, null, null);
    }

    // public for testing
    public DistributedHerder(Worker worker, Map<String, ?> configs, KafkaConfigStorage configStorage, WorkerGroupMember member) {
        this.worker = worker;
        if (configStorage != null) {
            // For testing. Assume configuration has already been performed
            this.configStorage = configStorage;
        } else {
            this.configStorage = new KafkaConfigStorage(worker.getInternalValueConverter(), connectorConfigCallback(), taskConfigCallback());
            this.configStorage.configure(configs);
        }
        configState = ClusterConfigState.EMPTY;

        DistributedHerderConfig config = new DistributedHerderConfig(configs);
        this.workerSyncTimeoutMs = config.getInt(DistributedHerderConfig.WORKER_SYNC_TIMEOUT_MS_CONFIG);
        this.workerUnsyncBackoffMs = config.getInt(DistributedHerderConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG);

        this.member = member != null ? member : new WorkerGroupMember(config, this.configStorage, rebalanceListener());
        stopping = new AtomicBoolean(false);

        rebalanceResolved = true; // If we still need to follow up after a rebalance occurred, starting up tasks
        needsReconfigRebalance = false;
    }

    @Override
    public void start() {
        Thread thread = new Thread(this);
        thread.start();
    }

    public void run() {
        try {
            log.info("Herder starting");

            configStorage.start();

            log.info("Herder started");

            while (!stopping.get()) {
                tick();
            }

            halt();

            log.info("Herder stopped");
        } finally {
            stopLatch.countDown();
        }
    }

    // public for testing
    public void tick() {
        // The main loop does two primary things: 1) drive the group membership protocol, responding to rebalance events
        // as they occur, and 2) handle external requests targeted at the leader. All the "real" work of the herder is
        // performed in this thread, which keeps synchronization straightforward at the cost of some operations possibly
        // blocking up this thread (especially those in callbacks due to rebalance events).

        try {
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
        while (!requests.isEmpty()) {
            HerderRequest request = requests.poll();
            try {
                request.callback().onCompletion(null, request.action().call());
            } catch (Throwable t) {
                request.callback().onCompletion(t, null);
            }
        }

        // Process any configuration updates
        synchronized (this) {
            if (needsReconfigRebalance || !connectorConfigUpdates.isEmpty()) {
                // Connector reconfigs only need local updates since there is no coordination between workers required.
                // However, if connectors were added or removed, work needs to be rebalanced since we have more work
                // items to distribute among workers.
                ClusterConfigState newConfigState = configStorage.snapshot();
                if (!newConfigState.connectors().equals(configState.connectors()))
                    needsReconfigRebalance = true;
                configState = newConfigState;
                if (needsReconfigRebalance) {
                    // Task reconfigs require a rebalance. Request the rebalance, clean out state, and then restart
                    // this loop, which will then ensure the rebalance occurs without any other requests being
                    // processed until it completes.
                    member.requestRejoin();
                    // Any connector config updates will be addressed during the rebalance too
                    connectorConfigUpdates.clear();
                    needsReconfigRebalance = false;
                    return;
                } else if (!connectorConfigUpdates.isEmpty()) {
                    // If we only have connector config updates, we can just bounce the updated connectors that are
                    // currently assigned to this worker.
                    Set<String> localConnectors = worker.connectorNames();
                    for (String connectorName : connectorConfigUpdates) {
                        if (!localConnectors.contains(connectorName))
                            continue;
                        worker.stopConnector(connectorName);
                        // The update may be a deletion, so verify we actually need to restart the connector
                        if (configState.connectors().contains(connectorName))
                            startConnector(connectorName);
                    }
                    connectorConfigUpdates.clear();
                }
            }
        }

        // Let the group take any actions it needs to
        try {
            member.poll(Long.MAX_VALUE);
            // Ensure we're in a good state in our group. If not restart and everything should be setup to rejoin
            if (!handleRebalanceCompleted()) return;
        } catch (WakeupException e) { // FIXME should not be WakeupException
            // Ignore. Just indicates we need to check the exit flag, for requested actions, etc.
        }
    }

    // public for testing
    public void halt() {
        synchronized (this) {
            // Clean up any connectors and tasks that are still running.
            log.info("Stopping connectors and tasks that are still assigned to this worker.");
            for (String connName : new HashSet<>(worker.connectorNames())) {
                try {
                    worker.stopConnector(connName);
                } catch (Throwable t) {
                    log.error("Failed to shut down connector " + connName, t);
                }
            }
            for (ConnectorTaskId taskId : new HashSet<>(worker.taskIds())) {
                try {
                    worker.stopTask(taskId);
                } catch (Throwable t) {
                    log.error("Failed to shut down task " + taskId, t);
                }
            }

            member.stop();

            // Explicitly fail any outstanding requests so they actually get a response and get an understandable reason
            // for their failure
            while (!requests.isEmpty()) {
                HerderRequest request = requests.poll();
                request.callback().onCompletion(new CopycatException("Worker is shutting down"), null);
            }

            if (configStorage != null)
                configStorage.stop();
        }
    }

    @Override
    public void stop() {
        log.info("Herder stopping");

        stopping.set(true);
        member.wakeup();
        while (stopLatch.getCount() > 0) {
            try {
                stopLatch.await();
            } catch (InterruptedException e) {
                // ignore, should not happen
            }
        }
    }

    @Override
    public synchronized void addConnector(final Map<String, String> connectorProps,
                                          final Callback<String> callback) {
        final ConnectorConfig connConfig;
        final String connName;
        try {
            connConfig = new ConnectorConfig(connectorProps);
            connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        } catch (Throwable t) {
            if (callback != null)
                callback.onCompletion(t, null);
            return;
        }

        log.debug("Submitting connector config {}", connName);

        requests.add(new HerderRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (!isLeader())
                            throw new NotLeaderException("Only the leader can add connectors.");

                        log.debug("Submitting connector config {}", connName);
                        configStorage.putConnectorConfig(connName, connectorProps);

                        return null;
                    }
                },
                new Callback<Void>() {
                    @Override
                    public void onCompletion(Throwable error, Void result) {
                        if (callback == null) return;

                        if (error != null)
                            callback.onCompletion(error, null);
                        else
                            callback.onCompletion(null, connName);
                    }
                }));
        member.wakeup();
    }

    @Override
    public synchronized void deleteConnector(final String connName, final Callback<Void> callback) {
        log.debug("Submitting connector config deletion {}", connName);

        requests.add(new HerderRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (!isLeader())
                            throw new NotLeaderException("Only the leader can delete connectors.");

                        log.debug("Submitting null connector config {}", connName);
                        configStorage.putConnectorConfig(connName, null);
                        return null;
                    }
                },
                new Callback<Void>() {
                    @Override
                    public void onCompletion(Throwable error, Void result) {
                        if (callback != null)
                            callback.onCompletion(error, null);
                    }
                }
        ));
        member.wakeup();
    }

    @Override
    public synchronized void requestTaskReconfiguration(final String connName) {
        requests.add(new HerderRequest(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        reconfigureConnector(connName);
                        return null;
                    }
                }
        ));
        member.wakeup();
    }


    private boolean isLeader() {
        return assignment != null && member.memberId().equals(assignment.leader());
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

        rebalanceResolved = true;

        // We need to handle a variety of cases after a rebalance:
        // 1. Assignment failed
        //  1a. We are the leader for the round. We will be leader again if we rejoin now, so we need to catch up before
        //      even attempting to. If we can't we should drop out of the group because we will block everyone from making
        //      progress. We can backoff and try rejoining later.
        //  1b. We are not the leader. We might need to catch up. If we're already caught up we can rejoin immediately,
        //      otherwise, we just want to wait indefinitely to catch up and rejoin whenver we're finally ready.
        // 2. Assignment succeeded.
        //  2a. We are caught up on configs. Awesome! We can proceed to run our assigned work.
        //  2b. We need to try to catch up. We can do this potentially indefinitely because if it takes to long, we'll
        //      be kicked out of the group anyway due to lack of heartbeats.

        boolean needsReadToEnd = false;
        long syncConfigsTimeoutMs = Long.MAX_VALUE;
        boolean needsRejoin = false;
        if (assignment.failed()) {
            needsRejoin = true;
            if (isLeader()) {
                log.warn("Join group completed, but assignment failed and we are the leader. Reading to end of config and retrying.");
                needsReadToEnd = true;
                syncConfigsTimeoutMs = workerSyncTimeoutMs;
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
            // we timed out. This should only happen if we were the leader and didn't finish quickly enough, in which
            // case we've waited a long time and should have already left the group OR the timeout should have been
            // very long and not having finished also indicates we've waited longer than the session timeout.
            if (!readConfigToEnd(syncConfigsTimeoutMs))
                needsRejoin = true;
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
            configStorage.readToEnd().get(timeoutMs, TimeUnit.MILLISECONDS);
            configState = configStorage.snapshot();
            log.info("Finished reading to end of log and updated config snapshot, new config log offset: {}", configState.offset());
            return true;
        } catch (TimeoutException e) {
            log.warn("Didn't reach end of config log quickly enough", e);
            // TODO: With explicit leave group support, it would be good to explicitly leave the group *before* this
            // backoff since it'll be longer than the session timeout
            if (isLeader())
                backoff(workerUnsyncBackoffMs);
            return false;
        } catch (InterruptedException | ExecutionException e) {
            throw new CopycatException("Error trying to catch up after assignment", e);
        }
    }

    private void backoff(long ms) {
        Utils.sleep(ms);
    }

    private void startWork() {
        // Start assigned connectors and tasks
        log.info("Starting connectors and tasks using config offset {}", assignment.offset());
        for (String connectorName : assignment.connectors()) {
            try {
                startConnector(connectorName);
            } catch (ConfigException e) {
                log.error("Couldn't instantiate connector " + connectorName + " because it has an invalid connector " +
                        "configuration. This connector will not execute until reconfigured.", e);
            }
        }
        for (ConnectorTaskId taskId : assignment.tasks()) {
            try {
                log.info("Starting task {}", taskId);
                Map<String, String> configs = configState.taskConfig(taskId);
                TaskConfig taskConfig = new TaskConfig(configs);
                worker.addTask(taskId, taskConfig);
            } catch (ConfigException e) {
                log.error("Couldn't instantiate task " + taskId + " because it has an invalid task " +
                        "configuration. This task will not execute until reconfigured.", e);
            }
        }
    }

    // Helper for starting a connector with the given name, which will extract & parse the config, generate connector
    // context and add to the worker. This needs to be called from within the main worker thread for this herder.
    private void startConnector(String connectorName) {
        log.info("Starting connector {}", connectorName);
        Map<String, String> configs = configState.connectorConfig(connectorName);
        ConnectorConfig connConfig = new ConnectorConfig(configs);
        String connName = connConfig.getString(ConnectorConfig.NAME_CONFIG);
        ConnectorContext ctx = new HerderConnectorContext(DistributedHerder.this, connName);
        worker.addConnector(connConfig, ctx);

        // Immediately request configuration since this could be a brand new connector. However, also only update those
        // task configs if they are actually different from the existing ones to avoid unnecessary updates when this is
        // just restoring an existing connector.
        reconfigureConnector(connName);
    }

    // Updates configurations for a connector by requesting them from the connector, filling in parameters provided
    // by the system, then checks whether any configs have actually changed before submitting the new configs to storage
    private void reconfigureConnector(String connName) {
        Map<String, String> configs = configState.connectorConfig(connName);
        ConnectorConfig connConfig = new ConnectorConfig(configs);

        List<String> sinkTopics = null;
        if (SinkConnector.class.isAssignableFrom(connConfig.getClass(ConnectorConfig.CONNECTOR_CLASS_CONFIG)))
            sinkTopics = connConfig.getList(ConnectorConfig.TOPICS_CONFIG);

        Map<ConnectorTaskId, Map<String, String>> taskProps
                = worker.reconfigureConnectorTasks(connName, connConfig.getInt(ConnectorConfig.TASKS_MAX_CONFIG), sinkTopics);
        boolean changed = false;
        int currentNumTasks = configState.taskCount(connName);
        if (taskProps.size() != currentNumTasks) {
            log.debug("Change in connector task count from {} to {}, writing updated task configurations", currentNumTasks, taskProps.size());
            changed = true;
        } else {
            for (Map.Entry<ConnectorTaskId, Map<String, String>> taskConfig : taskProps.entrySet()) {
                if (!taskConfig.getValue().equals(configState.taskConfig(taskConfig.getKey()))) {
                    log.debug("Change in task configurations, writing updated task configurations");
                    changed = true;
                    break;
                }
            }
        }
        if (changed) {
            // FIXME: Configs should only be written by the leader to avoid conflicts due to zombies. However, until the
            // REST API is available to forward this request, we need to do this on the worker that generates the config
            configStorage.putTaskConfigs(taskProps);
        }
    }


    private class HerderRequest {
        private final Callable<Void> action;
        private final Callback<Void> callback;

        public HerderRequest(Callable<Void> action, Callback<Void> callback) {
            this.action = action;
            this.callback = callback;
        }

        public HerderRequest(Callable<Void> action) {
            this.action = action;
            this.callback = DEFAULT_CALLBACK;
        }

        public Callable<Void> action() {
            return action;
        }

        public Callback<Void> callback() {
            return callback;
        }
    }

    private static final Callback<Void> DEFAULT_CALLBACK = new Callback<Void>() {
        @Override
        public void onCompletion(Throwable error, Void result) {
            if (error != null)
                log.error("HerderRequest's action threw an exception: ", error);
        }
    };


    // Config callbacks are triggered from the KafkaConfigStorage thread
    private Callback<String> connectorConfigCallback() {
        return new Callback<String>() {
            @Override
            public void onCompletion(Throwable error, String connector) {
                log.debug("Connector {} config updated", connector);
                // Stage the update and wake up the work thread. Connector config *changes* only need the one connector
                // to be bounced. However, this callback may also indicate a connector *addition*, which does require
                // a rebalance, so we need to be careful about what operation we request.
                synchronized (DistributedHerder.this) {
                    connectorConfigUpdates.add(connector);
                }
                member.wakeup();
            }
        };
    }

    private Callback<List<ConnectorTaskId>> taskConfigCallback() {
        return new Callback<List<ConnectorTaskId>>() {
            @Override
            public void onCompletion(Throwable error, List<ConnectorTaskId> tasks) {
                log.debug("Tasks {} configs updated", tasks);
                // Stage the update and wake up the work thread. No need to record the set of tasks here because task reconfigs
                // always need a rebalance to ensure offsets get committed.
                // TODO: As an optimization, some task config updates could avoid a rebalance. In particular, single-task
                // connectors clearly don't need any coordination.
                synchronized (DistributedHerder.this) {
                    needsReconfigRebalance = true;
                }
                member.wakeup();
            }
        };
    }

    // Rebalances are triggered internally from the group member, so these are always executed in the work thread.
    private WorkerRebalanceListener rebalanceListener() {
        return new WorkerRebalanceListener() {
            @Override
            public void onAssigned(CopycatProtocol.Assignment assignment) {
                // This callback just logs the info and saves it. The actual response is handled in the main loop, which
                // ensures the group member's logic for rebalancing can complete, potentially long-running steps to
                // catch up (or backoff if we fail) not executed in a callback, and so we'll be able to invoke other
                // group membership actions (e.g., we may need to explicitly leave the group if we cannot handle the
                // assigned tasks).
                log.info("Joined group and got assignment: {}", assignment);
                DistributedHerder.this.assignment = assignment;
                rebalanceResolved = false;
            }

            @Override
            public void onRevoked(String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
                log.info("Rebalance started");

                // Note that since we don't reset the assignment, we we don't revoke leadership here. During a rebalance,
                // it is still important to have a leader that can write configs, offsets, etc.

                // TODO: Parallelize this. We should be able to request all connectors and tasks to stop, then wait on all of
                // them to finish
                // TODO: Technically we don't have to stop connectors at all until we know they've really been removed from
                // this worker. Instead, we can let them continue to run but buffer any update requests (which should be
                // rare anyway). This would avoid a steady stream of start/stop, which probably also includes lots of
                // unnecessary repeated connections to the source/sink system.
                for (String connectorName : connectors)
                    worker.stopConnector(connectorName);
                // TODO: We need to at least commit task offsets, but if we could commit offsets & pause them instead of
                // stopping them then state could continue to be reused when the task remains on this worker. For example,
                // this would avoid having to close a connection and then reopen it when the task is assigned back to this
                // worker again.
                for (ConnectorTaskId taskId : tasks)
                    worker.stopTask(taskId);
            }
        };
    }
}

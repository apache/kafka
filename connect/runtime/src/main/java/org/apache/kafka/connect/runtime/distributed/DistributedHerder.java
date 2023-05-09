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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.CloseableConnectorContext;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.ConnectMetricsRegistry;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.HerderConnectorContext;
import org.apache.kafka.connect.runtime.HerderRequest;
import org.apache.kafka.connect.runtime.RestartPlan;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.SessionKey;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.storage.PrivilegedWriteException;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.SinkUtils;
import org.slf4j.Logger;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;

/**
 * <p>
 *     Distributed "herder" that coordinates with other workers to spread work across multiple processes.
 * </p>
 * <p>
 *     Under the hood, this is implemented as a group managed by Kafka's group membership facilities (i.e. the generalized
 *     group/consumer coordinator). Each instance of DistributedHerder joins the group and indicates what its current
 *     configuration state is (where it is in the configuration log). The group coordinator selects one member to take
 *     this information and assign each instance a subset of the active connectors & tasks to execute. The assignment
 *     strategy depends on the {@link ConnectAssignor} used. Once an assignment is received, the DistributedHerder simply
 *     runs its assigned connectors and tasks in a {@link Worker}.
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
    private static final AtomicInteger CONNECT_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private final Logger log;

    private static final long FORWARD_REQUEST_SHUTDOWN_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long START_AND_STOP_SHUTDOWN_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long RECONFIGURE_CONNECTOR_TASKS_BACKOFF_INITIAL_MS = 250;
    private static final long RECONFIGURE_CONNECTOR_TASKS_BACKOFF_MAX_MS = 60000;
    private static final long CONFIG_TOPIC_WRITE_PRIVILEGES_BACKOFF_MS = 250;
    private static final int START_STOP_THREAD_POOL_SIZE = 8;
    private static final short BACKOFF_RETRIES = 5;

    private final AtomicLong requestSeqNum = new AtomicLong();

    private final Time time;
    private final HerderMetrics herderMetrics;
    private final List<AutoCloseable> uponShutdown;

    private final String workerGroupId;
    private final int workerSyncTimeoutMs;
    private final int workerUnsyncBackoffMs;
    private final int keyRotationIntervalMs;
    private final String requestSignatureAlgorithm;
    private final List<String> keySignatureVerificationAlgorithms;
    private final KeyGenerator keyGenerator;
    private final RestClient restClient;

    // Visible for testing
    ExecutorService forwardRequestExecutor;
    private final ExecutorService herderExecutor;
    // Visible for testing
    ExecutorService startAndStopExecutor;
    private final WorkerGroupMember member;
    private final AtomicBoolean stopping;
    private final boolean isTopicTrackingEnabled;

    // Track enough information about the current membership state to be able to determine which requests via the API
    // and the from other nodes are safe to process
    private boolean rebalanceResolved;
    private ExtendedAssignment runningAssignment = ExtendedAssignment.empty();
    private final Set<ConnectorTaskId> tasksToRestart = new HashSet<>();
    // visible for testing
    ExtendedAssignment assignment;
    private boolean canReadConfigs;
    // visible for testing
    protected ClusterConfigState configState;

    // To handle most external requests, like creating or destroying a connector, we can use a generic request where
    // the caller specifies all the code that should be executed.
    final NavigableSet<DistributedHerderRequest> requests = new ConcurrentSkipListSet<>();
    // Config updates can be collected and applied together when possible. Also, we need to take care to rebalance when
    // needed (e.g. task reconfiguration, which requires everyone to coordinate offset commits).
    private Set<String> connectorConfigUpdates = new HashSet<>();
    private Set<ConnectorTaskId> taskConfigUpdates = new HashSet<>();
    // Similarly collect target state changes (when observed by the config storage listener) for handling in the
    // herder's main thread.
    private Set<String> connectorTargetStateChanges = new HashSet<>();
    // Access to this map is protected by the herder's monitor
    private final Map<String, ZombieFencing> activeZombieFencings = new HashMap<>();
    private final List<String> restNamespace;
    private boolean needsReconfigRebalance;
    private volatile boolean fencedFromConfigTopic;
    private volatile int generation;
    private volatile long scheduledRebalance;
    private volatile SecretKey sessionKey;
    private volatile long keyExpiration;
    private short currentProtocolVersion;
    private short backoffRetries;

    // visible for testing
    // The latest pending restart request for each named connector
    final Map<String, RestartRequest> pendingRestartRequests = new HashMap<>();

    // The thread that the herder's tick loop runs on. Would be final, but cannot be set in the constructor,
    // and it's also useful to be able to modify it for testing
    Thread herderThread;

    private final DistributedConfig config;

    /**
     * Create a herder that will form a Connect cluster with other {@link DistributedHerder} instances (in this or other JVMs)
     * that have the same group ID.
     *
     * @param config             the configuration for the worker; may not be null
     * @param time               the clock to use; may not be null
     * @param worker             the {@link Worker} instance to use; may not be null
     * @param kafkaClusterId     the identifier of the Kafka cluster to use for internal topics; may not be null
     * @param statusBackingStore the backing store for statuses; may not be null
     * @param configBackingStore the backing store for connector configurations; may not be null
     * @param restUrl            the URL of this herder's REST API; may not be null, but may be an arbitrary placeholder
     *                           value if this worker does not expose a REST API
     * @param restClient         a REST client that can be used to issue requests to other workers in the cluster; may
     *                           be null if inter-worker communication is not enabled
     * @param connectorClientConfigOverridePolicy the policy specifying the client configuration properties that may be overridden
     *                                            in connector configurations; may not be null
     * @param restNamespace      zero or more path elements to prepend to the paths of forwarded REST requests; may be empty, but not null
     * @param uponShutdown       any {@link AutoCloseable} objects that should be closed when this herder is {@link #stop() stopped},
     *                           after all services and resources owned by this herder are stopped
     */
    public DistributedHerder(DistributedConfig config,
                             Time time,
                             Worker worker,
                             String kafkaClusterId,
                             StatusBackingStore statusBackingStore,
                             ConfigBackingStore configBackingStore,
                             String restUrl,
                             RestClient restClient,
                             ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                             List<String> restNamespace,
                             AutoCloseable... uponShutdown) {
        this(config, worker, worker.workerId(), kafkaClusterId, statusBackingStore, configBackingStore, null, restUrl, restClient, worker.metrics(),
                time, connectorClientConfigOverridePolicy, restNamespace, null, uponShutdown);
        configBackingStore.setUpdateListener(new ConfigUpdateListener());
    }

    // visible for testing
    DistributedHerder(DistributedConfig config,
                      Worker worker,
                      String workerId,
                      String kafkaClusterId,
                      StatusBackingStore statusBackingStore,
                      ConfigBackingStore configBackingStore,
                      WorkerGroupMember member,
                      String restUrl,
                      RestClient restClient,
                      ConnectMetrics metrics,
                      Time time,
                      ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                      List<String> restNamespace,
                      ExecutorService forwardRequestExecutor,
                      AutoCloseable... uponShutdown) {
        super(worker, workerId, kafkaClusterId, statusBackingStore, configBackingStore, connectorClientConfigOverridePolicy);

        this.time = time;
        this.herderMetrics = new HerderMetrics(metrics);
        this.workerGroupId = config.getString(DistributedConfig.GROUP_ID_CONFIG);
        this.workerSyncTimeoutMs = config.getInt(DistributedConfig.WORKER_SYNC_TIMEOUT_MS_CONFIG);
        this.workerUnsyncBackoffMs = config.getInt(DistributedConfig.WORKER_UNSYNC_BACKOFF_MS_CONFIG);
        this.requestSignatureAlgorithm = config.getString(DistributedConfig.INTER_WORKER_SIGNATURE_ALGORITHM_CONFIG);
        this.keyRotationIntervalMs = config.getInt(DistributedConfig.INTER_WORKER_KEY_TTL_MS_CONFIG);
        this.keySignatureVerificationAlgorithms = config.getList(DistributedConfig.INTER_WORKER_VERIFICATION_ALGORITHMS_CONFIG);
        this.keyGenerator = config.getInternalRequestKeyGenerator();
        this.restClient = restClient;
        this.isTopicTrackingEnabled = config.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG);
        this.restNamespace = Objects.requireNonNull(restNamespace);
        this.uponShutdown = Arrays.asList(uponShutdown);

        String clientIdConfig = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        String clientId = clientIdConfig.length() <= 0 ? "connect-" + CONNECT_CLIENT_ID_SEQUENCE.getAndIncrement() : clientIdConfig;
        // Thread factory uses String.format and '%' is handled as a placeholder
        // need to escape if the client.id contains an actual % character
        String escapedClientIdForThreadNameFormat = clientId.replace("%", "%%");
        LogContext logContext = new LogContext("[Worker clientId=" + clientId + ", groupId=" + this.workerGroupId + "] ");
        log = logContext.logger(DistributedHerder.class);

        this.member = member != null
                      ? member
                      : new WorkerGroupMember(config, restUrl, this.configBackingStore,
                              new RebalanceListener(time), time, clientId, logContext);

        this.herderExecutor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(1),
                ThreadUtils.createThreadFactory(
                        this.getClass().getSimpleName() + "-" + escapedClientIdForThreadNameFormat + "-%d", false));

        this.forwardRequestExecutor = forwardRequestExecutor != null
                ? forwardRequestExecutor
                : Executors.newFixedThreadPool(
                        1,
                        ThreadUtils.createThreadFactory("ForwardRequestExecutor-" + escapedClientIdForThreadNameFormat + "-%d", false)
                );
        this.startAndStopExecutor = Executors.newFixedThreadPool(START_STOP_THREAD_POOL_SIZE,
                ThreadUtils.createThreadFactory(
                        "StartAndStopExecutor-" + escapedClientIdForThreadNameFormat + "-%d", false));
        this.config = config;

        stopping = new AtomicBoolean(false);
        configState = ClusterConfigState.EMPTY;
        rebalanceResolved = true; // If we still need to follow up after a rebalance occurred, starting up tasks
        needsReconfigRebalance = false;
        fencedFromConfigTopic = false;
        canReadConfigs = true; // We didn't try yet, but Configs are readable until proven otherwise
        scheduledRebalance = Long.MAX_VALUE;
        keyExpiration = Long.MAX_VALUE;
        sessionKey = null;
        backoffRetries = BACKOFF_RETRIES;

        currentProtocolVersion = ConnectProtocolCompatibility.compatibility(
            config.getString(DistributedConfig.CONNECT_PROTOCOL_CONFIG)
        ).protocolVersion();
        if (!internalRequestValidationEnabled(currentProtocolVersion)) {
            log.warn(
                "Internal request verification will be disabled for this cluster as this worker's {} configuration has been set to '{}'. "
                    + "If this is not intentional, either remove the '{}' configuration from the worker config file or change its value "
                    + "to '{}'. If this configuration is left as-is, the cluster will be insecure; for more information, see KIP-507: "
                    + "https://cwiki.apache.org/confluence/display/KAFKA/KIP-507%3A+Securing+Internal+Connect+REST+Endpoints",
                DistributedConfig.CONNECT_PROTOCOL_CONFIG,
                config.getString(DistributedConfig.CONNECT_PROTOCOL_CONFIG),
                DistributedConfig.CONNECT_PROTOCOL_CONFIG,
                ConnectProtocolCompatibility.SESSIONED.name()
            );
        }
    }

    @Override
    public void start() {
        this.herderExecutor.submit(this);
    }

    @Override
    public void run() {
        try {
            log.info("Herder starting");
            herderThread = Thread.currentThread();

            startServices();

            log.info("Herder started");
            running = true;

            while (!stopping.get()) {
                tick();
            }

            halt();

            log.info("Herder stopped");
            herderMetrics.close();
        } catch (Throwable t) {
            log.error("Uncaught exception in herder work thread, exiting: ", t);
            Exit.exit(1);
        } finally {
            running = false;
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
            if (!canReadConfigs) {
                if (readConfigToEnd(workerSyncTimeoutMs)) {
                    canReadConfigs = true;
                } else {
                    return; // Safe to return and tick immediately because readConfigToEnd will do the backoff for us
                }
            }

            log.debug("Ensuring group membership is still active");
            member.ensureActive();
            // Ensure we're in a good state in our group. If not restart and everything should be setup to rejoin
            if (!handleRebalanceCompleted()) return;
        } catch (WakeupException e) {
            // May be due to a request from another thread, or might be stopping. If the latter, we need to check the
            // flag immediately. If the former, we need to re-run the ensureActive call since we can't handle requests
            // unless we're in the group.
            log.trace("Woken up while ensure group membership is still active");
            return;
        }

        if (fencedFromConfigTopic) {
            if (isLeader()) {
                // We were accidentally fenced out, possibly by a zombie leader
                try {
                    log.debug("Reclaiming write privileges for config topic after being fenced out");
                    configBackingStore.claimWritePrivileges();
                    fencedFromConfigTopic = false;
                    log.debug("Successfully reclaimed write privileges for config topic after being fenced out");
                } catch (Exception e) {
                    log.warn("Unable to claim write privileges for config topic. Will backoff and possibly retry if still the leader", e);
                    backoff(CONFIG_TOPIC_WRITE_PRIVILEGES_BACKOFF_MS);
                    return;
                }
            } else {
                log.trace("Relinquished write privileges for config topic after being fenced out, since worker is no longer the leader of the cluster");
                // We were meant to be fenced out because we fell out of the group and a new leader was elected
                fencedFromConfigTopic = false;
            }
        }

        long now = time.milliseconds();

        if (checkForKeyRotation(now)) {
            log.debug("Distributing new session key");
            keyExpiration = Long.MAX_VALUE;
            try {
                SessionKey newSessionKey = new SessionKey(keyGenerator.generateKey(), now);
                writeToConfigTopicAsLeader(() -> configBackingStore.putSessionKey(newSessionKey));
            } catch (Exception e) {
                log.info("Failed to write new session key to config topic; forcing a read to the end of the config topic before possibly retrying", e);
                canReadConfigs = false;
                return;
            }
        }

        // Process any external requests
        // TODO: Some of these can be performed concurrently or even optimized away entirely.
        //       For example, if three different connectors are slated to be restarted, it's fine to
        //       restart all three at the same time instead.
        //       Another example: if multiple configurations are submitted for the same connector,
        //       the only one that actually has to be written to the config topic is the
        //       most-recently one.
        Long scheduledTick = null;
        while (true) {
            final DistributedHerderRequest next = peekWithoutException();
            if (next == null) {
                break;
            } else if (now >= next.at) {
                requests.pollFirst();
            } else {
                scheduledTick = next.at;
                break;
            }

            runRequest(next.action(), next.callback());
        }

        // Process all pending connector restart requests
        processRestartRequests();

        if (scheduledRebalance < Long.MAX_VALUE) {
            scheduledTick = scheduledTick != null ? Math.min(scheduledTick, scheduledRebalance) : scheduledRebalance;
            rebalanceResolved = false;
            log.debug("Scheduled rebalance at: {} (now: {} scheduledTick: {}) ",
                    scheduledRebalance, now, scheduledTick);
        }
        if (isLeader() && internalRequestValidationEnabled() && keyExpiration < Long.MAX_VALUE) {
            scheduledTick = scheduledTick != null ? Math.min(scheduledTick, keyExpiration) : keyExpiration;
            log.debug("Scheduled next key rotation at: {} (now: {} scheduledTick: {}) ",
                    keyExpiration, now, scheduledTick);
        }

        // Process any configuration updates
        AtomicReference<Set<String>> connectorConfigUpdatesCopy = new AtomicReference<>();
        AtomicReference<Set<String>> connectorTargetStateChangesCopy = new AtomicReference<>();
        AtomicReference<Set<ConnectorTaskId>> taskConfigUpdatesCopy = new AtomicReference<>();

        boolean shouldReturn;
        if (member.currentProtocolVersion() == CONNECT_PROTOCOL_V0) {
            shouldReturn = updateConfigsWithEager(connectorConfigUpdatesCopy,
                    connectorTargetStateChangesCopy);
            // With eager protocol we should return immediately if needsReconfigRebalance has
            // been set to retain the old workflow
            if (shouldReturn) {
                return;
            }

            if (connectorConfigUpdatesCopy.get() != null) {
                processConnectorConfigUpdates(connectorConfigUpdatesCopy.get());
            }

            if (connectorTargetStateChangesCopy.get() != null) {
                processTargetStateChanges(connectorTargetStateChangesCopy.get());
            }
        } else {
            shouldReturn = updateConfigsWithIncrementalCooperative(connectorConfigUpdatesCopy,
                    connectorTargetStateChangesCopy, taskConfigUpdatesCopy);

            if (connectorConfigUpdatesCopy.get() != null) {
                processConnectorConfigUpdates(connectorConfigUpdatesCopy.get());
            }

            if (connectorTargetStateChangesCopy.get() != null) {
                processTargetStateChanges(connectorTargetStateChangesCopy.get());
            }

            if (taskConfigUpdatesCopy.get() != null) {
                processTaskConfigUpdatesWithIncrementalCooperative(taskConfigUpdatesCopy.get());
            }

            if (shouldReturn) {
                return;
            }
        }

        // Let the group take any actions it needs to
        try {
            long nextRequestTimeoutMs = scheduledTick != null ? Math.max(scheduledTick - time.milliseconds(), 0L) : Long.MAX_VALUE;
            log.trace("Polling for group activity; will wait for {}ms or until poll is interrupted by "
                    + "either config backing store updates or a new external request",
                    nextRequestTimeoutMs);
            member.poll(nextRequestTimeoutMs);
            // Ensure we're in a good state in our group. If not restart and everything should be setup to rejoin
            handleRebalanceCompleted();
        } catch (WakeupException e) { // FIXME should not be WakeupException
            log.trace("Woken up while polling for group activity");
            // Ignore. Just indicates we need to check the exit flag, for requested actions, etc.
        }
    }

    private boolean checkForKeyRotation(long now) {
        SecretKey key;
        long expiration;
        synchronized (this) {
            // This happens on startup; the snapshot contains the session key,
            // but no callback in the config update listener has been fired for it yet.
            if (sessionKey == null && configState.sessionKey() != null) {
                sessionKey = configState.sessionKey().key();
                keyExpiration = configState.sessionKey().creationTimestamp() + keyRotationIntervalMs;
            }
            key = sessionKey;
            expiration = keyExpiration;
        }

        if (internalRequestValidationEnabled()) {
            if (isLeader()) {
                if (key == null) {
                    log.debug("Internal request signing is enabled but no session key has been distributed yet. "
                        + "Distributing new key now.");
                    return true;
                } else if (expiration <= now) {
                    log.debug("Existing key has expired. Distributing new key now.");
                    return true;
                } else if (!key.getAlgorithm().equals(keyGenerator.getAlgorithm())
                        || key.getEncoded().length != keyGenerator.generateKey().getEncoded().length) {
                    log.debug("Previously-distributed key uses different algorithm/key size "
                        + "than required by current worker configuration. Distributing new key now.");
                    return true;
                }
            }
        }
        return false;
    }

    private synchronized boolean updateConfigsWithEager(AtomicReference<Set<String>> connectorConfigUpdatesCopy,
                                                        AtomicReference<Set<String>> connectorTargetStateChangesCopy) {
        // This branch is here to avoid creating a snapshot if not needed
        if (needsReconfigRebalance
                || !connectorConfigUpdates.isEmpty()
                || !connectorTargetStateChanges.isEmpty()) {
            log.trace("Handling config updates with eager rebalancing");
            // Connector reconfigs only need local updates since there is no coordination between workers required.
            // However, if connectors were added or removed, work needs to be rebalanced since we have more work
            // items to distribute among workers.
            configState = configBackingStore.snapshot();

            if (needsReconfigRebalance) {
                // Task reconfigs require a rebalance. Request the rebalance, clean out state, and then restart
                // this loop, which will then ensure the rebalance occurs without any other requests being
                // processed until it completes.
                log.debug("Requesting rebalance due to reconfiguration of tasks (needsReconfigRebalance: {})",
                        needsReconfigRebalance);
                member.requestRejoin();
                needsReconfigRebalance = false;
                // Any connector config updates or target state changes will be addressed during the rebalance too
                connectorConfigUpdates.clear();
                connectorTargetStateChanges.clear();
                return true;
            } else {
                if (!connectorConfigUpdates.isEmpty()) {
                    // We can't start/stop while locked since starting connectors can cause task updates that will
                    // require writing configs, which in turn make callbacks into this class from another thread that
                    // require acquiring a lock. This leads to deadlock. Instead, just copy the info we need and process
                    // the updates after unlocking.
                    connectorConfigUpdatesCopy.set(connectorConfigUpdates);
                    connectorConfigUpdates = new HashSet<>();
                }

                if (!connectorTargetStateChanges.isEmpty()) {
                    // Similarly for target state changes which can cause connectors to be restarted
                    connectorTargetStateChangesCopy.set(connectorTargetStateChanges);
                    connectorTargetStateChanges = new HashSet<>();
                }
            }
        } else {
            log.trace("Skipping config updates with eager rebalancing "
                + "since no config rebalance is required "
                + "and there are no connector config, task config, or target state changes pending");
        }
        return false;
    }

    private synchronized boolean updateConfigsWithIncrementalCooperative(AtomicReference<Set<String>> connectorConfigUpdatesCopy,
                                                                         AtomicReference<Set<String>> connectorTargetStateChangesCopy,
                                                                         AtomicReference<Set<ConnectorTaskId>> taskConfigUpdatesCopy) {
        boolean retValue = false;
        // This branch is here to avoid creating a snapshot if not needed
        if (needsReconfigRebalance
                || !connectorConfigUpdates.isEmpty()
                || !connectorTargetStateChanges.isEmpty()
                || !taskConfigUpdates.isEmpty()) {
            log.trace("Handling config updates with incremental cooperative rebalancing");
            // Connector reconfigs only need local updates since there is no coordination between workers required.
            // However, if connectors were added or removed, work needs to be rebalanced since we have more work
            // items to distribute among workers.
            configState = configBackingStore.snapshot();

            if (needsReconfigRebalance) {
                log.debug("Requesting rebalance due to reconfiguration of tasks (needsReconfigRebalance: {})",
                        needsReconfigRebalance);
                member.requestRejoin();
                needsReconfigRebalance = false;
                retValue = true;
            }

            if (!connectorConfigUpdates.isEmpty()) {
                // We can't start/stop while locked since starting connectors can cause task updates that will
                // require writing configs, which in turn make callbacks into this class from another thread that
                // require acquiring a lock. This leads to deadlock. Instead, just copy the info we need and process
                // the updates after unlocking.
                connectorConfigUpdatesCopy.set(connectorConfigUpdates);
                connectorConfigUpdates = new HashSet<>();
            }

            if (!connectorTargetStateChanges.isEmpty()) {
                // Similarly for target state changes which can cause connectors to be restarted
                connectorTargetStateChangesCopy.set(connectorTargetStateChanges);
                connectorTargetStateChanges = new HashSet<>();
            }

            if (!taskConfigUpdates.isEmpty()) {
                // Similarly for task config updates
                taskConfigUpdatesCopy.set(taskConfigUpdates);
                taskConfigUpdates = new HashSet<>();
            }
        } else {
            log.trace("Skipping config updates with incremental cooperative rebalancing " 
                + "since no config rebalance is required " 
                + "and there are no connector config, task config, or target state changes pending");
        }
        return retValue;
    }

    private void processConnectorConfigUpdates(Set<String> connectorConfigUpdates) {
        // If we only have connector config updates, we can just bounce the updated connectors that are
        // currently assigned to this worker.
        Set<String> localConnectors = assignment == null ? Collections.emptySet() : new HashSet<>(assignment.connectors());
        Collection<Callable<Void>> connectorsToStart = new ArrayList<>();
        log.trace("Processing connector config updates; "
                + "currently-owned connectors are {}, and to-be-updated connectors are {}",
                localConnectors,
                connectorConfigUpdates);
        for (String connectorName : connectorConfigUpdates) {
            if (!localConnectors.contains(connectorName)) {
                log.trace("Skipping config update for connector {} as it is not owned by this worker",
                        connectorName);
                continue;
            }
            boolean remains = configState.contains(connectorName);
            log.info("Handling connector-only config update by {} connector {}",
                    remains ? "restarting" : "stopping", connectorName);
            worker.stopAndAwaitConnector(connectorName);
            // The update may be a deletion, so verify we actually need to restart the connector
            if (remains) {
                connectorsToStart.add(getConnectorStartingCallable(connectorName));
            }
        }
        startAndStop(connectorsToStart);
    }

    private void processTargetStateChanges(Set<String> connectorTargetStateChanges) {
        log.trace("Processing target state updates; "
                + "currently-known connectors are {}, and to-be-updated connectors are {}",
                configState.connectors(), connectorTargetStateChanges);
        for (String connector : connectorTargetStateChanges) {
            TargetState targetState = configState.targetState(connector);
            if (!configState.connectors().contains(connector)) {
                log.debug("Received target state change for unknown connector: {}", connector);
                continue;
            }

            // we must propagate the state change to the worker so that the connector's
            // tasks can transition to the new target state
            worker.setTargetState(connector, targetState, (error, newState) -> {
                if (error != null) {
                    log.error("Failed to transition connector to target state", error);
                    return;
                }
                // additionally, if the worker is running the connector itself, then we need to
                // request reconfiguration to ensure that config changes while paused take effect
                if (newState == TargetState.STARTED) {
                    requestTaskReconfiguration(connector);
                }
            });
        }
    }

    private void processTaskConfigUpdatesWithIncrementalCooperative(Set<ConnectorTaskId> taskConfigUpdates) {
        Set<ConnectorTaskId> localTasks = assignment == null
                                          ? Collections.emptySet()
                                          : new HashSet<>(assignment.tasks());
        log.trace("Processing task config updates with incremental cooperative rebalance protocol; "
                + "currently-owned tasks are {}, and to-be-updated tasks are {}",
                localTasks, taskConfigUpdates);
        Set<String> connectorsWhoseTasksToStop = taskConfigUpdates.stream()
                .map(ConnectorTaskId::connector).collect(Collectors.toSet());
        stopReconfiguredTasks(connectorsWhoseTasksToStop);
    }

    private void stopReconfiguredTasks(Set<String> connectors) {
        Set<ConnectorTaskId> localTasks = assignment == null
                ? Collections.emptySet()
                : new HashSet<>(assignment.tasks());

        List<ConnectorTaskId> tasksToStop = localTasks.stream()
                .filter(taskId -> connectors.contains(taskId.connector()))
                .collect(Collectors.toList());

        if (tasksToStop.isEmpty()) {
            // The rest of the method would essentially be a no-op so this isn't strictly necessary,
            // but it prevents an unnecessary log message from being emitted
            return;
        }

        log.info("Handling task config update by stopping tasks {}, which will be restarted after rebalance if still assigned to this worker", tasksToStop);
        worker.stopAndAwaitTasks(tasksToStop);
        tasksToRestart.addAll(tasksToStop);
    }

    // public for testing
    public void halt() {
        synchronized (this) {
            // Clean up any connectors and tasks that are still running.
            log.info("Stopping connectors and tasks that are still assigned to this worker.");
            worker.stopAndAwaitConnectors();
            worker.stopAndAwaitTasks();

            member.stop();

            // Explicitly fail any outstanding requests so they actually get a response and get an
            // understandable reason for their failure.
            DistributedHerderRequest request = requests.pollFirst();
            while (request != null) {
                request.callback().onCompletion(new ConnectException("Worker is shutting down"), null);
                request = requests.pollFirst();
            }

            stopServices();
        }
    }

    @Override
    protected void stopServices() {
        try {
            super.stopServices();
        } finally {
            this.uponShutdown.forEach(closeable -> Utils.closeQuietly(closeable, closeable != null ? closeable.toString() : "<unknown>"));
        }
    }

    // Timeout for herderExecutor to gracefully terminate is set to a value to accommodate
    // reading to the end of the config topic + successfully attempting to stop all connectors and tasks and a buffer of 10s
    private long herderExecutorTimeoutMs() {
        return this.workerSyncTimeoutMs +
                config.getLong(DistributedConfig.TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG) +
                Worker.CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS + 10000;
    }

    @Override
    public void stop() {
        log.info("Herder stopping");

        stopping.set(true);
        member.wakeup();
        ThreadUtils.shutdownExecutorServiceQuietly(herderExecutor, herderExecutorTimeoutMs(), TimeUnit.MILLISECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(forwardRequestExecutor, FORWARD_REQUEST_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(startAndStopExecutor, START_AND_STOP_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        log.info("Herder stopped");
        running = false;
    }

    @Override
    public void connectors(final Callback<Collection<String>> callback) {
        log.trace("Submitting connector listing request");

        addRequest(
            () -> {
                if (!checkRebalanceNeeded(callback))
                    callback.onCompletion(null, configState.connectors());
                return null;
            },
            forwardErrorCallback(callback)
        );
    }

    @Override
    public void connectorInfo(final String connName, final Callback<ConnectorInfo> callback) {
        log.trace("Submitting connector info request {}", connName);

        addRequest(
            () -> {
                if (checkRebalanceNeeded(callback))
                    return null;

                if (!configState.contains(connName)) {
                    callback.onCompletion(
                        new NotFoundException("Connector " + connName + " not found"), null);
                } else {
                    callback.onCompletion(null, connectorInfo(connName));
                }
                return null;
            },
            forwardErrorCallback(callback)
        );
    }

    @Override
    public void tasksConfig(String connName, final Callback<Map<ConnectorTaskId, Map<String, String>>> callback) {
        log.trace("Submitting tasks config request {}", connName);

        addRequest(
            () -> {
                if (checkRebalanceNeeded(callback))
                    return null;

                if (!configState.contains(connName)) {
                    callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                } else {
                    callback.onCompletion(null, buildTasksConfig(connName));
                }
                return null;
            },
            forwardErrorCallback(callback)
        );
    }

    @Override
    protected Map<String, String> rawConfig(String connName) {
        return configState.rawConnectorConfig(connName);
    }

    @Override
    public void connectorConfig(String connName, final Callback<Map<String, String>> callback) {
        log.trace("Submitting connector config read request {}", connName);
        super.connectorConfig(connName, callback);
    }

    @Override
    public void deleteConnectorConfig(final String connName, final Callback<Created<ConnectorInfo>> callback) {
        addRequest(
            () -> {
                log.trace("Handling connector config request {}", connName);
                if (!isLeader()) {
                    callback.onCompletion(new NotLeaderException("Only the leader can delete connector configs.", leaderUrl()), null);
                    return null;
                }

                if (!configState.contains(connName)) {
                    callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                } else {
                    log.trace("Removing connector config {} {}", connName, configState.connectors());
                    writeToConfigTopicAsLeader(() -> configBackingStore.removeConnectorConfig(connName));
                    callback.onCompletion(null, new Created<>(false, null));
                }
                return null;
            },
            forwardErrorCallback(callback)
        );
    }

    @Override
    protected Map<String, ConfigValue> validateSinkConnectorConfig(SinkConnector connector, ConfigDef configDef, Map<String, String> config) {
        Map<String, ConfigValue> result = super.validateSinkConnectorConfig(connector, configDef, config);
        validateSinkConnectorGroupId(result);
        return result;
    }

    @Override
    protected Map<String, ConfigValue> validateSourceConnectorConfig(SourceConnector connector, ConfigDef configDef, Map<String, String> config) {
        Map<String, ConfigValue> result = super.validateSourceConnectorConfig(connector, configDef, config);
        validateSourceConnectorExactlyOnceSupport(config, result, connector);
        validateSourceConnectorTransactionBoundary(config, result, connector);
        return result;
    }


    private void validateSinkConnectorGroupId(Map<String, ConfigValue> validatedConfig) {
        ConfigValue validatedName = validatedConfig.get(ConnectorConfig.NAME_CONFIG);
        String name = (String) validatedName.value();
        if (workerGroupId.equals(SinkUtils.consumerGroupId(name))) {
            validatedName.addErrorMessage("Consumer group for sink connector named " + name +
                    " conflicts with Connect worker group " + workerGroupId);
        }
    }

    private void validateSourceConnectorExactlyOnceSupport(
            Map<String, String> rawConfig,
            Map<String, ConfigValue> validatedConfig,
            SourceConnector connector) {
        ConfigValue validatedExactlyOnceSupport = validatedConfig.get(SourceConnectorConfig.EXACTLY_ONCE_SUPPORT_CONFIG);
        if (validatedExactlyOnceSupport.errorMessages().isEmpty()) {
            // Should be safe to parse the enum from the user-provided value since it's passed validation so far
            SourceConnectorConfig.ExactlyOnceSupportLevel exactlyOnceSupportLevel =
                    SourceConnectorConfig.ExactlyOnceSupportLevel.fromProperty(Objects.toString(validatedExactlyOnceSupport.value()));
            if (SourceConnectorConfig.ExactlyOnceSupportLevel.REQUIRED.equals(exactlyOnceSupportLevel)) {
                if (!config.exactlyOnceSourceEnabled()) {
                    validatedExactlyOnceSupport.addErrorMessage("This worker does not have exactly-once source support enabled.");
                }

                try {
                    ExactlyOnceSupport exactlyOnceSupport = connector.exactlyOnceSupport(rawConfig);
                    if (!ExactlyOnceSupport.SUPPORTED.equals(exactlyOnceSupport)) {
                        final String validationErrorMessage;
                        // Would do a switch here but that doesn't permit matching on null values
                        if (exactlyOnceSupport == null) {
                            validationErrorMessage = "The connector does not implement the API required for preflight validation of exactly-once "
                                    + "source support. Please consult the documentation for the connector to determine whether it supports exactly-once "
                                    + "semantics, and then consider reconfiguring the connector to use the value \""
                                    + SourceConnectorConfig.ExactlyOnceSupportLevel.REQUESTED
                                    + "\" for this property (which will disable this preflight check and allow the connector to be created).";
                        } else if (ExactlyOnceSupport.UNSUPPORTED.equals(exactlyOnceSupport)) {
                            validationErrorMessage = "The connector does not support exactly-once semantics with the provided configuration.";
                        } else {
                            throw new ConnectException("Unexpected value returned from SourceConnector::exactlyOnceSupport: " + exactlyOnceSupport);
                        }
                        validatedExactlyOnceSupport.addErrorMessage(validationErrorMessage);
                    }
                } catch (Exception e) {
                    log.error("Failed while validating connector support for exactly-once semantics", e);
                    String validationErrorMessage = "An unexpected error occurred during validation";
                    String failureMessage = e.getMessage();
                    if (failureMessage != null && !failureMessage.trim().isEmpty()) {
                        validationErrorMessage += ": " + failureMessage.trim();
                    } else {
                        validationErrorMessage += "; please see the worker logs for more details.";
                    }
                    validatedExactlyOnceSupport.addErrorMessage(validationErrorMessage);
                }
            }
        }
    }

    private void validateSourceConnectorTransactionBoundary(
            Map<String, String> rawConfig,
            Map<String, ConfigValue> validatedConfig,
            SourceConnector connector) {
        ConfigValue validatedTransactionBoundary = validatedConfig.get(SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG);
        if (validatedTransactionBoundary.errorMessages().isEmpty()) {
            // Should be safe to parse the enum from the user-provided value since it's passed validation so far
            SourceTask.TransactionBoundary transactionBoundary =
                    SourceTask.TransactionBoundary.fromProperty(Objects.toString(validatedTransactionBoundary.value()));
            if (SourceTask.TransactionBoundary.CONNECTOR.equals(transactionBoundary)) {
                try {
                    ConnectorTransactionBoundaries connectorTransactionSupport = connector.canDefineTransactionBoundaries(rawConfig);
                    if (connectorTransactionSupport == null) {
                        validatedTransactionBoundary.addErrorMessage(
                                "This connector has returned a null value from its canDefineTransactionBoundaries method, which is not permitted. " +
                                        "The connector will be treated as if it cannot define its own transaction boundaries, and cannot be configured with " +
                                        "'" + SourceConnectorConfig.TRANSACTION_BOUNDARY_CONFIG + "' set to '" + SourceTask.TransactionBoundary.CONNECTOR + "'."
                        );
                    } else if (!ConnectorTransactionBoundaries.SUPPORTED.equals(connectorTransactionSupport)) {
                        validatedTransactionBoundary.addErrorMessage(
                                "The connector does not support connector-defined transaction boundaries with the given configuration. "
                                        + "Please reconfigure it to use a different transaction boundary definition.");
                    }
                } catch (Exception e) {
                    log.error("Failed while validating connector support for defining its own transaction boundaries", e);
                    String validationErrorMessage = "An unexpected error occurred during validation";
                    String failureMessage = e.getMessage();
                    if (failureMessage != null && !failureMessage.trim().isEmpty()) {
                        validationErrorMessage += ": " + failureMessage.trim();
                    } else {
                        validationErrorMessage += "; please see the worker logs for more details.";
                    }
                    validatedTransactionBoundary.addErrorMessage(validationErrorMessage);
                }
            }
        }
    }

    @Override
    protected boolean connectorUsesAdmin(org.apache.kafka.connect.health.ConnectorType connectorType, Map<String, String> connProps) {
        return super.connectorUsesAdmin(connectorType, connProps)
                || connectorUsesSeparateOffsetsTopicClients(connectorType, connProps);
    }

    @Override
    protected boolean connectorUsesConsumer(org.apache.kafka.connect.health.ConnectorType connectorType, Map<String, String> connProps) {
        return super.connectorUsesConsumer(connectorType, connProps)
                || connectorUsesSeparateOffsetsTopicClients(connectorType, connProps);
    }

    private boolean connectorUsesSeparateOffsetsTopicClients(org.apache.kafka.connect.health.ConnectorType connectorType, Map<String, String> connProps) {
        if (connectorType != org.apache.kafka.connect.health.ConnectorType.SOURCE) {
            return false;
        }
        return config.exactlyOnceSourceEnabled()
                || !connProps.getOrDefault(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "").trim().isEmpty();
    }

    @Override
    public void putConnectorConfig(final String connName, final Map<String, String> config, final boolean allowReplace,
                                   final Callback<Created<ConnectorInfo>> callback) {
        log.trace("Submitting connector config write request {}", connName);
        addRequest(
            () -> {
                validateConnectorConfig(config, (error, configInfos) -> {
                    if (error != null) {
                        callback.onCompletion(error, null);
                        return;
                    }

                    // Complete the connector config write via another herder request in order to
                    // perform the write to the backing store (or forward to the leader) during
                    // the "external request" portion of the tick loop
                    addRequest(
                        () -> {
                            if (maybeAddConfigErrors(configInfos, callback)) {
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
                            writeToConfigTopicAsLeader(() -> configBackingStore.putConnectorConfig(connName, config));

                            // Note that we use the updated connector config despite the fact that we don't have an updated
                            // snapshot yet. The existing task info should still be accurate.
                            ConnectorInfo info = new ConnectorInfo(connName, config, configState.tasks(connName),
                                connectorType(config));
                            callback.onCompletion(null, new Created<>(!exists, info));
                            return null;
                        },
                        forwardErrorCallback(callback)
                    );
                });
                return null;
            },
            forwardErrorCallback(callback)
        );
    }

    @Override
    public void stopConnector(final String connName, final Callback<Void> callback) {
        log.trace("Submitting request to transition connector {} to STOPPED state", connName);

        addRequest(
                () -> {
                    if (!configState.contains(connName))
                        throw new NotFoundException("Unknown connector " + connName);

                    // We only allow the leader to handle this request since it involves writing task configs to the config topic
                    if (!isLeader()) {
                        callback.onCompletion(new NotLeaderException("Only the leader can transition connectors to the STOPPED state.", leaderUrl()), null);
                        return null;
                    }

                    // We write the task configs first since, if we fail between then and writing the target state, the
                    // cluster is still kept in a healthy state. A RUNNING connector with zero tasks is acceptable (although,
                    // if the connector is reassigned during the ensuing rebalance, it is likely that it will immediately generate
                    // a non-empty set of task configs). A STOPPED connector with a non-empty set of tasks is less acceptable
                    // and likely to confuse users.
                    writeTaskConfigs(connName, Collections.emptyList());
                    configBackingStore.putTargetState(connName, TargetState.STOPPED);
                    // Force a read of the new target state for the connector
                    refreshConfigSnapshot(workerSyncTimeoutMs);

                    callback.onCompletion(null, null);
                    return null;
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    public void requestTaskReconfiguration(final String connName) {
        log.trace("Submitting connector task reconfiguration request {}", connName);

        addRequest(
            () -> {
                reconfigureConnectorTasksWithRetry(time.milliseconds(), connName);
                return null;
            },
            (error, result) -> {
                if (error != null) {
                    log.error("Unexpected error during task reconfiguration: ", error);
                    log.error("Task reconfiguration for {} failed unexpectedly, this connector will not be properly reconfigured unless manually triggered.", connName);
                }
            }
        );
    }

    @Override
    public void taskConfigs(final String connName, final Callback<List<TaskInfo>> callback) {
        log.trace("Submitting get task configuration request {}", connName);

        addRequest(
            () -> {
                if (checkRebalanceNeeded(callback))
                    return null;

                if (!configState.contains(connName)) {
                    callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                } else {
                    List<TaskInfo> result = new ArrayList<>();
                    for (int i = 0; i < configState.taskCount(connName); i++) {
                        ConnectorTaskId id = new ConnectorTaskId(connName, i);
                        result.add(new TaskInfo(id, configState.rawTaskConfig(id)));
                    }
                    callback.onCompletion(null, result);
                }
                return null;
            },
            forwardErrorCallback(callback)
        );
    }

    @Override
    public void putTaskConfigs(final String connName, final List<Map<String, String>> configs, final Callback<Void> callback, InternalRequestSignature requestSignature) {
        log.trace("Submitting put task configuration request {}", connName);
        if (requestNotSignedProperly(requestSignature, callback)) {
            return;
        }

        addRequest(
            () -> {
                if (!isLeader())
                    callback.onCompletion(new NotLeaderException("Only the leader may write task configurations.", leaderUrl()), null);
                else if (!configState.contains(connName))
                    callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                else {
                    writeTaskConfigs(connName, configs);
                    callback.onCompletion(null, null);
                }
                return null;
            },
            forwardErrorCallback(callback)
        );
    }

    // Another worker has forwarded a request to this worker (which it believes is the leader) to perform a round of zombie fencing
    @Override
    public void fenceZombieSourceTasks(final String connName, final Callback<Void> callback, InternalRequestSignature requestSignature) {
        log.trace("Submitting zombie fencing request {}", connName);
        if (requestNotSignedProperly(requestSignature, callback)) {
            return;
        }

        fenceZombieSourceTasks(connName, callback);
    }

    // A task on this worker requires a round of zombie fencing
    void fenceZombieSourceTasks(final ConnectorTaskId id, Callback<Void> callback) {
        log.trace("Performing preflight zombie check for task {}", id);
        fenceZombieSourceTasks(id.connector(), (error, ignored) -> {
            if (error == null) {
                callback.onCompletion(null, null);
            } else if (error instanceof NotLeaderException) {
                if (restClient != null) {
                    String workerUrl = ((NotLeaderException) error).forwardUrl();
                    String fenceUrl = namespacedUrl(workerUrl)
                            .path("connectors")
                            .path(id.connector())
                            .path("fence")
                            .build()
                            .toString();
                    log.trace("Forwarding zombie fencing request for connector {} to leader at {}", id.connector(), fenceUrl);
                    forwardRequestExecutor.execute(() -> {
                        try {
                            restClient.httpRequest(fenceUrl, "PUT", null, null, null, sessionKey, requestSignatureAlgorithm);
                            callback.onCompletion(null, null);
                        } catch (Throwable t) {
                            callback.onCompletion(t, null);
                        }
                    });
                } else {
                    callback.onCompletion(
                            new ConnectException(
                                    "This worker is not able to communicate with the leader of the cluster, "
                                            + "which is required for exactly-once source tasks. If running MirrorMaker 2 "
                                            + "in dedicated mode, consider enabling inter-worker communication via the "
                                            + "'dedicated.mode.enable.internal.rest' property."
                            ),
                            null
                    );
                }
            } else {
                error = ConnectUtils.maybeWrap(error, "Failed to perform zombie fencing");
                callback.onCompletion(error, null);
            }
        });
    }

    // Visible for testing
    void fenceZombieSourceTasks(final String connName, final Callback<Void> callback) {
        addRequest(
                () -> {
                    log.trace("Performing zombie fencing request for connector {}", connName);
                    if (!isLeader())
                        callback.onCompletion(new NotLeaderException("Only the leader may perform zombie fencing.", leaderUrl()), null);
                    else if (!configState.contains(connName))
                        callback.onCompletion(new NotFoundException("Connector " + connName + " not found"), null);
                    else if (!isSourceConnector(connName))
                        callback.onCompletion(new BadRequestException("Connector " + connName + " is not a source connector"), null);
                    else {
                        if (!refreshConfigSnapshot(workerSyncTimeoutMs)) {
                            throw new ConnectException("Failed to read to end of config topic before performing zombie fencing");
                        }

                        int taskCount = configState.taskCount(connName);
                        Integer taskCountRecord = configState.taskCountRecord(connName);

                        ZombieFencing zombieFencing = null;
                        boolean newFencing = false;
                        synchronized (DistributedHerder.this) {
                            // Check first to see if we have to do a fencing. The control flow is a little awkward here (why not stick this in
                            // an else block lower down?) but we can't synchronize around the body below since that may contain a synchronous
                            // write to the config topic.
                            if (configState.pendingFencing(connName) && taskCountRecord != null
                                    && (taskCountRecord != 1 || taskCount != 1)) {
                                int taskGen = configState.taskConfigGeneration(connName);
                                zombieFencing = activeZombieFencings.get(connName);
                                if (zombieFencing == null) {
                                    zombieFencing = new ZombieFencing(connName, taskCountRecord, taskCount, taskGen);
                                    activeZombieFencings.put(connName, zombieFencing);
                                    newFencing = true;
                                }
                            }
                        }
                        if (zombieFencing != null) {
                            if (newFencing) {
                                zombieFencing.start();
                            }
                            zombieFencing.addCallback(callback);
                            return null;
                        }

                        if (!configState.pendingFencing(connName)) {
                            // If the latest task count record for the connector is present after the latest set of task configs, there's no need to
                            // do any zombie fencing or write a new task count record to the config topic
                            log.debug("Skipping zombie fencing round for connector {} as all old task generations have already been fenced out", connName);
                        } else {
                            if (taskCountRecord == null) {
                                // If there is no task count record present for the connector, no transactional producers should have been brought up for it,
                                // so there's nothing to fence--but we do need to write a task count record now so that we know to fence those tasks if/when
                                // the connector is reconfigured
                                log.debug("Skipping zombie fencing round but writing task count record for connector {} "
                                        + "as it is being brought up for the first time with exactly-once source support", connName);
                            } else {
                                // If the last generation of tasks only had one task, and the next generation only has one, then the new task will automatically
                                // fence out the older task if it's still running; no need to fence here, but again, we still need to write a task count record
                                log.debug("Skipping zombie fencing round but writing task count record for connector {} "
                                        + "as both the most recent and the current generation of task configs only contain one task", connName);
                            }
                            writeToConfigTopicAsLeader(() -> configBackingStore.putTaskCountRecord(connName, taskCount));
                        }
                        callback.onCompletion(null, null);
                        return null;
                    }
                    return null;
                },
                forwardErrorCallback(callback)
        );
    }

    @Override
    public void restartConnector(final String connName, final Callback<Void> callback) {
        restartConnector(0, connName, callback);
    }

    @Override
    public HerderRequest restartConnector(final long delayMs, final String connName, final Callback<Void> callback) {
        return addRequest(
            delayMs,
            () -> {
                if (checkRebalanceNeeded(callback))
                    return null;

                if (!configState.connectors().contains(connName)) {
                    callback.onCompletion(new NotFoundException("Unknown connector: " + connName), null);
                    return null;
                }

                if (assignment.connectors().contains(connName)) {
                    try {
                        worker.stopAndAwaitConnector(connName);
                        startConnector(connName, callback);
                    } catch (Throwable t) {
                        callback.onCompletion(t, null);
                    }
                } else if (isLeader()) {
                    callback.onCompletion(new NotAssignedException("Cannot restart connector since it is not assigned to this member", member.ownerUrl(connName)), null);
                } else {
                    callback.onCompletion(new NotLeaderException("Only the leader can process restart requests.", leaderUrl()), null);
                }
                return null;
            },
            forwardErrorCallback(callback));
    }

    @Override
    public void restartTask(final ConnectorTaskId id, final Callback<Void> callback) {
        addRequest(
            () -> {
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
            },
            forwardErrorCallback(callback));
    }

    @Override
    public int generation() {
        return generation;
    }

    @Override
    public void restartConnectorAndTasks(RestartRequest request, Callback<ConnectorStateInfo> callback) {
        final String connectorName = request.connectorName();
        addRequest(
                () -> {
                    if (checkRebalanceNeeded(callback)) {
                        return null;
                    }
                    if (!configState.connectors().contains(request.connectorName())) {
                        callback.onCompletion(new NotFoundException("Unknown connector: " + connectorName), null);
                        return null;
                    }
                    if (isLeader()) {
                        // Write a restart request to the config backing store, to be executed asynchronously in tick()
                        configBackingStore.putRestartRequest(request);
                        // Compute and send the response that this was accepted
                        Optional<RestartPlan> plan = buildRestartPlan(request);
                        if (!plan.isPresent()) {
                            callback.onCompletion(new NotFoundException("Status for connector " + connectorName + " not found", null), null);
                        } else {
                            callback.onCompletion(null, plan.get().restartConnectorStateInfo());
                        }
                    } else {
                        callback.onCompletion(new NotLeaderException("Only the leader can process restart requests.", leaderUrl()), null);
                    }
                    return null;
                },
                forwardErrorCallback(callback)
        );
    }

    /**
     * Process all pending restart requests. There can be at most one request per connector.
     *
     * <p>This method is called from within the {@link #tick()} method.
     */
    void processRestartRequests() {
        List<RestartRequest> restartRequests;
        synchronized (this) {
            if (pendingRestartRequests.isEmpty()) {
                return;
            }
            //dequeue into a local list to minimize the work being done within the synchronized block
            restartRequests = new ArrayList<>(pendingRestartRequests.values());
            pendingRestartRequests.clear();
        }
        restartRequests.forEach(restartRequest -> {
            try {
                doRestartConnectorAndTasks(restartRequest);
            } catch (Exception e) {
                log.warn("Unexpected error while trying to process " + restartRequest + ", the restart request will be skipped.", e);
            }
        });
    }

    /**
     * Builds and executes a restart plan for the connector and its tasks from <code>request</code>.
     * Execution of a plan involves triggering the stop of eligible connector/tasks and then queuing the start for eligible connector/tasks.
     *
     * @param request the request to restart connector and tasks
     */
    protected synchronized void doRestartConnectorAndTasks(RestartRequest request) {
        String connectorName = request.connectorName();
        Optional<RestartPlan> maybePlan = buildRestartPlan(request);
        if (!maybePlan.isPresent()) {
            log.debug("Skipping restart of connector '{}' since no status is available: {}", connectorName, request);
            return;
        }
        RestartPlan plan = maybePlan.get();
        log.info("Executing {}", plan);

        // If requested, stop the connector and any tasks, marking each as restarting
        final ExtendedAssignment currentAssignments = assignment;
        final Collection<ConnectorTaskId> assignedIdsToRestart = plan.taskIdsToRestart()
                .stream()
                .filter(taskId -> currentAssignments.tasks().contains(taskId))
                .collect(Collectors.toList());
        final boolean restartConnector = plan.shouldRestartConnector() && currentAssignments.connectors().contains(connectorName);
        final boolean restartTasks = !assignedIdsToRestart.isEmpty();
        if (restartConnector) {
            worker.stopAndAwaitConnector(connectorName);
            onRestart(connectorName);
        }
        if (restartTasks) {
            // Stop the tasks and mark as restarting
            worker.stopAndAwaitTasks(assignedIdsToRestart);
            assignedIdsToRestart.forEach(this::onRestart);
        }

        // Now restart the connector and tasks
        if (restartConnector) {
            try {
                startConnector(connectorName, (error, targetState) -> {
                    if (error == null) {
                        log.info("Connector '{}' restart successful", connectorName);
                    } else {
                        log.error("Connector '{}' restart failed", connectorName, error);
                    }
                });
            } catch (Throwable t) {
                log.error("Connector '{}' restart failed", connectorName, t);
            }
        }
        if (restartTasks) {
            log.debug("Restarting {} of {} tasks for {}", assignedIdsToRestart.size(), plan.totalTaskCount(), request);
            assignedIdsToRestart.forEach(taskId -> {
                try {
                    if (startTask(taskId)) {
                        log.info("Task '{}' restart successful", taskId);
                    } else {
                        log.error("Task '{}' restart failed", taskId);
                    }
                } catch (Throwable t) {
                    log.error("Task '{}' restart failed", taskId, t);
                }
            });
            log.debug("Restarted {} of {} tasks for {} as requested", assignedIdsToRestart.size(), plan.totalTaskCount(), request);
        }
        log.info("Completed {}", plan);
    }

    @Override
    public void connectorOffsets(String connName, Callback<ConnectorOffsets> cb) {
        log.trace("Submitting offset fetch request for connector: {}", connName);
        // Handle this in the tick thread to avoid concurrent calls to the Worker
        addRequest(
                () -> {
                    super.connectorOffsets(connName, cb);
                    return null;
                },
                forwardErrorCallback(cb)
        );
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
     * Perform an action that writes to the config topic, and if it fails because the leader has been fenced out, make note of that
     * fact so that we can try to reclaim write ownership (if still the leader of the cluster) in a subsequent iteration of the tick loop.
     * Note that it is not necessary to wrap every write to the config topic in this method, only the writes that should be performed
     * exclusively by the leader. For example, {@link ConfigBackingStore#putTargetState(String, TargetState)} does not require this
     * method, as it can be invoked by any worker in the cluster.
     * @param write the action that writes to the config topic, such as {@link ConfigBackingStore#putSessionKey(SessionKey)} or
     *              {@link ConfigBackingStore#putConnectorConfig(String, Map)}.
     */
    private void writeToConfigTopicAsLeader(Runnable write) {
        try {
            write.run();
        } catch (PrivilegedWriteException e) {
            log.warn("Failed to write to config topic as leader; will rejoin group if necessary and, if still leader, attempt to reclaim write privileges for the config topic", e);
            fencedFromConfigTopic = true;
            throw new ConnectException("Failed to write to config topic; this may be due to a transient error and the request can be safely retried", e);
        }
    }

    /**
     * Handle post-assignment operations, either trying to resolve issues that kept assignment from completing, getting
     * this node into sync and its work started.
     *
     * @return false if we couldn't finish
     */
    private boolean handleRebalanceCompleted() {
        if (rebalanceResolved) {
            log.trace("Returning early because rebalance is marked as resolved (rebalanceResolved: true)");
            return true;
        }
        log.debug("Handling completed but unresolved rebalance");

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

        long now = time.milliseconds();
        if (scheduledRebalance <= now) {
            log.debug("Requesting rebalance because scheduled rebalance timeout has been reached "
                    + "(now: {} scheduledRebalance: {}", scheduledRebalance, now);

            needsRejoin = true;
            scheduledRebalance = Long.MAX_VALUE;
        }

        if (needsReadToEnd) {
            // Force exiting this method to avoid creating any connectors/tasks and require immediate rejoining if
            // we timed out. This should only happen if we failed to read configuration for long enough,
            // in which case giving back control to the main loop will prevent hanging around indefinitely after getting kicked out of the group.
            // We also indicate to the main loop that we failed to readConfigs so it will check that the issue was resolved before trying to join the group
            if (readConfigToEnd(workerSyncTimeoutMs)) {
                canReadConfigs = true;
            } else {
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
        herderMetrics.rebalanceSucceeded(time.milliseconds());
        rebalanceResolved = true;

        if (!assignment.revokedConnectors().isEmpty() || !assignment.revokedTasks().isEmpty()) {
            assignment.revokedConnectors().clear();
            assignment.revokedTasks().clear();
            member.requestRejoin();
            return false;
        }
        return true;
    }

    /**
     * Try to read to the end of the config log within the given timeout. If unsuccessful, leave the group
     * and wait for a brief backoff period before returning
     * @param timeoutMs maximum time to wait to sync to the end of the log
     * @return true if successful, false if timed out
     */
    private boolean readConfigToEnd(long timeoutMs) {
        if (configState.offset() < assignment.offset()) {
            log.info("Current config state offset {} is behind group assignment {}, reading to end of config log", configState.offset(), assignment.offset());
        } else {
            log.info("Reading to end of config log; current config state offset: {}", configState.offset());
        }
        if (refreshConfigSnapshot(timeoutMs)) {
            backoffRetries = BACKOFF_RETRIES;
            return true;
        } else {
            // in case reading the log takes too long, leave the group to ensure a quick rebalance (although by default we should be out of the group already)
            // and back off to avoid a tight loop of rejoin-attempt-to-catch-up-leave
            member.maybeLeaveGroup("taking too long to read the log");
            backoff(workerUnsyncBackoffMs);
            return false;
        }
    }

    /**
     * Try to read to the end of the config log within the given timeout and capture a fresh snapshot via
     * {@link ConfigBackingStore#snapshot()}
     *
     * @param timeoutMs maximum time to wait to sync to the end of the log
     * @return true if successful; false if timed out
     */
    private boolean refreshConfigSnapshot(long timeoutMs) {
        try {
            configBackingStore.refresh(timeoutMs, TimeUnit.MILLISECONDS);
            configState = configBackingStore.snapshot();
            log.info("Finished reading to end of log and updated config snapshot, new config log offset: {}", configState.offset());
            return true;
        } catch (TimeoutException e) {
            log.warn("Didn't reach end of config log quickly enough", e);
            canReadConfigs = false;
            return false;
        }
    }

    private void backoff(long ms) {
        if (ConnectProtocolCompatibility.fromProtocolVersion(currentProtocolVersion) == EAGER) {
            time.sleep(ms);
            return;
        }

        if (backoffRetries > 0) {
            int rebalanceDelayFraction =
                    config.getInt(DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_CONFIG) / 10 / backoffRetries;
            time.sleep(rebalanceDelayFraction);
            --backoffRetries;
            return;
        }

        ExtendedAssignment runningAssignmentSnapshot;
        synchronized (this) {
            runningAssignmentSnapshot = ExtendedAssignment.duplicate(runningAssignment);
        }
        log.info("Revoking current running assignment {} because after {} retries the worker "
                + "has not caught up with the latest Connect cluster updates",
                runningAssignmentSnapshot, BACKOFF_RETRIES);
        member.revokeAssignment(runningAssignmentSnapshot);
        backoffRetries = BACKOFF_RETRIES;
    }

    // Visible for testing
    void startAndStop(Collection<Callable<Void>> callables) {
        try {
            startAndStopExecutor.invokeAll(callables);
        } catch (InterruptedException e) {
            // ignore
        } catch (RejectedExecutionException e) {
            // Shutting down. Just log the exception
            if (stopping.get()) {
                log.debug("Ignoring RejectedExecutionException thrown while starting/stopping connectors/tasks en masse " +
                        "as the herder is already in the process of shutting down. This is not indicative of a problem and is normal behavior");
            } else {
                throw e;
            }
        }
    }

    private void startWork() {
        // Start assigned connectors and tasks
        List<Callable<Void>> callables = new ArrayList<>();

        // The sets in runningAssignment may change when onRevoked is called voluntarily by this
        // herder (e.g. when a broker coordinator failure is detected). Otherwise the
        // runningAssignment is always replaced by the assignment here.
        synchronized (this) {
            log.info("Starting connectors and tasks using config offset {}", assignment.offset());
            log.debug("Received assignment: {}", assignment);
            log.debug("Currently running assignment: {}", runningAssignment);

            for (String connectorName : assignmentDifference(assignment.connectors(), runningAssignment.connectors())) {
                callables.add(getConnectorStartingCallable(connectorName));
            }

            // These tasks have been stopped by this worker due to task reconfiguration. In order to
            // restart them, they are removed just before the overall task startup from the set of
            // currently running tasks. Therefore, they'll be restarted only if they are included in
            // the assignment that was just received after rebalancing.
            log.debug("Tasks to restart from currently running assignment: {}", tasksToRestart);
            runningAssignment.tasks().removeAll(tasksToRestart);
            tasksToRestart.clear();
            for (ConnectorTaskId taskId : assignmentDifference(assignment.tasks(), runningAssignment.tasks())) {
                callables.add(getTaskStartingCallable(taskId));
            }
        }

        startAndStop(callables);

        synchronized (this) {
            runningAssignment = member.currentProtocolVersion() == CONNECT_PROTOCOL_V0
                                ? ExtendedAssignment.empty()
                                : assignment;
        }

        log.info("Finished starting connectors and tasks");
    }

    // arguments should be assignment collections (connectors or tasks) and should not be null
    private static <T> Collection<T> assignmentDifference(Collection<T> update, Collection<T> running) {
        if (running.isEmpty()) {
            return update;
        }
        HashSet<T> diff = new HashSet<>(update);
        diff.removeAll(running);
        return diff;
    }

    private boolean startTask(ConnectorTaskId taskId) {
        log.info("Starting task {}", taskId);
        Map<String, String> connProps = configState.connectorConfig(taskId.connector());
        switch (connectorType(connProps)) {
            case SINK:
                return worker.startSinkTask(
                        taskId,
                        configState,
                        connProps,
                        configState.taskConfig(taskId),
                        this,
                        configState.targetState(taskId.connector())
                );
            case SOURCE:
                if (config.exactlyOnceSourceEnabled()) {
                    int taskGeneration = configState.taskConfigGeneration(taskId.connector());
                    return worker.startExactlyOnceSourceTask(
                            taskId,
                            configState,
                            connProps,
                            configState.taskConfig(taskId),
                            this,
                            configState.targetState(taskId.connector()),
                            () -> {
                                FutureCallback<Void> preflightFencing = new FutureCallback<>();
                                fenceZombieSourceTasks(taskId, preflightFencing);
                                try {
                                    preflightFencing.get();
                                } catch (InterruptedException e) {
                                    throw new ConnectException("Interrupted while attempting to perform round of zombie fencing", e);
                                } catch (ExecutionException e) {
                                    Throwable cause = e.getCause();
                                    throw ConnectUtils.maybeWrap(cause, "Failed to perform round of zombie fencing");
                                }
                            },
                            () -> verifyTaskGenerationAndOwnership(taskId, taskGeneration)
                    );
                } else {
                    return worker.startSourceTask(
                            taskId,
                            configState,
                            connProps,
                            configState.taskConfig(taskId),
                            this,
                            configState.targetState(taskId.connector())
                    );
                }
            default:
                throw new ConnectException("Failed to start task " + taskId + " since it is not a recognizable type (source or sink)");
        }
    }

    private Callable<Void> getTaskStartingCallable(final ConnectorTaskId taskId) {
        return () -> {
            try {
                startTask(taskId);
            } catch (Throwable t) {
                log.error("Couldn't instantiate task {} because it has an invalid task configuration. This task will not execute until reconfigured.",
                        taskId, t);
                onFailure(taskId, t);
            }
            return null;
        };
    }

    private Callable<Void> getTaskStoppingCallable(final ConnectorTaskId taskId) {
        return () -> {
            worker.stopAndAwaitTask(taskId);
            return null;
        };
    }

    // Helper for starting a connector with the given name, which will extract & parse the config, generate connector
    // context and add to the worker. This needs to be called from within the main worker thread for this herder.
    // The callback is invoked after the connector has finished startup and generated task configs, or failed in the process.
    private void startConnector(String connectorName, Callback<Void> callback) {
        log.info("Starting connector {}", connectorName);
        final Map<String, String> configProps = configState.connectorConfig(connectorName);
        final CloseableConnectorContext ctx = new HerderConnectorContext(this, connectorName);
        final TargetState initialState = configState.targetState(connectorName);
        final Callback<TargetState> onInitialStateChange = (error, newState) -> {
            if (error != null) {
                callback.onCompletion(new ConnectException("Failed to start connector: " + connectorName, error), null);
                return;
            }

            // Use newState here in case the connector has been paused right after being created
            if (newState == TargetState.STARTED) {
                addRequest(
                    () -> {
                        // Request configuration since this could be a brand new connector. However, also only update those
                        // task configs if they are actually different from the existing ones to avoid unnecessary updates when this is
                        // just restoring an existing connector.
                        reconfigureConnectorTasksWithRetry(time.milliseconds(), connectorName);
                        callback.onCompletion(null, null);
                        return null;
                    },
                    forwardErrorCallback(callback)
                );
            } else {
                callback.onCompletion(null, null);
            }
        };
        worker.startConnector(connectorName, configProps, ctx, this, initialState, onInitialStateChange);
    }

    private Callable<Void> getConnectorStartingCallable(final String connectorName) {
        return () -> {
            try {
                startConnector(connectorName, (error, result) -> {
                    if (error != null) {
                        log.error("Failed to start connector '" + connectorName + "'", error);
                    }
                });
            } catch (Throwable t) {
                log.error("Unexpected error while trying to start connector " + connectorName, t);
                onFailure(connectorName, t);
            }
            return null;
        };
    }

    private Callable<Void> getConnectorStoppingCallable(final String connectorName) {
        return () -> {
            try {
                worker.stopAndAwaitConnector(connectorName);
            } catch (Throwable t) {
                log.error("Failed to shut down connector " + connectorName, t);
            }
            return null;
        };
    }

    /**
     * Request task configs from the connector and write them to the config storage in case the configs are detected to
     * have changed. This method retries infinitely with exponential backoff in case of any errors. The initial backoff
     * used is {@link #RECONFIGURE_CONNECTOR_TASKS_BACKOFF_INITIAL_MS} with a multiplier of 2 and a maximum backoff of
     * {@link #RECONFIGURE_CONNECTOR_TASKS_BACKOFF_MAX_MS}
     *
     * @param initialRequestTime the time in milliseconds when the original request was made (i.e. before any retries)
     * @param connName the name of the connector
     */
    private void reconfigureConnectorTasksWithRetry(long initialRequestTime, final String connName) {
        ExponentialBackoff exponentialBackoff = new ExponentialBackoff(
                RECONFIGURE_CONNECTOR_TASKS_BACKOFF_INITIAL_MS,
                2, RECONFIGURE_CONNECTOR_TASKS_BACKOFF_MAX_MS,
                0);

        reconfigureConnectorTasksWithExponentialBackoffRetries(initialRequestTime, connName, exponentialBackoff, 0);
    }

    /**
     * Request task configs from the connector and write them to the config storage in case the configs are detected to
     * have changed. This method retries infinitely with exponential backoff in case of any errors.
     *
     * @param initialRequestTime the time in milliseconds when the original request was made (i.e. before any retries)
     * @param connName the name of the connector
     * @param exponentialBackoff {@link ExponentialBackoff} used to calculate the retry backoff duration
     * @param attempts the number of retry attempts that have been made
     */
    private void reconfigureConnectorTasksWithExponentialBackoffRetries(long initialRequestTime, final String connName, ExponentialBackoff exponentialBackoff, int attempts) {
        reconfigureConnector(connName, (error, result) -> {
            // If we encountered an error, we don't have much choice but to just retry. If we don't, we could get
            // stuck with a connector that thinks it has generated tasks, but wasn't actually successful and therefore
            // never makes progress. The retry has to run through a DistributedHerderRequest since this callback could be happening
            // from the HTTP request forwarding thread.
            if (error != null) {
                if (isPossibleExpiredKeyException(initialRequestTime, error)) {
                    log.debug("Failed to reconfigure connector's tasks ({}), possibly due to expired session key. Retrying after backoff", connName);
                } else {
                    log.error("Failed to reconfigure connector's tasks ({}), retrying after backoff.", connName, error);
                }
                addRequest(exponentialBackoff.backoff(attempts),
                    () -> {
                        reconfigureConnectorTasksWithExponentialBackoffRetries(initialRequestTime, connName, exponentialBackoff, attempts + 1);
                        return null;
                    }, (err, res) -> {
                        if (err != null) {
                            log.error("Unexpected error during connector task reconfiguration: ", err);
                            log.error("Task reconfiguration for {} failed unexpectedly, this connector will not be properly reconfigured unless manually triggered.", connName);
                        }
                    }
                );
            }
        });
    }

    boolean isPossibleExpiredKeyException(long initialRequestTime, Throwable error) {
        if (error instanceof ConnectRestException) {
            ConnectRestException connectError = (ConnectRestException) error;
            return connectError.statusCode() == Response.Status.FORBIDDEN.getStatusCode()
                && initialRequestTime + TimeUnit.MINUTES.toMillis(1) >= time.milliseconds();
        }
        return false;
    }

    // Updates configurations for a connector by requesting them from the connector, filling in parameters provided
    // by the system, then checks whether any configs have actually changed before submitting the new configs to storage
    private void reconfigureConnector(final String connName, final Callback<Void> cb) {
        try {
            if (!worker.isRunning(connName)) {
                log.info("Skipping reconfiguration of connector {} since it is not running", connName);
                return;
            } else if (configState.targetState(connName) != TargetState.STARTED) {
                log.info("Skipping reconfiguration of connector {} since its target state is {}", connName, configState.targetState(connName));
                return;
            }

            Map<String, String> configs = configState.connectorConfig(connName);

            ConnectorConfig connConfig;
            if (worker.isSinkConnector(connName)) {
                connConfig = new SinkConnectorConfig(plugins(), configs);
            } else {
                connConfig = new SourceConnectorConfig(plugins(), configs, worker.isTopicCreationEnabled());
            }

            final List<Map<String, String>> taskProps = worker.connectorTaskConfigs(connName, connConfig);
            publishConnectorTaskConfigs(connName, taskProps, cb);
        } catch (Throwable t) {
            cb.onCompletion(t, null);
        }
    }

    private void publishConnectorTaskConfigs(String connName, List<Map<String, String>> taskProps, Callback<Void> cb) {
        if (!taskConfigsChanged(configState, connName, taskProps)) {
            return;
        }

        List<Map<String, String>> rawTaskProps = reverseTransform(connName, configState, taskProps);
        if (isLeader()) {
            writeTaskConfigs(connName, rawTaskProps);
            cb.onCompletion(null, null);
        } else if (restClient == null) {
            throw new NotLeaderException("This worker is not able to communicate with the leader of the cluster, "
                    + "which is required for dynamically-reconfiguring connectors. If running MirrorMaker 2 "
                    + "in dedicated mode, consider enabling inter-worker communication via the "
                    + "'dedicated.mode.enable.internal.rest' property.",
                    leaderUrl()
            );
        } else {
            // We cannot forward the request on the same thread because this reconfiguration can happen as a result of connector
            // addition or removal. If we blocked waiting for the response from leader, we may be kicked out of the worker group.
            forwardRequestExecutor.submit(() -> {
                try {
                    String leaderUrl = leaderUrl();
                    if (Utils.isBlank(leaderUrl)) {
                        cb.onCompletion(new ConnectException("Request to leader to " +
                                "reconfigure connector tasks failed " +
                                "because the URL of the leader's REST interface is empty!"), null);
                        return;
                    }
                    String reconfigUrl = namespacedUrl(leaderUrl)
                            .path("connectors")
                            .path(connName)
                            .path("tasks")
                            .build()
                            .toString();
                    log.trace("Forwarding task configurations for connector {} to leader", connName);
                    restClient.httpRequest(reconfigUrl, "POST", null, rawTaskProps, null, sessionKey, requestSignatureAlgorithm);
                    cb.onCompletion(null, null);
                } catch (ConnectException e) {
                    log.error("Request to leader to reconfigure connector tasks failed", e);
                    cb.onCompletion(e, null);
                }
            });
        }
    }

    private void writeTaskConfigs(String connName, List<Map<String, String>> taskConfigs) {
        if (!taskConfigs.isEmpty()) {
            if (configState.targetState(connName) == TargetState.STOPPED)
                throw new BadRequestException("Cannot submit non-empty set of task configs for stopped connector " + connName);
        }

        writeToConfigTopicAsLeader(() -> configBackingStore.putTaskConfigs(connName, taskConfigs));
    }

    // Invoked by exactly-once worker source tasks after they have successfully initialized their transactional
    // producer to ensure that it is still safe to bring up the task
    private void verifyTaskGenerationAndOwnership(ConnectorTaskId id, int initialTaskGen) {
        log.debug("Reading to end of config topic to ensure it is still safe to bring up source task {} with exactly-once support", id);
        if (!refreshConfigSnapshot(Long.MAX_VALUE)) {
            throw new ConnectException("Failed to read to end of config topic");
        }

        FutureCallback<Void> verifyCallback = new FutureCallback<>();

        addRequest(
            () -> verifyTaskGenerationAndOwnership(id, initialTaskGen, verifyCallback),
            forwardErrorCallback(verifyCallback)
        );

        try {
            verifyCallback.get();
        } catch (InterruptedException e) {
            throw new ConnectException("Interrupted while performing preflight check for task " + id, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw ConnectUtils.maybeWrap(cause, "Failed to perform preflight check for task " + id);
        }
    }

    // Visible for testing
    Void verifyTaskGenerationAndOwnership(ConnectorTaskId id, int initialTaskGen, Callback<Void> callback) {
        Integer currentTaskGen = configState.taskConfigGeneration(id.connector());
        if (!Objects.equals(initialTaskGen, currentTaskGen)) {
            throw new ConnectException("Cannot start source task "
                + id + " with exactly-once support as the connector has already generated a new set of task configs");
        }

        if (!assignment.tasks().contains(id)) {
            throw new ConnectException("Cannot start source task "
                + id + " as it has already been revoked from this worker");
        }

        callback.onCompletion(null, null);
        return null;
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

    /**
     * Execute the given action and subsequent callback immediately if the current thread is the herder's tick thread,
     * or use them to create and store a {@link DistributedHerderRequest} on the request queue and return the resulting request
     * if not.
     * @param action the action that should be run on the herder's tick thread
     * @param callback the callback that should be invoked once the action is complete
     * @return a new {@link DistributedHerderRequest} if one has been created and added to the request queue, and {@code null} otherwise
     */
    DistributedHerderRequest runOnTickThread(Callable<Void> action, Callback<Void> callback) {
        if (Thread.currentThread().equals(herderThread)) {
            runRequest(action, callback);
            return null;
        } else {
            return addRequest(action, callback);
        }
    }

    DistributedHerderRequest addRequest(Callable<Void> action, Callback<Void> callback) {
        return addRequest(0, action, callback);
    }

    DistributedHerderRequest addRequest(long delayMs, Callable<Void> action, Callback<Void> callback) {
        DistributedHerderRequest req = new DistributedHerderRequest(time.milliseconds() + delayMs, requestSeqNum.incrementAndGet(), action, callback);
        requests.add(req);
        if (peekWithoutException() == req)
            member.wakeup();
        return req;
    }

    private void runRequest(Callable<Void> action, Callback<Void> callback) {
        try {
            action.call();
            callback.onCompletion(null, null);
        } catch (Throwable t) {
            callback.onCompletion(t, null);
        }
    }

    private boolean internalRequestValidationEnabled() {
        return internalRequestValidationEnabled(member.currentProtocolVersion());
    }

    private static boolean internalRequestValidationEnabled(short protocolVersion) {
        return protocolVersion >= CONNECT_PROTOCOL_V2;
    }

    private DistributedHerderRequest peekWithoutException() {
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

            // Stage the update and wake up the work thread.
            // The set of tasks is recorded for incremental cooperative rebalancing, in which
            // tasks don't get restarted unless they are balanced between workers.
            // With eager rebalancing there's no need to record the set of tasks because task reconfigs
            // always need a rebalance to ensure offsets get committed. In eager rebalancing the
            // recorded set of tasks remains unused.
            // TODO: As an optimization, some task config updates could avoid a rebalance. In particular, single-task
            // connectors clearly don't need any coordination.
            synchronized (DistributedHerder.this) {
                needsReconfigRebalance = true;
                taskConfigUpdates.addAll(tasks);
            }
            tasks.stream()
                    .map(ConnectorTaskId::connector)
                    .distinct()
                    .forEach(connName -> {
                        synchronized (this) {
                            ZombieFencing activeFencing = activeZombieFencings.get(connName);
                            if (activeFencing != null) {
                                activeFencing.completeExceptionally(new ConnectRestException(
                                    Response.Status.CONFLICT.getStatusCode(),
                                    "Failed to complete zombie fencing because a new set of task configs was generated"
                                ));
                            }
                        }
                    });
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

        @Override
        public void onSessionKeyUpdate(SessionKey sessionKey) {
            log.info("Session key updated");

            synchronized (DistributedHerder.this) {
                DistributedHerder.this.sessionKey = sessionKey.key();
                // Track the expiration of the key.
                // Followers will receive rotated keys from the leader and won't be responsible for
                // tracking expiration and distributing new keys themselves, but may become leaders
                // later on and will need to know when to update the key.
                if (keyRotationIntervalMs > 0) {
                    DistributedHerder.this.keyExpiration = sessionKey.creationTimestamp() + keyRotationIntervalMs;
                }
            }
        }

        @Override
        public void onRestartRequest(RestartRequest request) {
            log.info("Received and enqueuing {}", request);

            synchronized (DistributedHerder.this) {
                String connectorName = request.connectorName();
                //preserve the highest impact request
                pendingRestartRequests.compute(connectorName, (k, existingRequest) -> {
                    if (existingRequest == null || request.compareTo(existingRequest) > 0) {
                        log.debug("Overwriting existing {} and enqueuing the higher impact {}", existingRequest, request);
                        return request;
                    } else {
                        log.debug("Preserving existing higher impact {} and ignoring incoming {}", existingRequest, request);
                        return existingRequest;
                    }
                });
            }
            member.wakeup();
        }
    }

    class DistributedHerderRequest implements HerderRequest, Comparable<DistributedHerderRequest> {
        private final long at;
        private final long seq;
        private final Callable<Void> action;
        private final Callback<Void> callback;

        public DistributedHerderRequest(long at, long seq, Callable<Void> action, Callback<Void> callback) {
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
        public void cancel() {
            DistributedHerder.this.requests.remove(this);
        }

        @Override
        public int compareTo(DistributedHerderRequest o) {
            final int cmp = Long.compare(at, o.at);
            return cmp == 0 ? Long.compare(seq, o.seq) : cmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DistributedHerderRequest))
                return false;
            DistributedHerderRequest other = (DistributedHerderRequest) o;
            return compareTo(other) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(at, seq);
        }
    }

    private static Callback<Void> forwardErrorCallback(final Callback<?> callback) {
        return (error, result) -> {
            if (error != null)
                callback.onCompletion(error, null);
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

    private void updateDeletedTaskStatus() {
        ClusterConfigState snapshot = configBackingStore.snapshot();
        for (String connector : statusBackingStore.connectors()) {
            Set<ConnectorTaskId> remainingTasks = new HashSet<>(snapshot.tasks(connector));
            
            statusBackingStore.getAll(connector).stream()
                .map(TaskStatus::id)
                .filter(task -> !remainingTasks.contains(task))
                .forEach(this::onDeletion);
        }
    }

    protected HerderMetrics herderMetrics() {
        return herderMetrics;
    }

    // Rebalances are triggered internally from the group member, so these are always executed in the work thread.
    public class RebalanceListener implements WorkerRebalanceListener {
        private final Time time;
        RebalanceListener(Time time) {
            this.time = time;
        }

        @Override
        public void onAssigned(ExtendedAssignment assignment, int generation) {
            // This callback just logs the info and saves it. The actual response is handled in the main loop, which
            // ensures the group member's logic for rebalancing can complete, potentially long-running steps to
            // catch up (or backoff if we fail) not executed in a callback, and so we'll be able to invoke other
            // group membership actions (e.g., we may need to explicitly leave the group if we cannot handle the
            // assigned tasks).
            short priorProtocolVersion = currentProtocolVersion;
            DistributedHerder.this.currentProtocolVersion = member.currentProtocolVersion();
            log.info(
                "Joined group at generation {} with protocol version {} and got assignment: {} with rebalance delay: {}",
                generation,
                DistributedHerder.this.currentProtocolVersion,
                assignment,
                assignment.delay()
            );
            synchronized (DistributedHerder.this) {
                DistributedHerder.this.assignment = assignment;
                DistributedHerder.this.generation = generation;
                int delay = assignment.delay();
                DistributedHerder.this.scheduledRebalance = delay > 0
                    ? time.milliseconds() + delay
                    : Long.MAX_VALUE;

                boolean requestValidationWasEnabled = internalRequestValidationEnabled(priorProtocolVersion);
                boolean requestValidationNowEnabled = internalRequestValidationEnabled(currentProtocolVersion);
                if (requestValidationNowEnabled != requestValidationWasEnabled) {
                    // Internal request verification has been switched on or off; let the user know
                    if (requestValidationNowEnabled) {
                        log.info("Internal request validation has been re-enabled");
                    } else {
                        log.warn(
                            "The protocol used by this Connect cluster has been downgraded from '{}' to '{}' and internal request "
                                + "validation is now disabled. This is most likely caused by a new worker joining the cluster with an "
                                + "older protocol specified for the {} configuration; if this is not intentional, either remove the {} "
                                + "configuration from that worker's config file, or change its value to '{}'. If this configuration is "
                                + "left as-is, the cluster will be insecure; for more information, see KIP-507: "
                                + "https://cwiki.apache.org/confluence/display/KAFKA/KIP-507%3A+Securing+Internal+Connect+REST+Endpoints",
                            ConnectProtocolCompatibility.fromProtocolVersion(priorProtocolVersion),
                            ConnectProtocolCompatibility.fromProtocolVersion(DistributedHerder.this.currentProtocolVersion),
                            DistributedConfig.CONNECT_PROTOCOL_CONFIG,
                            DistributedConfig.CONNECT_PROTOCOL_CONFIG,
                            ConnectProtocolCompatibility.SESSIONED.name()
                        );
                    }
                }

                rebalanceResolved = false;
                herderMetrics.rebalanceStarted(time.milliseconds());
            }

            if (isLeader()) {
                // Delete the statuses of all connectors and tasks removed prior to the start of this rebalance. This
                // has to be done after the rebalance completes to avoid race conditions as the previous generation
                // attempts to change the state to UNASSIGNED after tasks have been stopped.
                updateDeletedConnectorStatus();
                updateDeletedTaskStatus();
                // As the leader, we're now allowed to write directly to the config topic for important things like
                // connector configs, session keys, and task count records
                try {
                    configBackingStore.claimWritePrivileges();
                } catch (Exception e) {
                    fencedFromConfigTopic = true;
                    log.error("Unable to claim write privileges for config topic after being elected leader during rebalance", e);
                }
            }

            // We *must* interrupt any poll() call since this could occur when the poll starts, and we might then
            // sleep in the poll() for a long time. Forcing a wakeup ensures we'll get to process this event in the
            // main thread.
            member.wakeup();
        }

        @Override
        public void onRevoked(String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
            // Note that since we don't reset the assignment, we don't revoke leadership here. During a rebalance,
            // it is still important to have a leader that can write configs, offsets, etc.

            if (rebalanceResolved || currentProtocolVersion >= CONNECT_PROTOCOL_V1) {
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

                // The actual timeout for graceful task/connector stop is applied in worker's
                // stopAndAwaitTask/stopAndAwaitConnector methods.
                startAndStop(callables);
                log.info("Finished stopping tasks in preparation for rebalance");

                synchronized (DistributedHerder.this) {
                    log.debug("Removing connectors from running assignment {}", connectors);
                    runningAssignment.connectors().removeAll(connectors);
                    log.debug("Removing tasks from running assignment {}", tasks);
                    runningAssignment.tasks().removeAll(tasks);
                }

                if (isTopicTrackingEnabled) {
                    // Send tombstones to reset active topics for removed connectors only after
                    // connectors and tasks have been stopped, or these tombstones will be overwritten
                    resetActiveTopics(connectors, tasks);
                }

                // Ensure that all status updates have been pushed to the storage system before rebalancing.
                // Otherwise, we may inadvertently overwrite the state with a stale value after the rebalance
                // completes.
                statusBackingStore.flush();
                log.info("Finished flushing status backing store in preparation for rebalance");
            } else {
                log.info("Wasn't able to resume work after last rebalance, can skip stopping connectors and tasks");
            }
        }

        private void resetActiveTopics(Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
            connectors.stream()
                    .filter(connectorName -> !configState.contains(connectorName))
                    .forEach(DistributedHerder.this::resetConnectorActiveTopics);
            tasks.stream()
                    .map(ConnectorTaskId::connector)
                    .distinct()
                    .filter(connectorName -> !configState.contains(connectorName))
                    .forEach(DistributedHerder.this::resetConnectorActiveTopics);
        }
    }

    private boolean isSourceConnector(String connName) {
        return ConnectorType.SOURCE.equals(connectorType(configState.connectorConfig(connName)));
    }

    /**
     * Checks a given {@link InternalRequestSignature request signature} for validity and adds an exception
     * to the given {@link Callback} if any errors are found.
     *
     * @param requestSignature the request signature to validate
     * @param callback callback to report invalid signature errors to
     * @return true if the signature was not valid
     */
    private boolean requestNotSignedProperly(InternalRequestSignature requestSignature, Callback<?> callback) {
        if (internalRequestValidationEnabled()) {
            ConnectRestException requestValidationError = null;
            if (requestSignature == null) {
                requestValidationError = new BadRequestException("Internal request missing required signature");
            } else if (!keySignatureVerificationAlgorithms.contains(requestSignature.keyAlgorithm())) {
                requestValidationError = new BadRequestException(String.format(
                        "This worker does not support the '%s' key signing algorithm used by other workers. "
                                + "This worker is currently configured to use: %s. "
                                + "Check that all workers' configuration files permit the same set of signature algorithms, "
                                + "and correct any misconfigured worker and restart it.",
                        requestSignature.keyAlgorithm(),
                        keySignatureVerificationAlgorithms
                ));
            } else if (sessionKey == null) {
                requestValidationError = new ConnectRestException(
                        Response.Status.SERVICE_UNAVAILABLE,
                        "This worker is still starting up and has not been able to read a session key from the config topic yet"
                );
            } else if (!requestSignature.isValid(sessionKey)) {
                requestValidationError = new ConnectRestException(
                        Response.Status.FORBIDDEN,
                        "Internal request contained invalid signature."
                );
            }
            if (requestValidationError != null) {
                callback.onCompletion(requestValidationError, null);
                return true;
            }
        }

        return false;
    }

    private UriBuilder namespacedUrl(String workerUrl) {
        UriBuilder result = UriBuilder.fromUri(workerUrl);
        for (String namespacePath : restNamespace) {
            result = result.path(namespacePath);
        }
        return result;
    }

    /**
     * Represents an active zombie fencing: that is, an in-progress attempt to invoke
     * {@link Worker#fenceZombies(String, int, Map)} and then, if successful, write a new task count
     * record to the config topic.
     */
    class ZombieFencing {
        private final String connName;
        private final int tasksToFence;
        private final int tasksToRecord;
        private final int taskGen;
        private final FutureCallback<Void> fencingFollowup;
        private KafkaFuture<Void> fencingFuture;

        public ZombieFencing(String connName, int tasksToFence, int tasksToRecord, int taskGen) {
            this.connName = connName;
            this.tasksToFence = tasksToFence;
            this.tasksToRecord = tasksToRecord;
            this.taskGen = taskGen;
            this.fencingFollowup = new FutureCallback<>();
        }

        /**
         * Start sending requests to the Kafka cluster to fence zombies. In rare cases, may cause blocking calls to
         * take place before returning, so care should be taken to ensure that this method is not invoked while holding
         * any important locks (e.g., while synchronized on the surrounding DistributedHerder instance).
         * This method must be invoked before any {@link #addCallback(Callback) callbacks can be added},
         * and may only be invoked once.
         * @throws IllegalStateException if invoked multiple times
         */
        public void start() {
            if (fencingFuture != null) {
                throw new IllegalStateException("Cannot invoke start() multiple times");
            }
            fencingFuture = worker.fenceZombies(connName, tasksToFence, configState.connectorConfig(connName)).thenApply(ignored -> {
                // This callback will be called on the same thread that invokes KafkaFuture::thenApply if
                // the future is already completed. Since that thread is the herder tick thread, we don't need
                // to perform follow-up logic through an additional herder request (and if we tried, it would lead
                // to deadlock)
                runOnTickThread(
                        this::onZombieFencingSuccess,
                        fencingFollowup
                );
                awaitFollowup();
                return null;
            });
            // Immediately after the fencing and necessary followup work (i.e., writing the task count record to the config topic)
            // is complete, remove this from the list of active fencings
            addCallback((ignored, error) -> {
                synchronized (DistributedHerder.this) {
                    activeZombieFencings.remove(connName);
                }
            });

        }

        // Invoked after the worker has successfully fenced out the producers of old task generations using an admin client
        // Note that work here will be performed on the herder's tick thread, so it should not block for very long
        private Void onZombieFencingSuccess() {
            if (!refreshConfigSnapshot(workerSyncTimeoutMs)) {
                throw new ConnectException("Failed to read to end of config topic");
            }
            if (taskGen < configState.taskConfigGeneration(connName)) {
                throw new ConnectRestException(
                    Response.Status.CONFLICT.getStatusCode(),
                    "Fencing failed because new task configurations were generated for the connector");
            }
            // If we've already been cancelled, skip the write to the config topic
            if (fencingFollowup.isDone()) {
                return null;
            }
            writeToConfigTopicAsLeader(() -> configBackingStore.putTaskCountRecord(connName, tasksToRecord));
            return null;
        }

        private void awaitFollowup() {
            try {
                fencingFollowup.get();
            } catch (InterruptedException e) {
                throw new ConnectException("Interrupted while performing zombie fencing", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                throw ConnectUtils.maybeWrap(cause, "Failed to perform round of zombie fencing");
            }
        }

        /**
         * Fail the fencing if it is still active, reporting the given exception as the cause of failure
         * @param t the cause of failure to report for the failed fencing; may not be null
         */
        public void completeExceptionally(Throwable t) {
            Objects.requireNonNull(t);
            fencingFollowup.onCompletion(t, null);
        }

        /**
         * Add a callback to invoke after the fencing has succeeded and a record of it has been written to the config topic
         * Note that this fencing must be {@link #start() started} before this method is invoked
         * @param callback the callback to report the success or failure of the fencing to
         * @throws IllegalStateException if this method is invoked before {@link #start()}
         */
        public void addCallback(Callback<Void> callback) {
            if (fencingFuture == null) {
                throw new IllegalStateException("The start() method must be invoked before adding callbacks for this zombie fencing");
            }
            fencingFuture.whenComplete((ignored, error) -> {
                if (error != null) {
                    callback.onCompletion(
                            ConnectUtils.maybeWrap(error, "Failed to perform zombie fencing"),
                            null
                    );
                } else {
                    callback.onCompletion(null, null);
                }
            });
        }
    }

    /**
     * A collection of cluster rebalance related metrics.
     */
    class HerderMetrics {
        private final MetricGroup metricGroup;
        private final Sensor rebalanceCompletedCounts;
        private final Sensor rebalanceTime;
        private volatile long lastRebalanceCompletedAtMillis = Long.MIN_VALUE;
        private volatile boolean rebalancing = false;
        private volatile long rebalanceStartedAtMillis = 0L;

        public HerderMetrics(ConnectMetrics connectMetrics) {
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.workerRebalanceGroupName());

            metricGroup.addValueMetric(registry.connectProtocol, now ->
                ConnectProtocolCompatibility.fromProtocolVersion(member.currentProtocolVersion()).name()
            );
            metricGroup.addValueMetric(registry.leaderName, now -> leaderUrl());
            metricGroup.addValueMetric(registry.epoch, now -> (double) generation);
            metricGroup.addValueMetric(registry.rebalanceMode, now -> rebalancing ? 1.0d : 0.0d);

            rebalanceCompletedCounts = metricGroup.sensor("completed-rebalance-count");
            rebalanceCompletedCounts.add(metricGroup.metricName(registry.rebalanceCompletedTotal), new CumulativeSum());

            rebalanceTime = metricGroup.sensor("rebalance-time");
            rebalanceTime.add(metricGroup.metricName(registry.rebalanceTimeMax), new Max());
            rebalanceTime.add(metricGroup.metricName(registry.rebalanceTimeAvg), new Avg());

            metricGroup.addValueMetric(registry.rebalanceTimeSinceLast, now ->
                lastRebalanceCompletedAtMillis == Long.MIN_VALUE ? Double.POSITIVE_INFINITY : (double) (now - lastRebalanceCompletedAtMillis));
        }

        void close() {
            metricGroup.close();
        }

        void rebalanceStarted(long now) {
            rebalanceStartedAtMillis = now;
            rebalancing = true;
        }

        void rebalanceSucceeded(long now) {
            long duration = Math.max(0L, now - rebalanceStartedAtMillis);
            rebalancing = false;
            rebalanceCompletedCounts.record(1.0);
            rebalanceTime.record(duration);
            lastRebalanceCompletedAtMillis = now;
        }

        protected MetricGroup metricGroup() {
            return metricGroup;
        }
    }
}

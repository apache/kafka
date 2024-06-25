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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.LoggingContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Container for connectors which is responsible for managing their lifecycle (e.g. handling startup,
 * shutdown, pausing, etc.). Internally, we manage the runtime state of the connector and transition according
 * to target state changes. Note that unlike connector tasks, the connector does not really have a "pause"
 * state which is distinct from being stopped. We therefore treat pause operations as requests to momentarily
 * stop the connector, and resume operations as requests to restart it (without reinitialization). Connector
 * failures, whether in initialization or after startup, are treated as fatal, which means that we will not attempt
 * to restart this connector instance after failure. What this means from a user perspective is that you must
 * use the /restart REST API to restart a failed task. This behavior is consistent with task failures.
 * <p>
 * Note that this class is NOT thread-safe.
 */
public class WorkerConnector implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WorkerConnector.class);
    private static final String THREAD_NAME_PREFIX = "connector-thread-";

    private enum State {
        INIT,    // initial state before startup
        PAUSED,  // The connector has been paused.
        STOPPED, // the connector has been stopped.
        STARTED, // the connector has been started/resumed.
        FAILED,  // the connector has failed (no further transitions are possible after this state)
    }

    private final String connName;
    private final Map<String, String> config;
    private final ConnectorStatus.Listener statusListener;
    private final ClassLoader loader;
    private final CloseableConnectorContext ctx;
    private final Connector connector;
    private final ConnectorMetricsGroup metrics;
    private final AtomicReference<TargetState> pendingTargetStateChange;
    private final AtomicReference<Callback<TargetState>> pendingStateChangeCallback;
    private final CountDownLatch shutdownLatch;
    private volatile Throwable externalFailure;
    private volatile boolean stopping;  // indicates whether the Worker has asked the connector to stop
    private volatile boolean cancelled; // indicates whether the Worker has cancelled the connector (e.g. because of slow shutdown)

    private State state;
    private final CloseableOffsetStorageReader offsetStorageReader;
    private final ConnectorOffsetBackingStore offsetStore;

    public WorkerConnector(String connName,
                           Connector connector,
                           ConnectorConfig connectorConfig,
                           CloseableConnectorContext ctx,
                           ConnectMetrics metrics,
                           ConnectorStatus.Listener statusListener,
                           CloseableOffsetStorageReader offsetStorageReader,
                           ConnectorOffsetBackingStore offsetStore,
                           ClassLoader loader) {
        this.connName = connName;
        this.config = connectorConfig.originalsStrings();
        this.loader = loader;
        this.ctx = ctx;
        this.connector = connector;
        this.state = State.INIT;
        this.metrics = new ConnectorMetricsGroup(metrics, AbstractStatus.State.UNASSIGNED, statusListener);
        this.statusListener = this.metrics;
        this.offsetStorageReader = offsetStorageReader;
        this.offsetStore = offsetStore;
        this.pendingTargetStateChange = new AtomicReference<>();
        this.pendingStateChangeCallback = new AtomicReference<>();
        this.shutdownLatch = new CountDownLatch(1);
        this.externalFailure = null;
        this.stopping = false;
        this.cancelled = false;
    }

    public ClassLoader loader() {
        return loader;
    }

    @Override
    public void run() {
        // Clear all MDC parameters, in case this thread is being reused
        LoggingContext.clear();

        try (LoggingContext loggingContext = LoggingContext.forConnector(connName)) {
            String savedName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(THREAD_NAME_PREFIX + connName);
                doRun();
            } finally {
                Thread.currentThread().setName(savedName);
            }
        } finally {
            // In the rare case of an exception being thrown outside the doRun() method, or an
            // uncaught one being thrown from within it, mark the connector as shut down to avoid
            // unnecessarily blocking and eventually timing out during awaitShutdown
            shutdownLatch.countDown();
        }
    }

    /**
     * Fail the connector.
     * @param cause the cause of the failure; if null, the connector will not be failed
     */
    public void fail(Throwable cause) {
        synchronized (this) {
            if (this.externalFailure != null)
                return;
            log.error("{} Connector has failed", this, cause);
            this.externalFailure = cause;
            notify();
        }
    }

    void doRun() {
        initialize();
        while (!stopping) {
            Throwable failure = externalFailure;
            if (failure != null)
                onFailure(failure);

            TargetState newTargetState;
            Callback<TargetState> stateChangeCallback;
            synchronized (this) {
                newTargetState = pendingTargetStateChange.getAndSet(null);
                stateChangeCallback = pendingStateChangeCallback.getAndSet(null);
            }
            if (newTargetState != null && !stopping) {
                doTransitionTo(newTargetState, stateChangeCallback);
            }
            synchronized (this) {
                if (pendingTargetStateChange.get() != null
                        || (!State.FAILED.equals(state) && externalFailure != null)
                        || stopping
                ) {
                    // An update occurred before we entered the synchronized block; no big deal,
                    // just start the loop again until we've handled everything
                } else {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        // We'll pick up any potential state changes at the top of the loop
                    }
                }
            }
        }
        doShutdown();
    }

    void initialize() {
        try {
            if (!isSourceConnector() && !isSinkConnector()) {
                throw new ConnectException("Connector implementations must be a subclass of either SourceConnector or SinkConnector");
            }
            log.debug("{} Initializing connector {}", this, connName);
            if (isSinkConnector()) {
                SinkConnectorConfig.validate(config);
                connector.initialize(new WorkerSinkConnectorContext());
            } else {
                Objects.requireNonNull(offsetStore, "Offset store cannot be null for source connectors");
                Objects.requireNonNull(offsetStorageReader, "Offset reader cannot be null for source connectors");
                offsetStore.start();
                connector.initialize(new WorkerSourceConnectorContext(offsetStorageReader));
            }
        } catch (Throwable t) {
            log.error("{} Error initializing connector", this, t);
            onFailure(t);
        }
    }

    private boolean doStart() {
        try {
            switch (state) {
                case STARTED:
                    return false;

                case INIT:
                case PAUSED:
                case STOPPED:
                    connector.start(config);
                    this.state = State.STARTED;
                    return true;

                default:
                    throw new IllegalArgumentException("Cannot start connector in state " + state);
            }
        } catch (Throwable t) {
            log.error("{} Error while starting connector", this, t);
            onFailure(t);
            throw t;
        }
    }

    private synchronized void onFailure(Throwable t) {
        // If we've already failed, we don't overwrite the last-reported cause of failure
        if (this.state == State.FAILED)
            return;

        statusListener.onFailure(connName, t);
        this.state = State.FAILED;
    }

    private void resume() {
        if (doStart())
            statusListener.onResume(connName);
    }

    private void start() {
        if (doStart())
            statusListener.onStartup(connName);
    }

    public boolean isRunning() {
        return state == State.STARTED;
    }

    private void suspend(boolean paused) {
        State newState = paused ? State.PAUSED : State.STOPPED;
        try {
            if (state == newState) {
                // Already in the desired state
                return;
            }

            if (state == State.STARTED) {
                connector.stop();
            }

            if (state == State.FAILED && newState != State.STOPPED) {
                throw new IllegalArgumentException("Cannot transition to non-stopped state when connector has already failed");
            }

            if (paused) {
                statusListener.onPause(connName);
            } else {
                statusListener.onStop(connName);
            }

            this.state = newState;
        } catch (Throwable t) {
            log.error("{} Error while {} connector", this, paused ? "pausing" : "stopping", t);
            if (paused) {
                statusListener.onFailure(connName, t);
                this.state = State.FAILED;
            } else {
                // We say the connector is STOPPED even if it fails at this point
                this.state = State.STOPPED;
                // One more try to make sure the status is updated correctly
                statusListener.onStop(connName);
            }
        }
    }

    /**
     * Stop this connector. This method does not block, it only triggers shutdown. Use
     * #{@link #awaitShutdown} to block until completion.
     */
    public synchronized void shutdown() {
        log.info("Scheduled shutdown for {}", this);
        stopping = true;
        notify();
    }

    void doShutdown() {
        try {
            TargetState preEmptedState = pendingTargetStateChange.getAndSet(null);
            Callback<TargetState> stateChangeCallback = pendingStateChangeCallback.getAndSet(null);
            if (stateChangeCallback != null) {
                stateChangeCallback.onCompletion(
                        new ConnectException(
                                "Could not begin changing connector state to " + preEmptedState.name()
                                    + " as the connector has been scheduled for shutdown"),
                        null);
            }
            // Call stop() when the connector has failed, for the connector
            // to release its resources.
            if (state == State.STARTED || state == State.FAILED)
                connector.stop();
            this.state = State.STOPPED;
            statusListener.onShutdown(connName);
            log.info("Completed shutdown for {}", this);
        } catch (Throwable t) {
            log.error("{} Error while shutting down connector", this, t);
            onFailure(t);
        } finally {
            Utils.closeQuietly(ctx, "connector context for " + connName);
            Utils.closeQuietly(metrics, "connector metrics for " + connName);
            Utils.closeQuietly(offsetStorageReader, "offset reader for " + connName);
            if (offsetStore != null) {
                Utils.closeQuietly(offsetStore::stop, "offset backing store for " + connName);
            }
        }
    }

    public synchronized void cancel() {
        // Proactively update the status of the connector to UNASSIGNED since this connector
        // instance is being abandoned and we won't update the status on its behalf any more
        // after this since a new instance may be started soon
        statusListener.onShutdown(connName);
        Utils.closeQuietly(ctx, "connector context for " + connName);
        // Preemptively close the offset reader in case the connector is blocked on an offset read.
        Utils.closeQuietly(offsetStorageReader, "offset reader for " + connName);
        cancelled = true;
    }

    /**
     * Wait for this connector to finish shutting down.
     *
     * @param timeoutMs time in milliseconds to await shutdown
     * @return true if successful, false if the timeout was reached
     */
    public boolean awaitShutdown(long timeoutMs) {
        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public void transitionTo(TargetState targetState, Callback<TargetState> stateChangeCallback) {
        Callback<TargetState> preEmptedStateChangeCallback;
        TargetState preEmptedState;
        synchronized (this) {
            preEmptedStateChangeCallback = pendingStateChangeCallback.getAndSet(stateChangeCallback);
            preEmptedState = pendingTargetStateChange.getAndSet(targetState);
            notify();
        }
        if (preEmptedStateChangeCallback != null) {
            preEmptedStateChangeCallback.onCompletion(
                    new ConnectException(
                            "Could not begin changing connector state to " + preEmptedState.name()
                                + " before another request to change state was made;"
                                + " the new request (which is to change the state to " + targetState.name()
                                + ") has pre-empted this one"),
                    null
            );
        }
    }

    void doTransitionTo(TargetState targetState, Callback<TargetState> stateChangeCallback) {
        // Edge case: we don't do transitions most of the time if we've already failed, but for the STOPPED state, it's fine
        if (state == State.FAILED && targetState != TargetState.STOPPED) {
            stateChangeCallback.onCompletion(
                    new ConnectException(this + " Cannot transition connector to " + targetState + " since it has failed"),
                    null);
            return;
        }

        try {
            doTransitionTo(targetState);
            stateChangeCallback.onCompletion(null, targetState);
        } catch (Throwable t) {
            stateChangeCallback.onCompletion(
                    new ConnectException(
                            "Failed to transition connector " + connName + " to state " + targetState,
                            t),
                    null);
        }
    }

    private void doTransitionTo(TargetState targetState) {
        log.debug("{} Transition connector to {}", this, targetState);
        if (targetState == TargetState.PAUSED) {
            suspend(true);
        } else if (targetState == TargetState.STOPPED) {
            suspend(false);
        } else if (targetState == TargetState.STARTED) {
            if (state == State.INIT)
                start();
            else
                resume();
        } else {
            throw new IllegalArgumentException("Unhandled target state " + targetState);
        }
    }

    public final boolean isSinkConnector() {
        return ConnectUtils.isSinkConnector(connector);
    }

    public final boolean isSourceConnector() {
        return ConnectUtils.isSourceConnector(connector);
    }

    protected final String connectorType() {
        if (isSinkConnector())
            return "sink";
        if (isSourceConnector())
            return "source";
        return "unknown";
    }

    public Connector connector() {
        return connector;
    }

    ConnectorMetricsGroup metrics() {
        return metrics;
    }

    @Override
    public String toString() {
        return "WorkerConnector{" +
                       "id=" + connName +
                       '}';
    }

    class ConnectorMetricsGroup implements ConnectorStatus.Listener, AutoCloseable {
        /**
         * Use {@link AbstractStatus.State} since it has all of the states we want,
         * unlike {@link WorkerConnector.State}.
         */
        private volatile AbstractStatus.State state;
        private final MetricGroup metricGroup;
        private final ConnectorStatus.Listener delegate;

        public ConnectorMetricsGroup(ConnectMetrics connectMetrics, AbstractStatus.State initialState, ConnectorStatus.Listener delegate) {
            Objects.requireNonNull(connectMetrics);
            Objects.requireNonNull(connector);
            Objects.requireNonNull(initialState);
            Objects.requireNonNull(delegate);
            this.delegate = delegate;
            this.state = initialState;
            ConnectMetricsRegistry registry = connectMetrics.registry();
            this.metricGroup = connectMetrics.group(registry.connectorGroupName(),
                    registry.connectorTagName(), connName);
            // prevent collisions by removing any previously created metrics in this group.
            metricGroup.close();

            metricGroup.addImmutableValueMetric(registry.connectorType, connectorType());
            metricGroup.addImmutableValueMetric(registry.connectorClass, connector.getClass().getName());
            metricGroup.addImmutableValueMetric(registry.connectorVersion, connector.version());
            metricGroup.addValueMetric(registry.connectorStatus, now -> state.toString().toLowerCase(Locale.getDefault()));
        }

        public void close() {
            metricGroup.close();
        }

        @Override
        public void onStartup(String connector) {
            state = AbstractStatus.State.RUNNING;
            synchronized (this) {
                if (!cancelled) {
                    delegate.onStartup(connector);
                }
            }
        }

        @Override
        public void onShutdown(String connector) {
            state = AbstractStatus.State.UNASSIGNED;
            synchronized (this) {
                if (!cancelled) {
                    delegate.onShutdown(connector);
                }
            }
        }

        @Override
        public void onStop(String connector) {
            state = AbstractStatus.State.STOPPED;
            synchronized (this) {
                if (!cancelled) {
                    delegate.onStop(connector);
                }
            }
        }

        @Override
        public void onPause(String connector) {
            state = AbstractStatus.State.PAUSED;
            synchronized (this) {
                if (!cancelled) {
                    delegate.onPause(connector);
                }
            }
        }

        @Override
        public void onResume(String connector) {
            state = AbstractStatus.State.RUNNING;
            synchronized (this) {
                if (!cancelled) {
                    delegate.onResume(connector);
                }
            }
        }

        @Override
        public void onFailure(String connector, Throwable cause) {
            state = AbstractStatus.State.FAILED;
            synchronized (this) {
                if (!cancelled) {
                    delegate.onFailure(connector, cause);
                }
            }
        }

        @Override
        public void onDeletion(String connector) {
            state = AbstractStatus.State.DESTROYED;
            delegate.onDeletion(connector);
        }

        @Override
        public void onRestart(String connector) {
            state = AbstractStatus.State.RESTARTING;
            delegate.onRestart(connector);
        }

        boolean isUnassigned() {
            return state == AbstractStatus.State.UNASSIGNED;
        }

        boolean isRunning() {
            return state == AbstractStatus.State.RUNNING;
        }

        boolean isPaused() {
            return state == AbstractStatus.State.PAUSED;
        }

        boolean isStopped() {
            return state == AbstractStatus.State.STOPPED;
        }

        boolean isFailed() {
            return state == AbstractStatus.State.FAILED;
        }

        protected MetricGroup metricGroup() {
            return metricGroup;
        }
    }

    private abstract class WorkerConnectorContext implements ConnectorContext {

        @Override
        public void requestTaskReconfiguration() {
            WorkerConnector.this.ctx.requestTaskReconfiguration();
        }

        @Override
        public void raiseError(Exception e) {
            log.error("{} Connector raised an error", WorkerConnector.this, e);
            onFailure(e);
            WorkerConnector.this.ctx.raiseError(e);
        }
    }

    private class WorkerSinkConnectorContext extends WorkerConnectorContext implements SinkConnectorContext {
    }

    private class WorkerSourceConnectorContext extends WorkerConnectorContext implements SourceConnectorContext {

        private final OffsetStorageReader offsetStorageReader;

        WorkerSourceConnectorContext(OffsetStorageReader offsetStorageReader) {
            this.offsetStorageReader = offsetStorageReader;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return offsetStorageReader;
        }
    }
}

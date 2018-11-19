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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.runtime.ConnectMetrics.LiteralSupplier;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Container for connectors which is responsible for managing their lifecycle (e.g. handling startup,
 * shutdown, pausing, etc.). Internally, we manage the runtime state of the connector and transition according
 * to target state changes. Note that unlike connector tasks, the connector does not really have a "pause"
 * state which is distinct from being stopped. We therefore treat pause operations as requests to momentarily
 * stop the connector, and resume operations as requests to restart it (without reinitialization). Connector
 * failures, whether in initialization or after startup, are treated as fatal, which means that we will not attempt
 * to restart this connector instance after failure. What this means from a user perspective is that you must
 * use the /restart REST API to restart a failed task. This behavior is consistent with task failures.
 *
 * Note that this class is NOT thread-safe.
 */
public class WorkerConnector {
    private static final Logger log = LoggerFactory.getLogger(WorkerConnector.class);

    private enum State {
        INIT,    // initial state before startup
        STOPPED, // the connector has been stopped/paused.
        STARTED, // the connector has been started/resumed.
        FAILED,  // the connector has failed (no further transitions are possible after this state)
    }

    private final String connName;
    private final ConnectorStatus.Listener statusListener;
    private final ConnectorContext ctx;
    private final Connector connector;
    private final ConnectorMetricsGroup metrics;

    private Map<String, String> config;
    private State state;

    public WorkerConnector(String connName,
                           Connector connector,
                           ConnectorContext ctx,
                           ConnectMetrics metrics,
                           ConnectorStatus.Listener statusListener) {
        this.connName = connName;
        this.ctx = ctx;
        this.connector = connector;
        this.state = State.INIT;
        this.metrics = new ConnectorMetricsGroup(metrics, AbstractStatus.State.UNASSIGNED, statusListener);
        this.statusListener = this.metrics;
    }

    public void initialize(ConnectorConfig connectorConfig) {
        try {
            this.config = connectorConfig.originalsStrings();
            log.debug("{} Initializing connector {} with config {}", this, connName, config);
            if (isSinkConnector()) {
                SinkConnectorConfig.validate(config);
            }

            connector.initialize(new ConnectorContext() {
                @Override
                public void requestTaskReconfiguration() {
                    ctx.requestTaskReconfiguration();
                }

                @Override
                public void raiseError(Exception e) {
                    log.error("{} Connector raised an error", WorkerConnector.this, e);
                    onFailure(e);
                    ctx.raiseError(e);
                }
            });
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
            return false;
        }
    }

    private void onFailure(Throwable t) {
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

    @SuppressWarnings("fallthrough")
    private void pause() {
        try {
            switch (state) {
                case STOPPED:
                    return;

                case STARTED:
                    connector.stop();
                    // fall through

                case INIT:
                    statusListener.onPause(connName);
                    this.state = State.STOPPED;
                    break;

                default:
                    throw new IllegalArgumentException("Cannot pause connector in state " + state);
            }
        } catch (Throwable t) {
            log.error("{} Error while shutting down connector", this, t);
            statusListener.onFailure(connName, t);
            this.state = State.FAILED;
        }
    }

    public void shutdown() {
        try {
            if (state == State.STARTED)
                connector.stop();
            this.state = State.STOPPED;
            statusListener.onShutdown(connName);
        } catch (Throwable t) {
            log.error("{} Error while shutting down connector", this, t);
            this.state = State.FAILED;
            statusListener.onFailure(connName, t);
        } finally {
            metrics.close();
        }
    }

    public void transitionTo(TargetState targetState) {
        if (state == State.FAILED) {
            log.warn("{} Cannot transition connector to {} since it has failed", this, targetState);
            return;
        }

        log.debug("{} Transition connector to {}", this, targetState);
        if (targetState == TargetState.PAUSED) {
            pause();
        } else if (targetState == TargetState.STARTED) {
            if (state == State.INIT)
                start();
            else
                resume();
        } else {
            throw new IllegalArgumentException("Unhandled target state " + targetState);
        }
    }

    public boolean isSinkConnector() {
        return SinkConnector.class.isAssignableFrom(connector.getClass());
    }

    public boolean isSourceConnector() {
        return SourceConnector.class.isAssignableFrom(connector.getClass());
    }

    protected String connectorType() {
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
            metricGroup.addValueMetric(registry.connectorStatus, new LiteralSupplier<String>() {
                @Override
                public String metricValue(long now) {
                    return state.toString().toLowerCase(Locale.getDefault());
                }
            });
        }

        public void close() {
            metricGroup.close();
        }

        @Override
        public void onStartup(String connector) {
            state = AbstractStatus.State.RUNNING;
            delegate.onStartup(connector);
        }

        @Override
        public void onShutdown(String connector) {
            state = AbstractStatus.State.UNASSIGNED;
            delegate.onShutdown(connector);
        }

        @Override
        public void onPause(String connector) {
            state = AbstractStatus.State.PAUSED;
            delegate.onPause(connector);
        }

        @Override
        public void onResume(String connector) {
            state = AbstractStatus.State.RUNNING;
            delegate.onResume(connector);
        }

        @Override
        public void onFailure(String connector, Throwable cause) {
            state = AbstractStatus.State.FAILED;
            delegate.onFailure(connector, cause);
        }

        @Override
        public void onDeletion(String connector) {
            state = AbstractStatus.State.DESTROYED;
            delegate.onDeletion(connector);
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

        boolean isFailed() {
            return state == AbstractStatus.State.FAILED;
        }

        protected MetricGroup metricGroup() {
            return metricGroup;
        }
    }
}

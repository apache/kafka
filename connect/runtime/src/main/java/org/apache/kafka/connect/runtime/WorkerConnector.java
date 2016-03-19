/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WorkerConnector {
    private static final Logger log = LoggerFactory.getLogger(WorkerConnector.class);

    private enum State {
        INIT, STOPPED, STARTED, FAILED
    }

    private final String connName;
    private final ConnectorStatus.Listener statusListener;
    private final ConnectorContext ctx;
    private final Connector connector;

    private Map<String, String> config;
    private State state;

    public WorkerConnector(String connName,
                           Connector connector,
                           ConnectorContext ctx,
                           ConnectorStatus.Listener statusListener) {
        this.connName = connName;
        this.ctx = ctx;
        this.connector = connector;
        this.statusListener = statusListener;
        this.state = State.INIT;
    }

    public synchronized void initialize(Map<String, String> config) {
        log.debug("Initializing connector {} with config {}", connName, config);

        this.config = config;

        connector.initialize(new ConnectorContext() {
            @Override
            public void requestTaskReconfiguration() {
                ctx.requestTaskReconfiguration();
            }

            @Override
            public void raiseError(Exception e) {
                WorkerConnector.this.state = State.FAILED;
                ctx.raiseError(e);
            }
        });
    }

    private void start() {
        try {
            switch (state) {
                case STARTED:
                    return;

                case INIT:
                case FAILED:
                case STOPPED:
                    connector.start(config);
                    statusListener.onStartup(connName);
                    this.state = State.STARTED;
                    break;

                default:
                    throw new IllegalArgumentException("Cannot start connector in state " + state);
            }
        } catch (Throwable t) {
            log.error("Error while starting connector {}", connName, t);
            statusListener.onFailure(connName, t);
            this.state = State.FAILED;
        }
    }

    private void pause() {
        try {
            switch (state) {
                case STOPPED:
                    return;

                case STARTED:
                    connector.stop();
                    // fall through

                case INIT:
                case FAILED:
                    statusListener.onPause(connName);
                    this.state = State.STOPPED;
                    break;

                default:
                    throw new IllegalArgumentException("Cannot pause connector in state " + state);
            }
        } catch (Throwable t) {
            log.error("Error while shutting down connector {}", connName, t);
            statusListener.onFailure(connName, t);
            this.state = State.FAILED;
        }
    }

    public synchronized void shutdown() {
        try {
            if (state == State.STARTED)
                connector.stop();
            this.state = State.STOPPED;
        } catch (Throwable t) {
            log.error("Error while shutting down connector {}", connName, t);
            this.state = State.FAILED;
        } finally {
            statusListener.onShutdown(connName);
        }
    }

    public synchronized void transitionTo(TargetState targetState) {
        log.debug("Transition connector {} to {}", connName, targetState);
        if (targetState == TargetState.PAUSED) {
            pause();
        } else if (targetState == TargetState.STARTED) {
            start();
        } else {
            throw new IllegalArgumentException("Unhandled target state " + targetState);
        }
    }

    public boolean isSinkConnector() {
        return SinkConnector.class.isAssignableFrom(connector.getClass());
    }

    public Connector connector() {
        return connector;
    }

    @Override
    public String toString() {
        return "WorkerConnector{" +
                "connName='" + connName + '\'' +
                ", connector=" + connector +
                '}';
    }
}

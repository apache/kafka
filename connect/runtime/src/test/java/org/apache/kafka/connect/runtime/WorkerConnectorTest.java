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
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.expectLastCall;

@RunWith(EasyMockRunner.class)
public class WorkerConnectorTest extends EasyMockSupport {

    public static final String CONNECTOR = "connector";
    public static final Map<String, String> CONFIG = new HashMap<>();
    static {
        CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestConnector.class.getName());
        CONFIG.put(ConnectorConfig.NAME_CONFIG, CONNECTOR);
    }
    public static final ConnectorConfig CONNECTOR_CONFIG = new ConnectorConfig(CONFIG);

    @Mock Connector connector;
    @Mock ConnectorContext ctx;
    @Mock ConnectorStatus.Listener listener;

    @Test
    public void testInitializeFailure() {
        RuntimeException exception = new RuntimeException();

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testFailureIsFinalState() {
        RuntimeException exception = new RuntimeException();

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        // expect no call to onStartup() after failure

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testStartupAndShutdown() {
        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testStartupAndPause() {
        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onPause(CONNECTOR);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.transitionTo(TargetState.PAUSED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testOnResume() {
        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        listener.onPause(CONNECTOR);
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onResume(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.PAUSED);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testStartupPaused() {
        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        // connector never gets started

        listener.onPause(CONNECTOR);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.PAUSED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testStartupFailure() {
        RuntimeException exception = new RuntimeException();

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testShutdownFailure() {
        RuntimeException exception = new RuntimeException();

        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall().andThrow(exception);

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testTransitionStartedToStarted() {
        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        // expect only one call to onStartup()
        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.shutdown();

        verifyAll();
    }

    @Test
    public void testTransitionPausedToPaused() {
        connector.initialize(EasyMock.notNull(ConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onPause(CONNECTOR);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, ctx, listener);

        workerConnector.initialize(CONNECTOR_CONFIG);
        workerConnector.transitionTo(TargetState.STARTED);
        workerConnector.transitionTo(TargetState.PAUSED);
        workerConnector.transitionTo(TargetState.PAUSED);
        workerConnector.shutdown();

        verifyAll();
    }

    private static abstract class TestConnector extends Connector {
    }

}

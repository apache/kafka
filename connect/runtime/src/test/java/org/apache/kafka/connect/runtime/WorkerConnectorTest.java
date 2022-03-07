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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.Capture;
import org.apache.kafka.connect.util.Callback;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
public class WorkerConnectorTest extends EasyMockSupport {

    private static final String VERSION = "1.1";
    public static final String CONNECTOR = "connector";
    public static final Map<String, String> CONFIG = new HashMap<>();
    static {
        CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestConnector.class.getName());
        CONFIG.put(ConnectorConfig.NAME_CONFIG, CONNECTOR);
        CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, "my-topic");
    }
    public ConnectorConfig connectorConfig;
    public MockConnectMetrics metrics;

    @Mock Plugins plugins;
    @Mock SourceConnector sourceConnector;
    @Mock SinkConnector sinkConnector;
    @Mock Connector connector;
    @Mock CloseableConnectorContext ctx;
    @Mock ConnectorStatus.Listener listener;
    @Mock OffsetStorageReader offsetStorageReader;
    @Mock ClassLoader classLoader;

    @Before
    public void setup() {
        connectorConfig = new ConnectorConfig(plugins, CONFIG);
        metrics = new MockConnectMetrics();
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testInitializeFailure() {
        RuntimeException exception = new RuntimeException();
        connector = sourceConnector;

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SourceConnectorContext.class));
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        ctx.close();
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testFailureIsFinalState() {
        RuntimeException exception = new RuntimeException();
        connector = sinkConnector;

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SinkConnectorContext.class));
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        // expect no call to onStartup() after failure

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.anyObject(Exception.class), EasyMock.isNull());
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertFailedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupAndShutdown() {
        connector = sourceConnector;
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SourceConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.STARTED));
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupAndPause() {
        connector = sinkConnector;
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SinkConnectorContext.class));
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

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.STARTED));
        expectLastCall();
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.PAUSED));
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSinkMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testOnResume() {
        connector = sourceConnector;
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SourceConnectorContext.class));
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

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.PAUSED));
        expectLastCall();
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.STARTED));
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupPaused() {
        connector = sinkConnector;
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SinkConnectorContext.class));
        expectLastCall();

        // connector never gets started

        listener.onPause(CONNECTOR);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.PAUSED));
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSinkMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testStartupFailure() {
        RuntimeException exception = new RuntimeException();

        connector = sinkConnector;
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SinkConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall().andThrow(exception);

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        listener.onShutdown(CONNECTOR);
        expectLastCall();

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.anyObject(Exception.class), EasyMock.isNull());
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSinkMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testShutdownFailure() {
        RuntimeException exception = new RuntimeException();
        connector = sourceConnector;

        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SourceConnectorContext.class));
        expectLastCall();

        connector.start(CONFIG);
        expectLastCall();

        listener.onStartup(CONNECTOR);
        expectLastCall();

        connector.stop();
        expectLastCall().andThrow(exception);

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.STARTED));
        expectLastCall();

        listener.onFailure(CONNECTOR, exception);
        expectLastCall();

        ctx.close();
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertFailedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testTransitionStartedToStarted() {
        connector = sourceConnector;
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SourceConnectorContext.class));
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

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.STARTED));
        expectLastCall().times(2);

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testTransitionPausedToPaused() {
        connector = sourceConnector;
        connector.version();
        expectLastCall().andReturn(VERSION);

        connector.initialize(EasyMock.notNull(SourceConnectorContext.class));
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

        ctx.close();
        expectLastCall();

        Callback<TargetState> onStateChange = createStrictMock(Callback.class);
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.STARTED));
        expectLastCall();
        onStateChange.onCompletion(EasyMock.isNull(), EasyMock.eq(TargetState.PAUSED));
        expectLastCall().times(2);

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verifyAll();
    }

    @Test
    public void testFailConnectorThatIsNeitherSourceNorSink() {
        connector.version();
        expectLastCall().andReturn(VERSION);

        Capture<Throwable> exceptionCapture = Capture.newInstance();
        listener.onFailure(EasyMock.eq(CONNECTOR), EasyMock.capture(exceptionCapture));
        expectLastCall();

        replayAll();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, classLoader);

        workerConnector.initialize();
        Throwable e = exceptionCapture.getValue();
        assertTrue(e instanceof ConnectException);
        assertTrue(e.getMessage().contains("must be a subclass of"));

        verifyAll();
    }

    protected void assertFailedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertTrue(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertPausedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertTrue(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertRunningMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertTrue(workerConnector.metrics().isRunning());
    }

    protected void assertStoppedMetric(WorkerConnector workerConnector) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertInitializedSinkMetric(WorkerConnector workerConnector) {
        assertInitializedMetric(workerConnector, "sink");
    }

    protected void assertInitializedSourceMetric(WorkerConnector workerConnector) {
        assertInitializedMetric(workerConnector, "source");
    }

    protected void assertInitializedMetric(WorkerConnector workerConnector, String expectedType) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
        MetricGroup metricGroup = workerConnector.metrics().metricGroup();
        String status = metrics.currentMetricValueAsString(metricGroup, "status");
        String type = metrics.currentMetricValueAsString(metricGroup, "connector-type");
        String clazz = metrics.currentMetricValueAsString(metricGroup, "connector-class");
        String version = metrics.currentMetricValueAsString(metricGroup, "connector-version");
        assertEquals(expectedType, type);
        assertNotNull(clazz);
        assertEquals(VERSION, version);
    }

    private static abstract class TestConnector extends Connector {
    }
}

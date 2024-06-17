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
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class WorkerConnectorTest {

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

    @Rule
    public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    @Mock private Plugins plugins;
    @Mock private CloseableConnectorContext ctx;
    @Mock private ConnectorStatus.Listener listener;
    @Mock private ClassLoader classLoader;

    private final ConnectorType connectorType;
    private final Connector connector;
    private final CloseableOffsetStorageReader offsetStorageReader;
    private final ConnectorOffsetBackingStore offsetStore;

    @Parameterized.Parameters
    public static Collection<ConnectorType> parameters() {
        return Arrays.asList(ConnectorType.SOURCE, ConnectorType.SINK);
    }

    public WorkerConnectorTest(ConnectorType connectorType) {
        this.connectorType = connectorType;
        switch (connectorType) {
            case SINK:
                this.connector = mock(SinkConnector.class);
                this.offsetStorageReader = null;
                this.offsetStore = null;
                break;
            case SOURCE:
                this.connector = mock(SourceConnector.class);
                this.offsetStorageReader = mock(CloseableOffsetStorageReader.class);
                this.offsetStore = mock(ConnectorOffsetBackingStore.class);
                break;
            default:
                throw new IllegalStateException("Unexpected connector type: " + connectorType);
        }
    }

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

        when(connector.version()).thenReturn(VERSION);
        doThrow(exception).when(connector).initialize(any());

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(listener).onFailure(CONNECTOR, exception);
        verifyCleanShutdown(false);
    }

    @Test
    public void testFailureIsFinalState() {
        RuntimeException exception = new RuntimeException();

        when(connector.version()).thenReturn(VERSION);
        doThrow(exception).when(connector).initialize(any());

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertFailedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(listener).onFailure(CONNECTOR, exception);
        // expect no call to onStartup() after failure
        verifyCleanShutdown(false);

        verify(onStateChange).onCompletion(any(Exception.class), isNull());
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupAndShutdown() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verifyCleanShutdown(true);

        verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupAndPause() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();

        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(listener).onPause(CONNECTOR);
        verifyCleanShutdown(true);

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.PAUSED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupAndStop() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);

        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STOPPED, onStateChange);
        assertStoppedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(listener).onStop(CONNECTOR);
        verifyCleanShutdown(true);

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STOPPED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testOnResume() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(listener).onPause(CONNECTOR);
        verify(connector).start(CONFIG);
        verify(listener).onResume(CONNECTOR);
        verifyCleanShutdown(true);

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.PAUSED));
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupPaused() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        // connector never gets started
        verify(listener).onPause(CONNECTOR);
        verifyCleanShutdown(false);

        verify(onStateChange).onCompletion(isNull(), eq(TargetState.PAUSED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupStopped() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STOPPED, onStateChange);
        assertStoppedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        // connector never gets started
        verify(listener).onStop(CONNECTOR);
        verifyCleanShutdown(false);

        verify(onStateChange).onCompletion(isNull(), eq(TargetState.STOPPED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupFailure() {
        RuntimeException exception = new RuntimeException();

        when(connector.version()).thenReturn(VERSION);
        doThrow(exception).when(connector).start(CONFIG);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        verify(listener).onFailure(CONNECTOR, exception);
        verifyCleanShutdown(false);

        verify(onStateChange).onCompletion(any(Exception.class), isNull());
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStopFailure() {
        RuntimeException exception = new RuntimeException();

        when(connector.version()).thenReturn(VERSION);

        // Fail during the first call to stop, then succeed for the next attempt
        doThrow(exception).doNothing().when(connector).stop();

        Callback<TargetState> onFirstStateChange = mockCallback();
        Callback<TargetState> onSecondStateChange = mockCallback();
        Callback<TargetState> onThirdStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onFirstStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STOPPED, onSecondStateChange);
        assertStoppedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onThirdStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector, times(2)).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(listener).onResume(CONNECTOR);
        verify(listener).onStop(CONNECTOR);
        verify(onFirstStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onFirstStateChange);
        // We swallow failures when transitioning to the STOPPED state
        verify(onSecondStateChange).onCompletion(isNull(), eq(TargetState.STOPPED));
        verifyNoMoreInteractions(onSecondStateChange);
        verify(onThirdStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onThirdStateChange);
        verifyShutdown(2, true, true);
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testShutdownFailure() {
        RuntimeException exception = new RuntimeException();

        when(connector.version()).thenReturn(VERSION);

        doThrow(exception).when(connector).stop();

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertFailedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
        verify(listener).onFailure(CONNECTOR, exception);
        verifyShutdown(false, true);
    }

    @Test
    public void testTransitionStartedToStarted() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        // expect only one call to onStartup()
        verify(listener).onStartup(CONNECTOR);
        verifyCleanShutdown(true);
        verify(onStateChange, times(2)).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testTransitionPausedToPaused() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(listener).onPause(CONNECTOR);
        verifyCleanShutdown(true);

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        inOrder.verify(onStateChange, times(2)).onCompletion(isNull(), eq(TargetState.PAUSED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testTransitionStoppedToStopped() {
        when(connector.version()).thenReturn(VERSION);

        Callback<TargetState> onStateChange = mockCallback();
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STOPPED, onStateChange);
        assertStoppedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STOPPED, onStateChange);
        assertStoppedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertDestroyedMetric(workerConnector);

        verifyInitialize();
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(listener).onStop(CONNECTOR);
        verifyCleanShutdown(true);

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        inOrder.verify(onStateChange, times(2)).onCompletion(isNull(), eq(TargetState.STOPPED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testFailConnectorThatIsNeitherSourceNorSink() {
        Connector badConnector = mock(Connector.class);
        when(badConnector.version()).thenReturn(VERSION);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, badConnector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();

        verify(badConnector).version();
        ArgumentCaptor<Throwable> exceptionCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).onFailure(eq(CONNECTOR), exceptionCapture.capture());
        Throwable e = exceptionCapture.getValue();
        assertInstanceOf(ConnectException.class, e);
        assertTrue(e.getMessage().contains("must be a subclass of"));
    }

    protected void assertFailedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertTrue(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isStopped());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertStoppedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertTrue(workerConnector.metrics().isStopped());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertPausedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isStopped());
        assertTrue(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertRunningMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isStopped());
        assertFalse(workerConnector.metrics().isPaused());
        assertTrue(workerConnector.metrics().isRunning());
    }

    protected void assertDestroyedMetric(WorkerConnector workerConnector) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isStopped());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertInitializedMetric(WorkerConnector workerConnector) {
        String expectedType;
        switch (connectorType) {
            case SINK:
                expectedType = "sink";
                break;
            case SOURCE:
                expectedType = "source";
                break;
            default:
                throw new IllegalStateException("Unexpected connector type: " + connectorType);
        }
        assertInitializedMetric(workerConnector, expectedType);
    }

    protected void assertInitializedMetric(WorkerConnector workerConnector, String expectedType) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isStopped());
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

    @SuppressWarnings("unchecked")
    private Callback<TargetState> mockCallback() {
        return mock(Callback.class);
    }

    private void verifyInitialize() {
        verify(connector).version();
        if (connectorType == ConnectorType.SOURCE) {
            verify(offsetStore).start();
            verify(connector).initialize(any(SourceConnectorContext.class));
        } else if (connectorType == ConnectorType.SINK) {
            verify(connector).initialize(any(SinkConnectorContext.class));
        }
    }

    private void verifyCleanShutdown(boolean started) {
        verifyShutdown(true, started);
    }

    private void verifyShutdown(boolean clean, boolean started) {
        verifyShutdown(1, clean, started);
    }

    private void verifyShutdown(int connectorStops, boolean clean, boolean started) {
        verify(ctx).close();
        if (connectorType == ConnectorType.SOURCE) {
            verify(offsetStorageReader).close();
            verify(offsetStore).stop();
        }
        if (clean) {
            verify(listener).onShutdown(CONNECTOR);
        }
        if (started) {
            verify(connector, times(connectorStops)).stop();
        }
    }

    private static abstract class TestConnector extends Connector {
    }
}

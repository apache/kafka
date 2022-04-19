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

import java.util.Collection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.MockFileConfigProvider;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.MockConnectMetrics.MockMetricsReporter;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.ParameterizedTest;
import org.apache.kafka.connect.util.ThreadedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoSession;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.mockito.quality.Strictness;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstructionWithAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class WorkerTest extends ThreadedTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);
    private static final String WORKER_ID = "localhost:8083";
    private static final String CLUSTER_ID = "test-cluster";
    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();
    private final ConnectorClientConfigOverridePolicy allConnectorClientConfigOverridePolicy = new AllConnectorClientConfigOverridePolicy();

    private final Map<String, String> workerProps = new HashMap<>();
    private WorkerConfig config;
    private Worker worker;

    private final Map<String, String> defaultProducerConfigs = new HashMap<>();
    private final Map<String, String> defaultConsumerConfigs = new HashMap<>();

    @Mock
    private Plugins plugins;

    @Mock
    private PluginClassLoader pluginLoader;

    @Mock
    private DelegatingClassLoader delegatingLoader;

    @Mock
    private OffsetBackingStore offsetBackingStore;

    @Mock
    private TaskStatus.Listener taskStatusListener;

    @Mock
    private ConnectorStatus.Listener connectorStatusListener;

    @Mock
    private Herder herder;

    @Mock
    private StatusBackingStore statusBackingStore;

    @Mock
    private SourceConnector sourceConnector;

    @Mock
    private SinkConnector sinkConnector;

    @Mock
    private CloseableConnectorContext ctx;

    @Mock private TestSourceTask task;
    @Mock private Converter taskKeyConverter;
    @Mock private Converter taskValueConverter;
    @Mock private HeaderConverter taskHeaderConverter;
    @Mock private ExecutorService executorService;
    @Mock private ConnectorConfig connectorConfig;
    private String mockFileProviderTestId;
    private Map<String, String> connectorProps;

    private final boolean enableTopicCreation;

    private MockedStatic<Plugins> pluginsMockedStatic;
    private MockedStatic<ConnectUtils> connectUtilsMockedStatic;
    private MockedConstruction<WorkerSourceTask> sourceTaskMockedConstruction;
    private MockitoSession mockitoSession;

    @ParameterizedTest.Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public WorkerTest(boolean enableTopicCreation) {
        this.enableTopicCreation = enableTopicCreation;
    }

    @Before
    public void setup() {
        super.setup();

        // Use strict mode to detect unused mocks
        mockitoSession = Mockito.mockitoSession()
                                .initMocks(this)
                                .strictness(Strictness.STRICT_STUBS)
                                .startMocking();

        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        workerProps.put("config.providers", "file");
        workerProps.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        mockFileProviderTestId = UUID.randomUUID().toString();
        workerProps.put("config.providers.file.param.testId", mockFileProviderTestId);
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(enableTopicCreation));
        config = new StandaloneConfig(workerProps);

        defaultProducerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        defaultProducerConfigs.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultProducerConfigs.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultProducerConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        // By default, producers that are instantiated and used by Connect have idempotency disabled even after idempotency became
        // default for Kafka producers. This is chosen to avoid breaking changes when Connect contacts Kafka brokers that do not support
        // idempotent producers or require explicit steps to enable them (e.g. adding the IDEMPOTENT_WRITE ACL to brokers older than 2.8).
        // These settings might change when https://cwiki.apache.org/confluence/display/KAFKA/KIP-318%3A+Make+Kafka+Connect+Source+idempotent
        // gets approved and scheduled for release.
        defaultProducerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        defaultProducerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
        defaultProducerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        defaultProducerConfigs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));

        defaultConsumerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        defaultConsumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        defaultConsumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        defaultConsumerConfigs
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        defaultConsumerConfigs
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Some common defaults. They might change on individual tests
        connectorProps = anyConnectorConfigMap();

        pluginsMockedStatic = mockStatic(Plugins.class);

        // pass through things that aren't explicitly mocked out
        connectUtilsMockedStatic = mockStatic(ConnectUtils.class, new CallsRealMethods());
        connectUtilsMockedStatic.when(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class))).thenReturn(CLUSTER_ID);

        // Make calls to new WorkerSourceTask() return a mock to avoid the source task trying to connect to a broker.
        sourceTaskMockedConstruction = mockConstructionWithAnswer(WorkerSourceTask.class, invocation -> {

            // provide implementations of three methods used during testing
            switch (invocation.getMethod().getName()) {
                case "id":
                    return TASK_ID;
                case "loader":
                    return pluginLoader;
                case "awaitStop":
                    return true;
                default:
                    return null;
            }
        });
    }

    @After
    public void teardown() {
        // Critical to always close MockedStatics
        // Ideal would be to use try-with-resources in an individual test, but it introduced a rather large level of
        // indentation of most test bodies, hence sticking with setup() / teardown()
        pluginsMockedStatic.close();
        connectUtilsMockedStatic.close();
        sourceTaskMockedConstruction.close();

        mockitoSession.finishMocking();
    }

    @Test
    public void testStartAndStopConnector() throws Throwable {

        final String connectorClass = SampleSourceConnector.class.getName();
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);

        // Create
        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(connectorClass)).thenReturn(pluginLoader);
        when(plugins.newConnector(connectorClass)).thenReturn(sourceConnector);
        when(sourceConnector.version()).thenReturn("1.0");

        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);
        connectUtilsMockedStatic.when(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)))
                                .thenReturn(CLUSTER_ID);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());

        FutureCallback<TargetState> onFirstStart = new FutureCallback<>();

        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onFirstStart);

        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onFirstStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());


        FutureCallback<TargetState> onSecondStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onSecondStart);
        try {
            onSecondStart.get(0, TimeUnit.MILLISECONDS);
            fail("Should have failed while trying to start second connector with same name");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ConnectException.class));
        }

        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        worker.stopAndAwaitConnector(CONNECTOR_ID);

        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());

        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);


        verify(plugins, times(2)).currentThreadLoader();
        verify(plugins).delegatingLoader();
        verify(delegatingLoader).connectorLoader(connectorClass);
        verify(plugins).newConnector(connectorClass);
        verify(sourceConnector, times(2)).version();
        verify(sourceConnector).initialize(any(ConnectorContext.class));
        verify(sourceConnector).start(connectorProps);
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);

        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(pluginLoader), times(2));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        connectUtilsMockedStatic.verify(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)));

        verify(sourceConnector).stop();
        verify(connectorStatusListener).onShutdown(CONNECTOR_ID);
        verify(ctx).close();
        MockFileConfigProvider.assertClosed(mockFileProviderTestId);
    }

    private void mockFileConfigProvider() {
        MockFileConfigProvider mockFileConfigProvider = new MockFileConfigProvider();
        mockFileConfigProvider.configure(Collections.singletonMap("testId", mockFileProviderTestId));
        when(plugins.newConfigProvider(any(AbstractConfig.class),
                                       eq("config.providers.file"),
                                       any(ClassLoaderUsage.class)))
               .thenReturn(mockFileConfigProvider);
    }

    @Test
    public void testStartConnectorFailure() throws Exception {

        final String nonConnectorClass = "java.util.HashMap";
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, nonConnectorClass); // Bad connector class name

        Exception exception = new ConnectException("Failed to find Connector");

        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(nonConnectorClass)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(delegatingLoader);
        connectUtilsMockedStatic.when(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)))
                                .thenReturn("test-cluster");

        when(plugins.newConnector(anyString())).thenThrow(exception);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        try {
            onStart.get(0, TimeUnit.MILLISECONDS);
            fail("Should have failed to start connector");
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }

        assertStartupStatistics(worker, 1, 1, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());

        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 1, 0, 0);
        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 1, 0, 0);

        verify(plugins).currentThreadLoader();
        verify(plugins).delegatingLoader();
        verify(plugins).delegatingLoader();
        verify(delegatingLoader).connectorLoader(nonConnectorClass);
        verify(plugins).newConnector(anyString());
        verify(connectorStatusListener).onFailure(eq(CONNECTOR_ID), any(ConnectException.class));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        connectUtilsMockedStatic.verify(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)));
    }

    @Test
    public void testAddConnectorByAlias() throws Throwable {

        final String connectorAlias = "SampleSourceConnector";

        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(plugins.newConnector(connectorAlias)).thenReturn(sinkConnector);
        when(delegatingLoader.connectorLoader(connectorAlias)).thenReturn(pluginLoader);
        when(sinkConnector.version()).thenReturn("1.0");
        
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);
        connectUtilsMockedStatic.when(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)))
                                .thenReturn("test-cluster");

        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorAlias);
        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "gfieyls, wfru");

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());
        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);

        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);

        verify(plugins, times(2)).currentThreadLoader();
        verify(plugins).delegatingLoader();
        verify(plugins).newConnector(connectorAlias);
        verify(delegatingLoader).connectorLoader(connectorAlias);
        verify(sinkConnector, times(2)).version();
        verify(sinkConnector).initialize(any(ConnectorContext.class));
        verify(sinkConnector).start(connectorProps);
        verify(sinkConnector).stop();
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);
        verify(ctx).close();

        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(pluginLoader), times(2));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        connectUtilsMockedStatic.verify(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)));
    }

    @Test
    public void testAddConnectorByShortAlias() throws Throwable {

        final String shortConnectorAlias = "WorkerTest";

        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(plugins.newConnector(shortConnectorAlias)).thenReturn(sinkConnector);
        when(delegatingLoader.connectorLoader(shortConnectorAlias)).thenReturn(pluginLoader);
        when(sinkConnector.version()).thenReturn("1.0");
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, shortConnectorAlias);
        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "gfieyls, wfru");

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());
        assertStatistics(worker, 1, 0);

        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verify(plugins, times(2)).currentThreadLoader();
        verify(plugins).delegatingLoader();
        verify(plugins).newConnector(shortConnectorAlias);
        verify(sinkConnector, times(2)).version();
        verify(sinkConnector).initialize(any(ConnectorContext.class));
        verify(sinkConnector).start(connectorProps);
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);
        verify(sinkConnector).stop();
        verify(connectorStatusListener).onShutdown(CONNECTOR_ID);
        verify(ctx).close();

        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        connectUtilsMockedStatic.verify(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)));
    }

    @Test
    public void testStopInvalidConnector() {
        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        worker.stopAndAwaitConnector(CONNECTOR_ID);

        verifyConverters();
    }

    @Test
    public void testReconfigureConnectorTasks() throws Throwable {
        final String connectorClass = SampleSourceConnector.class.getName();

        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(connectorClass)).thenReturn(pluginLoader);
        when(plugins.newConnector(connectorClass)).thenReturn(sinkConnector);
        when(sinkConnector.version()).thenReturn("1.0");

        Map<String, String> taskProps = Collections.singletonMap("foo", "bar");
        when(sinkConnector.taskConfigs(2)).thenReturn(Arrays.asList(taskProps, taskProps));

        // Use doReturn().when() syntax due to when().thenReturn() not being able to return wildcard generic types
        doReturn(TestSourceTask.class).when(sinkConnector).taskClass();

        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);


        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onFirstStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onFirstStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onFirstStart.get(1000, TimeUnit.MILLISECONDS));
        assertStatistics(worker, 1, 0);
        assertEquals(Collections.singleton(CONNECTOR_ID), worker.connectorNames());

        FutureCallback<TargetState> onSecondStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onSecondStart);
        try {
            onSecondStart.get(0, TimeUnit.MILLISECONDS);
            fail("Should have failed while trying to start second connector with same name");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ConnectException.class));
        }

        Map<String, String> connProps = new HashMap<>(connectorProps);
        connProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "2");
        ConnectorConfig connConfig = new SinkConnectorConfig(plugins, connProps);
        List<Map<String, String>> taskConfigs = worker.connectorTaskConfigs(CONNECTOR_ID, connConfig);
        Map<String, String> expectedTaskProps = new HashMap<>();
        expectedTaskProps.put("foo", "bar");
        expectedTaskProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        expectedTaskProps.put(SinkTask.TOPICS_CONFIG, "foo,bar");
        assertEquals(2, taskConfigs.size());
        assertEquals(expectedTaskProps, taskConfigs.get(0));
        assertEquals(expectedTaskProps, taskConfigs.get(1));
        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verify(plugins, times(3)).currentThreadLoader();
        verify(plugins).delegatingLoader();
        verify(delegatingLoader).connectorLoader(connectorClass);
        verify(plugins).newConnector(connectorClass);
        verify(sinkConnector, times(2)).version();
        verify(sinkConnector).initialize(any(ConnectorContext.class));
        verify(sinkConnector).start(connectorProps);
        verify(connectorStatusListener).onStartup(CONNECTOR_ID);
        verify(sinkConnector).taskClass();
        verify(sinkConnector).taskConfigs(2);
        verify(sinkConnector).stop();
        verify(connectorStatusListener).onShutdown(CONNECTOR_ID);
        verify(ctx).close();

        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(pluginLoader), times(3));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(3));
    }

    @Test
    public void testAddRemoveTask() {
        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(SampleSourceConnector.class.getName())).thenReturn(pluginLoader);

        when(plugins.newTask(TestSourceTask.class)).thenReturn(task);
        when(task.version()).thenReturn("1.0");
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);
        when(executorService.submit(any(WorkerSourceTask.class))).thenReturn(null);
        doReturn(SampleSourceConnector.class).when(plugins).connectorClass(SampleSourceConnector.class.getName());
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(Collections.singleton(TASK_ID), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        verify(plugins, times(2)).currentThreadLoader();
        verify(plugins).newTask(TestSourceTask.class);
        verify(task).version();
        verifyTaskConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG);
        verifyTaskConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG);
        verifyTaskHeaderConverter();

        verify(executorService).submit(any(WorkerSourceTask.class));
        verify(plugins).delegatingLoader();
        verify(delegatingLoader).connectorLoader(SampleSourceConnector.class.getName());
        verify(plugins).connectorClass(SampleSourceConnector.class.getName());

        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(pluginLoader), times(2));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        connectUtilsMockedStatic.verify(() -> ConnectUtils.lookupKafkaClusterId(any(WorkerConfig.class)));

    }

    @Test
    public void testTaskStatusMetricsStatuses() {
        mockInternalConverters();
        mockStorage();
        mockFileConfigProvider();



        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);

        when(plugins.newTask(TestSourceTask.class)).thenReturn(task);
        when(task.version()).thenReturn("1.0");

        // Expect that the worker will create converters and will find them using the current classloader ...
        assertNotNull(taskKeyConverter);
        assertNotNull(taskValueConverter);
        assertNotNull(taskHeaderConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);

        when(executorService.submit(any(WorkerSourceTask.class))).thenReturn(null);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(SampleSourceConnector.class.getName())).thenReturn(pluginLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);

        doReturn(SampleSourceConnector.class).when(plugins).connectorClass(SampleSourceConnector.class.getName());



        // Each time we check the task metrics, the worker will call the herder
        when(herder.taskStatus(TASK_ID)).thenReturn(
                new ConnectorStateInfo.TaskState(0, "RUNNING", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "PAUSED", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "FAILED", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "DESTROYED", "worker", "msg"),
                new ConnectorStateInfo.TaskState(0, "UNASSIGNED", "worker", "msg")
        );

        worker = new Worker(WORKER_ID,
            new MockTime(),
            plugins,
            config,
            offsetBackingStore,
            executorService,
            noneConnectorClientConfigOverridePolicy);

        worker.herder = herder;

        worker.start();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startTask(
            TASK_ID,
            ClusterConfigState.EMPTY,
            anyConnectorConfigMap(),
            origProps,
            taskStatusListener,
            TargetState.STARTED);

        assertStatusMetrics(1L, "connector-running-task-count");
        assertStatusMetrics(1L, "connector-paused-task-count");
        assertStatusMetrics(1L, "connector-failed-task-count");
        assertStatusMetrics(1L, "connector-destroyed-task-count");
        assertStatusMetrics(1L, "connector-unassigned-task-count");

        worker.stopAndAwaitTask(TASK_ID);
        assertStatusMetrics(0L, "connector-running-task-count");
        assertStatusMetrics(0L, "connector-paused-task-count");
        assertStatusMetrics(0L, "connector-failed-task-count");
        assertStatusMetrics(0L, "connector-destroyed-task-count");
        assertStatusMetrics(0L, "connector-unassigned-task-count");

        WorkerSourceTask instantiatedTask = sourceTaskMockedConstruction.constructed().get(0);
        verify(instantiatedTask).initialize(taskConfig);
        verify(herder, times(5)).taskStatus(TASK_ID);
        verify(plugins).delegatingLoader();
        verify(delegatingLoader).connectorLoader(SampleSourceConnector.class.getName());
        verify(executorService).submit(instantiatedTask);
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(pluginLoader), times(2));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        verify(plugins).connectorClass(SampleSourceConnector.class.getName());
        verify(instantiatedTask, atLeastOnce()).id();
        verify(instantiatedTask).awaitStop(anyLong());
        verify(instantiatedTask).removeMetrics();

        // Called when we stop the worker
        verify(instantiatedTask).loader();
        verify(instantiatedTask).stop();
        verifyTaskConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG);
        verifyTaskConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG);
        verifyTaskHeaderConverter();
        verify(plugins, times(2)).currentThreadLoader();

    }

    @Test
    public void testConnectorStatusMetricsGroup_taskStatusCounter() {
        ConcurrentMap<ConnectorTaskId, WorkerTask> tasks = new ConcurrentHashMap<>();
        tasks.put(new ConnectorTaskId("c1", 0), mock(WorkerSourceTask.class));
        tasks.put(new ConnectorTaskId("c1", 1), mock(WorkerSourceTask.class));
        tasks.put(new ConnectorTaskId("c2", 0), mock(WorkerSourceTask.class));

        mockInternalConverters();
        mockFileConfigProvider();

        connectUtilsMockedStatic.when(() -> ConnectUtils.lookupKafkaClusterId(any())).thenReturn(CLUSTER_ID);

        worker = new Worker(WORKER_ID,
            new MockTime(),
            plugins,
            config,
            offsetBackingStore,
            noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;

        Worker.ConnectorStatusMetricsGroup metricGroup = new Worker.ConnectorStatusMetricsGroup(
            worker.metrics(), tasks, herder
        );
        assertEquals(2L, (long) metricGroup.taskCounter("c1").metricValue(0L));
        assertEquals(1L, (long) metricGroup.taskCounter("c2").metricValue(0L));
        assertEquals(0L, (long) metricGroup.taskCounter("fakeConnector").metricValue(0L));

        connectUtilsMockedStatic.verify(() -> ConnectUtils.lookupKafkaClusterId(any()));
    }

    @Test
    public void testStartTaskFailure() {
        mockInternalConverters();
        mockFileConfigProvider();

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, "missing.From.This.Workers.Classpath");

        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(SampleSourceConnector.class.getName())).thenReturn(pluginLoader);

        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 0, 0);

        assertFalse(worker.startTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED));
        assertStartupStatistics(worker, 0, 0, 1, 1);

        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 1, 1);
        assertEquals(Collections.emptySet(), worker.taskIds());

        verify(taskStatusListener).onFailure(eq(TASK_ID), any(ConfigException.class));
        pluginsMockedStatic.verify(() ->  Plugins.compareAndSwapLoaders(pluginLoader));
        pluginsMockedStatic.verify(() ->  Plugins.compareAndSwapLoaders(delegatingLoader));
    }

    @Test
    public void testCleanupTasksOnStop() {
        mockInternalConverters();
        mockStorage();
        mockFileConfigProvider();

        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        when(plugins.newTask(TestSourceTask.class)).thenReturn(task);
        when(task.version()).thenReturn("1.0");

        // Expect that the worker will create converters and will not initially find them using the current classloader ...
        assertNotNull(taskKeyConverter);
        assertNotNull(taskValueConverter);
        assertNotNull(taskHeaderConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        mockTaskHeaderConverter(ClassLoaderUsage.PLUGINS, taskHeaderConverter);

        when(executorService.submit(any(WorkerSourceTask.class))).thenReturn(null);

        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(SampleSourceConnector.class.getName())).thenReturn(pluginLoader);
        doReturn(SampleSourceConnector.class).when(plugins).connectorClass(SampleSourceConnector.class.getName());

        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);

        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        worker.startTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        worker.stop();
        assertStatistics(worker, 0, 0);

        verifyStorage();

        WorkerSourceTask constructedMockTask = sourceTaskMockedConstruction.constructed().get(0);
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(pluginLoader), times(2));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        verify(plugins).newTask(TestSourceTask.class);
        verify(plugins, times(2)).currentThreadLoader();

        verify(plugins).delegatingLoader();
        verify(delegatingLoader).connectorLoader(SampleSourceConnector.class.getName());
        verify(plugins).connectorClass(SampleSourceConnector.class.getName());
        verify(constructedMockTask).initialize(taskConfig);
        verify(constructedMockTask).loader();
        verify(constructedMockTask).stop();
        verify(constructedMockTask).awaitStop(anyLong());
        verify(constructedMockTask).removeMetrics();
        verifyConverters();

        verify(executorService).submit(any(WorkerSourceTask.class));
    }

    @Test
    public void testConverterOverrides() {
        mockInternalConverters();
        mockStorage();
        mockFileConfigProvider();

        when(plugins.currentThreadLoader()).thenReturn(delegatingLoader);
        Map<String, String> origProps = Collections.singletonMap(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        TaskConfig taskConfig = new TaskConfig(origProps);

        when(plugins.newTask(TestSourceTask.class)).thenReturn(task);
        when(task.version()).thenReturn("1.0");

        // Expect that the worker will create converters and will not initially find them using the current classloader ...
        assertNotNull(taskKeyConverter);
        assertNotNull(taskValueConverter);
        assertNotNull(taskHeaderConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, taskKeyConverter);
        mockTaskConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, null);
        mockTaskConverter(ClassLoaderUsage.PLUGINS, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, taskValueConverter);
        mockTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        mockTaskHeaderConverter(ClassLoaderUsage.PLUGINS, taskHeaderConverter);

        when(executorService.submit(any(WorkerSourceTask.class))).thenReturn(null);

        when(plugins.delegatingLoader()).thenReturn(delegatingLoader);
        when(delegatingLoader.connectorLoader(SampleSourceConnector.class.getName())).thenReturn(pluginLoader);
        doReturn(SampleSourceConnector.class).when(plugins).connectorClass(SampleSourceConnector.class.getName());

        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(pluginLoader)).thenReturn(delegatingLoader);
        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);


        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        Map<String, String> connProps = anyConnectorConfigMap();
        connProps.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, SampleConverterWithHeaders.class.getName());
        connProps.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, SampleConverterWithHeaders.class.getName());
        worker.startTask(TASK_ID, ClusterConfigState.EMPTY, connProps, origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(Collections.singleton(TASK_ID), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        // We've mocked the Plugin.newConverter method, so we don't currently configure the converters
        verify(plugins).newTask(TestSourceTask.class);
        WorkerSourceTask instantiatedTask = sourceTaskMockedConstruction.constructed().get(0);
        verify(instantiatedTask).initialize(taskConfig);
        verify(executorService).submit(any(WorkerSourceTask.class));
        verify(plugins).delegatingLoader();
        verify(delegatingLoader).connectorLoader(SampleSourceConnector.class.getName());
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(pluginLoader), times(2));
        pluginsMockedStatic.verify(() -> Plugins.compareAndSwapLoaders(delegatingLoader), times(2));
        verify(plugins).connectorClass(SampleSourceConnector.class.getName());

        // Remove
        verify(instantiatedTask).stop();
        verify(instantiatedTask).awaitStop(anyLong());
        verify(instantiatedTask).removeMetrics();

        verify(plugins, times(2)).currentThreadLoader();
        verifyStorage();
    }

    @Test
    public void testProducerConfigsWithoutOverrides() {
        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());
        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("client.id", "connector-producer-job-0");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);
        assertEquals(expectedConfigs,
                     Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, config, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX);
    }

    @Test
    public void testProducerConfigsWithOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("producer.acks", "-1");
        props.put("producer.linger.ms", "1000");
        props.put("producer.client.id", "producer-test-id");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("acks", "-1");
        expectedConfigs.put("linger.ms", "1000");
        expectedConfigs.put("client.id", "producer-test-id");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());
        assertEquals(expectedConfigs,
            Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX);
    }

    @Test
    public void testProducerConfigsWithClientOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("producer.acks", "-1");
        props.put("producer.linger.ms", "1000");
        props.put("producer.client.id", "producer-test-id");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("acks", "-1");
        expectedConfigs.put("linger.ms", "5000");
        expectedConfigs.put("batch.size", "1000");
        expectedConfigs.put("client.id", "producer-test-id");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("linger.ms", "5000");
        connConfig.put("batch.size", "1000");

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertEquals(expectedConfigs,
            Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX);
    }

    @Test
    public void testConsumerConfigsWithoutOverrides() {
        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test");
        expectedConfigs.put("client.id", "connector-consumer-test-1");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());
        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), config, connectorConfig,
            null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX);
    }

    @Test
    public void testConsumerConfigsWithOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("consumer.auto.offset.reset", "latest");
        props.put("consumer.max.poll.records", "1000");
        props.put("consumer.client.id", "consumer-test-id");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test");
        expectedConfigs.put("auto.offset.reset", "latest");
        expectedConfigs.put("max.poll.records", "1000");
        expectedConfigs.put("client.id", "consumer-test-id");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(new HashMap<>());

        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
            null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX);
    }

    @Test
    public void testConsumerConfigsWithClientOverrides() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("consumer.auto.offset.reset", "latest");
        props.put("consumer.max.poll.records", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test");
        expectedConfigs.put("auto.offset.reset", "latest");
        expectedConfigs.put("max.poll.records", "5000");
        expectedConfigs.put("max.poll.interval.ms", "1000");
        expectedConfigs.put("client.id", "connector-consumer-test-1");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("max.poll.records", "5000");
        connConfig.put("max.poll.interval.ms", "1000");

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
            null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX);
    }

    @Test
    public void testConsumerConfigsClientOverridesWithNonePolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("consumer.auto.offset.reset", "latest");
        props.put("consumer.max.poll.records", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("max.poll.records", "5000");
        connConfig.put("max.poll.interval.ms", "1000");
        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).thenReturn(connConfig);

        assertThrows(ConnectException.class, () -> Worker.consumerConfigs(new ConnectorTaskId("test", 1),
            configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX);
    }

    @Test
    public void testAdminConfigsClientOverridesWithAllPolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        props.put("producer.bootstrap.servers", "cbeauho.com");
        props.put("consumer.bootstrap.servers", "localhost:4761");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = Collections.singletonMap("metadata.max.age.ms", "10000");
        Map<String, String> expectedConfigs = new HashMap<>(workerProps);
        expectedConfigs.put("bootstrap.servers", "localhost:9092");
        expectedConfigs.put("client.id", "testid");
        expectedConfigs.put("metadata.max.age.ms", "10000");

        //we added a config on the fly
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX)).thenReturn(connConfig);
        assertEquals(expectedConfigs, Worker.adminConfigs(new ConnectorTaskId("test", 1), "", configWithOverrides, connectorConfig,
                                                             null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX);
    }

    @Test
    public void testAdminConfigsClientOverridesWithNonePolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = Collections.singletonMap("metadata.max.age.ms", "10000");

        when(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX)).thenReturn(connConfig);
        assertThrows(ConnectException.class, () -> Worker.adminConfigs(new ConnectorTaskId("test", 1),
            "", configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));

        verify(connectorConfig).originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX);
    }

    @Test
    public void testWorkerMetrics() throws Exception {
        mockInternalConverters();
        mockFileConfigProvider();

        pluginsMockedStatic.when(() -> Plugins.compareAndSwapLoaders(delegatingLoader)).thenReturn(pluginLoader);
        Worker worker = new Worker("worker-1",
                Time.SYSTEM,
                plugins,
                config,
                offsetBackingStore,
                noneConnectorClientConfigOverridePolicy
                );
        MetricName name = worker.metrics().metrics().metricName("test.avg", "grp1");
        worker.metrics().metrics().addMetric(name, new Avg());
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        List<MetricsReporter> list = worker.metrics().metrics().reporters();
        for (MetricsReporter reporter : list) {
            if (reporter instanceof MockMetricsReporter) {
                MockMetricsReporter mockMetricsReporter = (MockMetricsReporter) reporter;
                //verify connect cluster is set in MetricsContext
                assertEquals(CLUSTER_ID, mockMetricsReporter.getMetricsContext().contextLabels().get(WorkerConfig.CONNECT_KAFKA_CLUSTER_ID));
            }
        }
        //verify metric is created with correct jmx prefix
        assertNotNull(server.getObjectInstance(new ObjectName("kafka.connect:type=grp1")));
    }

    private void assertStatusMetrics(long expected, String metricName) {
        MetricGroup statusMetrics = worker.connectorStatusMetricsGroup().metricGroup(TASK_ID.connector());
        if (expected == 0L) {
            assertNull(statusMetrics);
            return;
        }
        assertEquals(expected, MockConnectMetrics.currentMetricValue(worker.metrics(), statusMetrics, metricName));
    }

    private void assertStatistics(Worker worker, int connectors, int tasks) {
        assertStatusMetrics(tasks, "connector-total-task-count");
        MetricGroup workerMetrics = worker.workerMetricsGroup().metricGroup();
        assertEquals(connectors, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-count"), 0.0001d);
        assertEquals(tasks, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-count"), 0.0001d);
        assertEquals(tasks, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-count"), 0.0001d);
    }

    private void assertStartupStatistics(Worker worker, int connectorStartupAttempts, int connectorStartupFailures, int taskStartupAttempts, int taskStartupFailures) {
        double connectStartupSuccesses = connectorStartupAttempts - connectorStartupFailures;
        double taskStartupSuccesses = taskStartupAttempts - taskStartupFailures;
        double connectStartupSuccessPct = 0.0d;
        double connectStartupFailurePct = 0.0d;
        double taskStartupSuccessPct = 0.0d;
        double taskStartupFailurePct = 0.0d;
        if (connectorStartupAttempts != 0) {
            connectStartupSuccessPct = connectStartupSuccesses / connectorStartupAttempts;
            connectStartupFailurePct = (double) connectorStartupFailures / connectorStartupAttempts;
        }
        if (taskStartupAttempts != 0) {
            taskStartupSuccessPct = taskStartupSuccesses / taskStartupAttempts;
            taskStartupFailurePct = (double) taskStartupFailures / taskStartupAttempts;
        }
        MetricGroup workerMetrics = worker.workerMetricsGroup().metricGroup();
        assertEquals(connectorStartupAttempts, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-attempts-total"), 0.0001d);
        assertEquals(connectStartupSuccesses, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-success-total"), 0.0001d);
        assertEquals(connectorStartupFailures, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-failure-total"), 0.0001d);
        assertEquals(connectStartupSuccessPct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-success-percentage"), 0.0001d);
        assertEquals(connectStartupFailurePct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "connector-startup-failure-percentage"), 0.0001d);
        assertEquals(taskStartupAttempts, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-attempts-total"), 0.0001d);
        assertEquals(taskStartupSuccesses, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-success-total"), 0.0001d);
        assertEquals(taskStartupFailures, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-failure-total"), 0.0001d);
        assertEquals(taskStartupSuccessPct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-success-percentage"), 0.0001d);
        assertEquals(taskStartupFailurePct, MockConnectMetrics.currentMetricValueAsDouble(worker.metrics(), workerMetrics, "task-startup-failure-percentage"), 0.0001d);
    }

    private void mockStorage() {
        when(herder.statusBackingStore()).thenReturn(statusBackingStore);
    }


    private void verifyStorage() {
        verify(offsetBackingStore).configure(any(WorkerConfig.class));
        verify(offsetBackingStore).start();
        verify(herder).statusBackingStore();
        verify(offsetBackingStore).stop();
    }

    private void mockInternalConverters() {
        Converter internalKeyConverter = mock(JsonConverter.class);
        Converter internalValueConverter = mock(JsonConverter.class);

        when(plugins.newInternalConverter(eq(true), anyString(), anyMap()))
                       .thenReturn(internalKeyConverter);

        when(plugins.newInternalConverter(eq(false), anyString(), anyMap()))
                       .thenReturn(internalValueConverter);
    }

    private void verifyConverters() {
        verify(plugins, times(1)).newInternalConverter(eq(true), anyString(), anyMap());
        verify(plugins).newInternalConverter(eq(false), anyString(), anyMap());
    }

    private void mockTaskConverter(ClassLoaderUsage classLoaderUsage, String converterClassConfig, Converter returning) {
        when(plugins.newConverter(any(AbstractConfig.class), eq(converterClassConfig), eq(classLoaderUsage)))
                       .thenReturn(returning);
    }

    private void verifyTaskConverter(String converterClassConfig) {
        verify(plugins).newConverter(any(AbstractConfig.class), eq(converterClassConfig), eq(ClassLoaderUsage.CURRENT_CLASSLOADER));
    }

    private void mockTaskHeaderConverter(ClassLoaderUsage classLoaderUsage, HeaderConverter returning) {
        when(plugins.newHeaderConverter(any(AbstractConfig.class), eq(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG), eq(classLoaderUsage)))
               .thenReturn(returning);
    }

    private void verifyTaskHeaderConverter() {
        verify(plugins).newHeaderConverter(any(AbstractConfig.class), eq(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG), eq(ClassLoaderUsage.CURRENT_CLASSLOADER));
    }


    private Map<String, String> anyConnectorConfigMap() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SampleSourceConnector.class.getName());
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }


    private static class TestSourceTask extends SourceTask {
        public TestSourceTask() {
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public List<SourceRecord> poll() {
            return null;
        }

        @Override
        public void stop() {
        }
    }


}

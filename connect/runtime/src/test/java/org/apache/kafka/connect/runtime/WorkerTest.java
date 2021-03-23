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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.config.provider.MockFileConfigProvider;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.MockConnectMetrics.MockMetricsReporter;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
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
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.ParameterizedTest;
import org.apache.kafka.connect.util.ThreadedTest;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockNice;
import org.powermock.api.easymock.annotation.MockStrict;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest.NOOP_OPERATOR;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(ParameterizedTest.class)
@PrepareForTest({Worker.class, Plugins.class, ConnectUtils.class})
@PowerMockIgnore("javax.management.*")
public class WorkerTest extends ThreadedTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);
    private static final String WORKER_ID = "localhost:8083";
    private static final String CLUSTER_ID = "test-cluster";
    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();
    private final ConnectorClientConfigOverridePolicy allConnectorClientConfigOverridePolicy = new AllConnectorClientConfigOverridePolicy();

    private Map<String, String> workerProps = new HashMap<>();
    private WorkerConfig config;
    private Worker worker;

    private Map<String, String> defaultProducerConfigs = new HashMap<>();
    private Map<String, String> defaultConsumerConfigs = new HashMap<>();

    @Mock
    private Plugins plugins;
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock
    private DelegatingClassLoader delegatingLoader;
    @Mock
    private OffsetBackingStore offsetBackingStore;
    @MockStrict
    private TaskStatus.Listener taskStatusListener;
    @MockStrict
    private ConnectorStatus.Listener connectorStatusListener;

    @Mock private Herder herder;
    @Mock private StatusBackingStore statusBackingStore;
    @Mock private SourceConnector sourceConnector;
    @Mock private SinkConnector sinkConnector;
    @Mock private CloseableConnectorContext ctx;
    @Mock private TestSourceTask task;
    @Mock private WorkerSourceTask workerTask;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private Converter taskKeyConverter;
    @Mock private Converter taskValueConverter;
    @Mock private HeaderConverter taskHeaderConverter;
    @Mock private ExecutorService executorService;
    @MockNice private ConnectorConfig connectorConfig;
    private String mockFileProviderTestId;
    private Map<String, String> connectorProps;

    private boolean enableTopicCreation;

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
        PowerMock.mockStatic(Plugins.class);
    }

    @Test
    public void testStartAndStopConnector() throws Throwable {
        expectConverters();
        expectStartStorage();

        final String connectorClass = WorkerTestConnector.class.getName();

        // Create
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(connectorClass)).andReturn(pluginLoader);
        EasyMock.expect(plugins.newConnector(connectorClass))
                .andReturn(sourceConnector);
        EasyMock.expect(sourceConnector.version()).andReturn("1.0");

        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);

        EasyMock.expect(sourceConnector.version()).andReturn("1.0");

        expectFileConfigProvider();
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader))
                .andReturn(delegatingLoader)
                .times(3);
        sourceConnector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        sourceConnector.start(connectorProps);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader).times(3);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        sourceConnector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        ctx.close();
        expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());

        FutureCallback<TargetState> onFirstStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onFirstStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onFirstStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());

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

        PowerMock.verifyAll();
        MockFileConfigProvider.assertClosed(mockFileProviderTestId);
    }

    private void expectFileConfigProvider() {
        EasyMock.expect(plugins.newConfigProvider(EasyMock.anyObject(),
                    EasyMock.eq("config.providers.file"), EasyMock.anyObject()))
                .andAnswer(() -> {
                    MockFileConfigProvider mockFileConfigProvider = new MockFileConfigProvider();
                    mockFileConfigProvider.configure(Collections.singletonMap("testId", mockFileProviderTestId));
                    return mockFileConfigProvider;
                });
    }

    @Test
    public void testStartConnectorFailure() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        final String nonConnectorClass = "java.util.HashMap";
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, nonConnectorClass); // Bad connector class name

        Exception exception = new ConnectException("Failed to find Connector");
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(nonConnectorClass)).andReturn(delegatingLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString()))
                .andThrow(exception);

        connectorStatusListener.onFailure(
                EasyMock.eq(CONNECTOR_ID),
                EasyMock.<ConnectException>anyObject()
        );
        EasyMock.expectLastCall();

        expectClusterId();

        PowerMock.replayAll();

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

        PowerMock.verifyAll();
    }

    @Test
    public void testAddConnectorByAlias() throws Throwable {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        final String connectorAlias = "WorkerTestConnector";

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(connectorAlias)).andReturn(pluginLoader);
        EasyMock.expect(plugins.newConnector(connectorAlias)).andReturn(sinkConnector);
        EasyMock.expect(sinkConnector.version()).andReturn("1.0");

        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorAlias);
        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "gfieyls, wfru");

        EasyMock.expect(sinkConnector.version()).andReturn("1.0");
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader))
                .andReturn(delegatingLoader)
                .times(3);
        sinkConnector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        sinkConnector.start(connectorProps);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader)
                .times(3);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        sinkConnector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        ctx.close();
        expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
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

        PowerMock.verifyAll();
    }

    @Test
    public void testAddConnectorByShortAlias() throws Throwable {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        final String shortConnectorAlias = "WorkerTest";

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(shortConnectorAlias)).andReturn(pluginLoader);
        EasyMock.expect(plugins.newConnector(shortConnectorAlias)).andReturn(sinkConnector);
        EasyMock.expect(sinkConnector.version()).andReturn("1.0");

        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, shortConnectorAlias);
        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "gfieyls, wfru");

        EasyMock.expect(sinkConnector.version()).andReturn("1.0");
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader))
                .andReturn(delegatingLoader)
                .times(3);
        sinkConnector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        sinkConnector.start(connectorProps);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader)
                .times(3);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        sinkConnector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        ctx.close();
        expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        FutureCallback<TargetState> onStart = new FutureCallback<>();
        worker.startConnector(CONNECTOR_ID, connectorProps, ctx, connectorStatusListener, TargetState.STARTED, onStart);
        // Wait for the connector to actually start
        assertEquals(TargetState.STARTED, onStart.get(1000, TimeUnit.MILLISECONDS));
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
        assertStatistics(worker, 1, 0);

        worker.stopAndAwaitConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        PowerMock.verifyAll();
    }

    @Test
    public void testStopInvalidConnector() {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();
        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        worker.stopAndAwaitConnector(CONNECTOR_ID);

        PowerMock.verifyAll();
    }

    @Test
    public void testReconfigureConnectorTasks() throws Throwable {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        final String connectorClass = WorkerTestConnector.class.getName();

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(3);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader).times(1);
        EasyMock.expect(delegatingLoader.connectorLoader(connectorClass)).andReturn(pluginLoader);
        EasyMock.expect(plugins.newConnector(connectorClass))
                .andReturn(sinkConnector);
        EasyMock.expect(sinkConnector.version()).andReturn("1.0");

        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass);

        EasyMock.expect(sinkConnector.version()).andReturn("1.0");
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader))
                .andReturn(delegatingLoader)
                .times(4);
        sinkConnector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        sinkConnector.start(connectorProps);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader)
                .times(4);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Reconfigure
        EasyMock.<Class<? extends Task>>expect(sinkConnector.taskClass()).andReturn(TestSourceTask.class);
        Map<String, String> taskProps = new HashMap<>();
        taskProps.put("foo", "bar");
        EasyMock.expect(sinkConnector.taskConfigs(2)).andReturn(Arrays.asList(taskProps, taskProps));

        // Remove
        sinkConnector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        ctx.close();
        expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

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
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());

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

        PowerMock.verifyAll();
    }

    @Test
    public void testAddRemoveTask() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        expectNewWorkerTask();
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);
        // We should expect this call, but the pluginLoader being swapped in is only mocked.
        // EasyMock.expect(pluginLoader.loadClass(TestSourceTask.class.getName()))
        //        .andReturn((Class) TestSourceTask.class);
        EasyMock.expect(plugins.newTask(TestSourceTask.class)).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");

        workerTask.initialize(taskConfig);
        EasyMock.expectLastCall();

        // Expect that the worker will create converters and will find them using the current classloader ...
        assertNotNull(taskKeyConverter);
        assertNotNull(taskValueConverter);
        assertNotNull(taskHeaderConverter);
        expectTaskKeyConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, taskKeyConverter);
        expectTaskValueConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, taskValueConverter);
        expectTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);

        EasyMock.expect(executorService.submit(workerTask)).andReturn(null);

        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestConnector.class.getName()))
                .andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
                .times(2);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
                .times(2);
        plugins.connectorClass(WorkerTestConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestConnector.class);
        // Remove
        workerTask.stop();
        EasyMock.expectLastCall();
        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andStubReturn(true);
        EasyMock.expectLastCall();

        workerTask.removeMetrics();
        EasyMock.expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertStartupStatistics(worker, 0, 0, 1, 0);
        assertEquals(new HashSet<>(Arrays.asList(TASK_ID)), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 1, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 1, 0);

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskStatusMetricsStatuses() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        expectNewWorkerTask();
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);
        // We should expect this call, but the pluginLoader being swapped in is only mocked.
        // EasyMock.expect(pluginLoader.loadClass(TestSourceTask.class.getName()))
        //        .andReturn((Class) TestSourceTask.class);
        EasyMock.expect(plugins.newTask(TestSourceTask.class)).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");

        workerTask.initialize(taskConfig);
        EasyMock.expectLastCall();

        // Expect that the worker will create converters and will find them using the current classloader ...
        assertNotNull(taskKeyConverter);
        assertNotNull(taskValueConverter);
        assertNotNull(taskHeaderConverter);
        expectTaskKeyConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, taskKeyConverter);
        expectTaskValueConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, taskValueConverter);
        expectTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, taskHeaderConverter);

        EasyMock.expect(executorService.submit(workerTask)).andReturn(null);

        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestConnector.class.getName()))
            .andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
            .times(2);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
            .times(2);
        plugins.connectorClass(WorkerTestConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestConnector.class);

        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andStubReturn(true);
        EasyMock.expectLastCall();

        workerTask.removeMetrics();
        EasyMock.expectLastCall();

        // Each time we check the task metrics, the worker will call the herder
        herder.taskStatus(TASK_ID);
        EasyMock.expectLastCall()
            .andReturn(new ConnectorStateInfo.TaskState(0, "RUNNING", "worker", "msg"));

        herder.taskStatus(TASK_ID);
        EasyMock.expectLastCall()
            .andReturn(new ConnectorStateInfo.TaskState(0, "PAUSED", "worker", "msg"));

        herder.taskStatus(TASK_ID);
        EasyMock.expectLastCall()
            .andReturn(new ConnectorStateInfo.TaskState(0, "FAILED", "worker", "msg"));

        herder.taskStatus(TASK_ID);
        EasyMock.expectLastCall()
            .andReturn(new ConnectorStateInfo.TaskState(0, "DESTROYED", "worker", "msg"));

        herder.taskStatus(TASK_ID);
        EasyMock.expectLastCall()
            .andReturn(new ConnectorStateInfo.TaskState(0, "UNASSIGNED", "worker", "msg"));

        // Called when we stop the worker
        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);
        workerTask.stop();
        EasyMock.expectLastCall();

        expectClusterId();

        PowerMock.replayAll();

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

        PowerMock.verifyAll();
    }

    @Test
    public void testConnectorStatusMetricsGroup_taskStatusCounter() {
        ConcurrentMap<ConnectorTaskId, WorkerTask> tasks = new ConcurrentHashMap<>();
        tasks.put(new ConnectorTaskId("c1", 0), workerTask);
        tasks.put(new ConnectorTaskId("c1", 1), workerTask);
        tasks.put(new ConnectorTaskId("c2", 0), workerTask);

        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        taskStatusListener.onFailure(EasyMock.eq(TASK_ID), EasyMock.<ConfigException>anyObject());
        EasyMock.expectLastCall();

        expectClusterId();

        PowerMock.replayAll();

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
    }

    @Test
    public void testStartTaskFailure() {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, "missing.From.This.Workers.Classpath");

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestConnector.class.getName()))
                .andReturn(pluginLoader);

        // We would normally expect this since the plugin loader would have been swapped in. However, since we mock out
        // all classloader changes, the call actually goes to the normal default classloader. However, this works out
        // fine since we just wanted a ClassNotFoundException anyway.
        // EasyMock.expect(pluginLoader.loadClass(origProps.get(TaskConfig.TASK_CLASS_CONFIG)))
        //        .andThrow(new ClassNotFoundException());

        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader))
                .andReturn(delegatingLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader);

        taskStatusListener.onFailure(EasyMock.eq(TASK_ID), EasyMock.<ConfigException>anyObject());
        EasyMock.expectLastCall();

        expectClusterId();

        PowerMock.replayAll();

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

        PowerMock.verifyAll();
    }

    @Test
    public void testCleanupTasksOnStop() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        expectNewWorkerTask();
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);
        // We should expect this call, but the pluginLoader being swapped in is only mocked.
        // EasyMock.expect(pluginLoader.loadClass(TestSourceTask.class.getName()))
        //        .andReturn((Class) TestSourceTask.class);
        EasyMock.expect(plugins.newTask(TestSourceTask.class)).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");

        workerTask.initialize(taskConfig);
        EasyMock.expectLastCall();

        // Expect that the worker will create converters and will not initially find them using the current classloader ...
        assertNotNull(taskKeyConverter);
        assertNotNull(taskValueConverter);
        assertNotNull(taskHeaderConverter);
        expectTaskKeyConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        expectTaskKeyConverters(ClassLoaderUsage.PLUGINS, taskKeyConverter);
        expectTaskValueConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        expectTaskValueConverters(ClassLoaderUsage.PLUGINS, taskValueConverter);
        expectTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        expectTaskHeaderConverter(ClassLoaderUsage.PLUGINS, taskHeaderConverter);

        EasyMock.expect(executorService.submit(workerTask)).andReturn(null);

        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestConnector.class.getName()))
                .andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
                .times(2);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
                .times(2);
        plugins.connectorClass(WorkerTestConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestConnector.class);
        // Remove on Worker.stop()
        workerTask.stop();
        EasyMock.expectLastCall();

        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andReturn(true);
        // Note that in this case we *do not* commit offsets since it's an unclean shutdown
        EasyMock.expectLastCall();

        workerTask.removeMetrics();
        EasyMock.expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        worker.startTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        worker.stop();
        assertStatistics(worker, 0, 0);

        PowerMock.verifyAll();
    }

    @Test
    public void testConverterOverrides() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        expectNewWorkerTask();
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);
        // We should expect this call, but the pluginLoader being swapped in is only mocked.
        // EasyMock.expect(pluginLoader.loadClass(TestSourceTask.class.getName()))
        //        .andReturn((Class) TestSourceTask.class);
        EasyMock.expect(plugins.newTask(TestSourceTask.class)).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");

        workerTask.initialize(taskConfig);
        EasyMock.expectLastCall();

        // Expect that the worker will create converters and will not initially find them using the current classloader ...
        assertNotNull(taskKeyConverter);
        assertNotNull(taskValueConverter);
        assertNotNull(taskHeaderConverter);
        expectTaskKeyConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        expectTaskKeyConverters(ClassLoaderUsage.PLUGINS, taskKeyConverter);
        expectTaskValueConverters(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        expectTaskValueConverters(ClassLoaderUsage.PLUGINS, taskValueConverter);
        expectTaskHeaderConverter(ClassLoaderUsage.CURRENT_CLASSLOADER, null);
        expectTaskHeaderConverter(ClassLoaderUsage.PLUGINS, taskHeaderConverter);

        EasyMock.expect(executorService.submit(workerTask)).andReturn(null);

        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestConnector.class.getName()))
                .andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
                .times(2);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
                .times(2);
        plugins.connectorClass(WorkerTestConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestConnector.class);

        // Remove
        workerTask.stop();
        EasyMock.expectLastCall();
        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andStubReturn(true);
        EasyMock.expectLastCall();

        workerTask.removeMetrics();
        EasyMock.expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        Map<String, String> connProps = anyConnectorConfigMap();
        connProps.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        connProps.put("key.converter.extra.config", "foo");
        connProps.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, TestConfigurableConverter.class.getName());
        connProps.put("value.converter.extra.config", "bar");
        worker.startTask(TASK_ID, ClusterConfigState.EMPTY, connProps, origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(new HashSet<>(Arrays.asList(TASK_ID)), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        // We've mocked the Plugin.newConverter method, so we don't currently configure the converters

        PowerMock.verifyAll();
    }

    @Test
    public void testProducerConfigsWithoutOverrides() {
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).andReturn(
            new HashMap<>());
        PowerMock.replayAll();
        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("client.id", "connector-producer-job-0");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);
        assertEquals(expectedConfigs,
                     Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, config, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));
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

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).andReturn(
            new HashMap<>());
        PowerMock.replayAll();
        assertEquals(expectedConfigs,
            Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
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
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertEquals(expectedConfigs,
            Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
    }

    @Test
    public void testConsumerConfigsWithoutOverrides() {
        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test");
        expectedConfigs.put("client.id", "connector-consumer-test-1");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).andReturn(new HashMap<>());
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), config, connectorConfig,
            null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));
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

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).andReturn(new HashMap<>());
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
            null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));

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
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
            null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
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
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertThrows(ConnectException.class, () -> Worker.consumerConfigs(new ConnectorTaskId("test", 1),
            configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));
    }

    @Test
    public void testAdminConfigsClientOverridesWithAllPolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        props.put("producer.bootstrap.servers", "cbeauho.com");
        props.put("consumer.bootstrap.servers", "localhost:4761");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("metadata.max.age.ms", "10000");

        Map<String, String> expectedConfigs = new HashMap<>(workerProps);

        expectedConfigs.put("bootstrap.servers", "localhost:9092");
        expectedConfigs.put("client.id", "testid");
        expectedConfigs.put("metadata.max.age.ms", "10000");
        //we added a config on the fly
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.adminConfigs(new ConnectorTaskId("test", 1), "", configWithOverrides, connectorConfig,
                                                             null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
    }

    @Test
    public void testAdminConfigsClientOverridesWithNonePolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = new HashMap<>();
        connConfig.put("metadata.max.age.ms", "10000");

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertThrows(ConnectException.class, () -> Worker.adminConfigs(new ConnectorTaskId("test", 1),
            "", configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));

    }

    @Test
    public void testWorkerMetrics() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        // Create
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.newConnector(WorkerTestConnector.class.getName()))
                .andReturn(sourceConnector);
        EasyMock.expect(sourceConnector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, WorkerTestConnector.class.getName());

        EasyMock.expect(sourceConnector.version()).andReturn("1.0");

        EasyMock.expect(plugins.compareAndSwapLoaders(sourceConnector))
                .andReturn(delegatingLoader)
                .times(2);
        sourceConnector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        sourceConnector.start(props);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader).times(2);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        sourceConnector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();
        expectClusterId();

        PowerMock.replayAll();

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
        Set<ObjectInstance> ret = server.queryMBeans(null, null);

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

    private void expectStartStorage() {
        offsetBackingStore.configure(anyObject(WorkerConfig.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();
        EasyMock.expect(herder.statusBackingStore())
                .andReturn(statusBackingStore).anyTimes();
    }

    private void expectStopStorage() {
        offsetBackingStore.stop();
        EasyMock.expectLastCall();
    }

    private void expectConverters() {
        expectConverters(JsonConverter.class, false);
    }

    private void expectConverters(Boolean expectDefaultConverters) {
        expectConverters(JsonConverter.class, expectDefaultConverters);
    }

    @SuppressWarnings("deprecation")
    private void expectConverters(Class<? extends Converter> converterClass, Boolean expectDefaultConverters) {
        // As default converters are instantiated when a task starts, they are expected only if the `startTask` method is called
        if (expectDefaultConverters) {

            // Instantiate and configure default
            EasyMock.expect(plugins.newConverter(config, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.PLUGINS))
                    .andReturn(keyConverter);
            EasyMock.expect(plugins.newConverter(config, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.PLUGINS))
                    .andReturn(valueConverter);
            EasyMock.expectLastCall();
        }

        //internal
        Converter internalKeyConverter = PowerMock.createMock(converterClass);
        Converter internalValueConverter = PowerMock.createMock(converterClass);

        // Instantiate and configure internal
        EasyMock.expect(
                plugins.newConverter(
                        config,
                        WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG,
                        ClassLoaderUsage.PLUGINS
                )
        ).andReturn(internalKeyConverter);
        EasyMock.expect(
                plugins.newConverter(
                        config,
                        WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG,
                        ClassLoaderUsage.PLUGINS
                )
        ).andReturn(internalValueConverter);
        EasyMock.expectLastCall();
    }

    private void expectTaskKeyConverters(ClassLoaderUsage classLoaderUsage, Converter returning) {
        EasyMock.expect(
                plugins.newConverter(
                        anyObject(AbstractConfig.class),
                        eq(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG),
                        eq(classLoaderUsage)))
                .andReturn(returning);
    }

    private void expectTaskValueConverters(ClassLoaderUsage classLoaderUsage, Converter returning) {
        EasyMock.expect(
                plugins.newConverter(
                        anyObject(AbstractConfig.class),
                        eq(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG),
                        eq(classLoaderUsage)))
                .andReturn(returning);
    }

    private void expectTaskHeaderConverter(ClassLoaderUsage classLoaderUsage, HeaderConverter returning) {
        EasyMock.expect(
                plugins.newHeaderConverter(
                        anyObject(AbstractConfig.class),
                        eq(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG),
                        eq(classLoaderUsage)))
                .andReturn(returning);
    }

    private Map<String, String> anyConnectorConfigMap() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, WorkerTestConnector.class.getName());
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }

    private void expectClusterId() {
        PowerMock.mockStaticPartial(ConnectUtils.class, "lookupKafkaClusterId");
        EasyMock.expect(ConnectUtils.lookupKafkaClusterId(EasyMock.anyObject())).andReturn("test-cluster").anyTimes();
    }

    private void expectNewWorkerTask() throws Exception {
        PowerMock.expectNew(
                WorkerSourceTask.class, EasyMock.eq(TASK_ID),
                EasyMock.eq(task),
                anyObject(TaskStatus.Listener.class),
                EasyMock.eq(TargetState.STARTED),
                anyObject(JsonConverter.class),
                anyObject(JsonConverter.class),
                anyObject(JsonConverter.class),
                EasyMock.eq(new TransformationChain<>(Collections.emptyList(), NOOP_OPERATOR)),
                anyObject(KafkaProducer.class),
                anyObject(TopicAdmin.class),
                EasyMock.<Map<String, TopicCreationGroup>>anyObject(),
                anyObject(OffsetStorageReader.class),
                anyObject(OffsetStorageWriter.class),
                EasyMock.eq(config),
                anyObject(ClusterConfigState.class),
                anyObject(ConnectMetrics.class),
                EasyMock.eq(pluginLoader),
                anyObject(Time.class),
                anyObject(RetryWithToleranceOperator.class),
                anyObject(StatusBackingStore.class),
                anyObject(Executor.class))
                .andReturn(workerTask);
    }
    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class WorkerTestConnector extends SourceConnector {

        private static final ConfigDef CONFIG_DEF  = new ConfigDef()
            .define("configName", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Test configName.");

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {

        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return null;
        }

        @Override
        public void stop() {

        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }
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
        public List<SourceRecord> poll() throws InterruptedException {
            return null;
        }

        @Override
        public void stop() {
        }
    }

    public static class TestConverter implements Converter {
        public Map<String, ?> configs;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.configs = configs;
        }

        @Override
        public byte[] fromConnectData(String topic, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectData(String topic, byte[] value) {
            return null;
        }
    }

    public static class TestConfigurableConverter implements Converter, Configurable {
        public Map<String, ?> configs;

        public ConfigDef config() {
            return JsonConverterConfig.configDef();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            new JsonConverterConfig(configs); // requires the `converter.type` config be set
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.configs = configs;
        }

        @Override
        public byte[] fromConnectData(String topic, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectData(String topic, byte[] value) {
            return null;
        }
    }
}

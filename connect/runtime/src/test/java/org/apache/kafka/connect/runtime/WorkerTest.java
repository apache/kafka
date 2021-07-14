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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.MockFileConfigProvider;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.Avg;
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
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.MockConnectMetrics.MockMetricsReporter;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ClusterConfigState;
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
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import static org.apache.kafka.clients.admin.AdminClientConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest.NOOP_OPERATOR;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(ParameterizedTest.class)
@PrepareForTest({Worker.class, Plugins.class, ConnectUtils.class})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*"})
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

    private final boolean enableTopicCreation;

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

        defaultConsumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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

        final String connectorClass = WorkerTestSourceConnector.class.getName();

        // Create
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(connectorClass)).andReturn(pluginLoader);
        EasyMock.expect(plugins.newConnector(connectorClass))
                .andReturn(sourceConnector);
        EasyMock.expect(sourceConnector.version()).andReturn("1.0");

        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

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
        connectorProps.put(CONNECTOR_CLASS_CONFIG, nonConnectorClass); // Bad connector class name

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

        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorAlias);
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

        connectorProps.put(CONNECTOR_CLASS_CONFIG, shortConnectorAlias);
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

        final String connectorClass = WorkerTestSourceConnector.class.getName();

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(3);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader).times(1);
        EasyMock.expect(delegatingLoader.connectorLoader(connectorClass)).andReturn(pluginLoader);
        EasyMock.expect(plugins.newConnector(connectorClass))
                .andReturn(sinkConnector);
        EasyMock.expect(sinkConnector.version()).andReturn("1.0");

        connectorProps.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        connectorProps.put(CONNECTOR_CLASS_CONFIG, connectorClass);

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
        expectedTaskProps.put(TOPICS_CONFIG, "foo,bar");
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
        expectNewWorkerSourceTask();
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
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSourceConnector.class.getName()))
                .andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
                .times(2);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
                .times(2);
        plugins.connectorClass(WorkerTestSourceConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestSourceConnector.class);
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
        worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(new HashSet<>(Arrays.asList(TASK_ID)), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        PowerMock.verifyAll();
    }

    @Test
    public void testAddSinkTask() throws Exception {
        // Most of the other cases use source tasks; we make sure to get code coverage for sink tasks here as well
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        WorkerSinkTask workerTask = EasyMock.mock(WorkerSinkTask.class);
        SinkTask task = EasyMock.mock(SinkTask.class);
        expectNewWorkerSinkTask(workerTask, task);
        Map<String, String> origProps = new HashMap<>();
        origProps.put(CONNECTOR_CLASS_CONFIG, WorkerSinkTask.class.getName());
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);
        // We should expect this call, but the pluginLoader being swapped in is only mocked.
        // EasyMock.expect(pluginLoader.loadClass(TestSinkTask.class.getName()))
        //        .andReturn((Class) TestSinkTask.class);
        EasyMock.expect(plugins.newTask(TestSinkTask.class)).andReturn(task);
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
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSinkConnector.class.getName()))
                .andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);
        plugins.connectorClass(WorkerTestSinkConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestSinkConnector.class);

        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());

        Map<String, String> connectorConfigs = anyConnectorConfigMap();
        connectorConfigs.put(TOPICS_CONFIG, "t1");
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, WorkerTestSinkConnector.class.getName());

        worker.startSinkTask(TASK_ID, ClusterConfigState.EMPTY, connectorConfigs, origProps, taskStatusListener, TargetState.STARTED);
        assertStatistics(worker, 0, 1);
        assertEquals(new HashSet<>(Arrays.asList(TASK_ID)), worker.taskIds());

        PowerMock.verifyAll();
    }

    @Test
    public void testAddExactlyOnceSourceTask() throws Exception {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        workerProps.put("config.providers", "file");
        workerProps.put("config.providers.file.class", MockFileConfigProvider.class.getName());
        mockFileProviderTestId = UUID.randomUUID().toString();
        workerProps.put("config.providers.file.param.testId", mockFileProviderTestId);
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(enableTopicCreation));
        workerProps.put(GROUP_ID_CONFIG, "connect-cluster");
        workerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:2606");
        workerProps.put(OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        workerProps.put(CONFIG_TOPIC_CONFIG, "connect-configs");
        workerProps.put(STATUS_STORAGE_TOPIC_CONFIG, "connect-statuses");
        workerProps.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        config = new DistributedConfig(workerProps);

        // Most of the other cases use vanilla source tasks; we make sure to get code coverage for exactly-once source tasks here as well
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        ExactlyOnceWorkerSourceTask workerTask = EasyMock.mock(ExactlyOnceWorkerSourceTask.class);
        Runnable preProducer = EasyMock.mock(Runnable.class);
        Runnable postProducer = EasyMock.mock(Runnable.class);
        expectNewExactlyOnceWorkerSourceTask(workerTask, preProducer, postProducer);
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());

        TaskConfig taskConfig = new TaskConfig(origProps);
        // We should expect this call, but the pluginLoader being swapped in is only mocked.
        // EasyMock.expect(pluginLoader.loadClass(TestSinkTask.class.getName()))
        //        .andReturn((Class) TestSinkTask.class);
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
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSourceConnector.class.getName()))
                .andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);
        plugins.connectorClass(WorkerTestSourceConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestSourceConnector.class);

        expectClusterId();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                noneConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 0, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startExactlyOnceSourceTask(TASK_ID, ClusterConfigState.EMPTY,  anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED, preProducer, postProducer);
        assertStatistics(worker, 0, 1);
        assertEquals(new HashSet<>(Arrays.asList(TASK_ID)), worker.taskIds());

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskStatusMetricsStatuses() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        expectNewWorkerSourceTask();
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
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSourceConnector.class.getName()))
            .andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
            .times(2);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
            .times(2);
        plugins.connectorClass(WorkerTestSourceConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestSourceConnector.class);

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
        worker.startSourceTask(
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
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSourceConnector.class.getName()))
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

        assertFalse(worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED));
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
        expectNewWorkerSourceTask();
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
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSourceConnector.class.getName()))
                .andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
                .times(2);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
                .times(2);
        plugins.connectorClass(WorkerTestSourceConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestSourceConnector.class);
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
        worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
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
        expectNewWorkerSourceTask();
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
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSourceConnector.class.getName()))
                .andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader)
                .times(2);

        EasyMock.expect(workerTask.loader()).andReturn(pluginLoader);

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader)
                .times(2);
        plugins.connectorClass(WorkerTestSourceConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestSourceConnector.class);

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
        worker.startSourceTask(TASK_ID, ClusterConfigState.EMPTY, connProps, origProps, taskStatusListener, TargetState.STARTED);
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
        EasyMock.expect(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).andReturn(
            new HashMap<>());
        PowerMock.replayAll();
        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("client.id", "connector-producer-job-0");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);
        assertEquals(expectedConfigs,
                     Worker.baseProducerConfigs(CONNECTOR_ID, "connector-producer-" + TASK_ID, config, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID));
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

        EasyMock.expect(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).andReturn(
            new HashMap<>());
        PowerMock.replayAll();
        assertEquals(expectedConfigs,
            Worker.baseProducerConfigs(CONNECTOR_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
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
        EasyMock.expect(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertEquals(expectedConfigs,
            Worker.baseProducerConfigs(CONNECTOR_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID));
    }

    @Test
    public void testConsumerConfigsWithoutOverrides() {
        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test");
        expectedConfigs.put("client.id", "connector-consumer-test-1");
        expectedConfigs.put("metrics.context.connect.kafka.cluster.id", CLUSTER_ID);

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).andReturn(new HashMap<>());
        PowerMock.replayAll();
        ConnectorTaskId id = new ConnectorTaskId("test", 1);
        assertEquals(expectedConfigs, Worker.baseConsumerConfigs("test", "connector-consumer-" + id, config, connectorConfig,
            null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
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
        ConnectorTaskId id = new ConnectorTaskId("test", 1);
        assertEquals(expectedConfigs, Worker.baseConsumerConfigs(id.connector(), "connector-consumer-" + id, configWithOverrides, connectorConfig,
            null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));

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
        ConnectorTaskId id = new ConnectorTaskId("test", 1);
        assertEquals(expectedConfigs, Worker.baseConsumerConfigs(id.connector(), "connector-consumer-" + id, configWithOverrides, connectorConfig,
            null, allConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
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
        ConnectorTaskId id = new ConnectorTaskId("test", 1);
        assertThrows(ConnectException.class, () -> Worker.baseConsumerConfigs(id.connector(), "connector-consumer-" + id,
            configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
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
        assertEquals(expectedConfigs, Worker.adminConfigs("test", "", configWithOverrides, connectorConfig,
                                                             null, allConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
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
        assertThrows(ConnectException.class, () -> Worker.adminConfigs("test",
            "", configWithOverrides, connectorConfig, null, noneConnectorClientConfigOverridePolicy, CLUSTER_ID, ConnectorType.SINK));
    }

    @Test
    public void testRegularSourceOffsetsConsumerConfigs() {
        final Map<String, Object> connectorConsumerOverrides = new HashMap<>();
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX))
                .andReturn(connectorConsumerOverrides)
                .anyTimes();
        PowerMock.replayAll();

        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", "localhost:4761");
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", "connect-offsets");
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        Map<String, Object> consumerConfigs = Worker.regularSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:4761", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.put("consumer." + BOOTSTRAP_SERVERS_CONFIG, "localhost:9021");
        workerProps.put("consumer." + ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.regularSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:9021", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is allowed to override the isolation level for regular (non-exactly-once) source connectors and their tasks
        assertEquals("read_uncommitted", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.remove("consumer." + ISOLATION_LEVEL_CONFIG);
        connectorConsumerOverrides.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:489");
        connectorConsumerOverrides.put(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.regularSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:489", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is allowed to override the isolation level for regular (non-exactly-once) source connectors and their tasks
        assertEquals("read_uncommitted", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void testExactlyOnceSourceOffsetsConsumerConfigs() {
        final Map<String, Object> connectorConsumerOverrides = new HashMap<>();
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX))
                .andReturn(connectorConsumerOverrides)
                .anyTimes();
        PowerMock.replayAll();

        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", "localhost:4761");
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", "connect-offsets");
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        Map<String, Object> consumerConfigs = Worker.exactlyOnceSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:4761", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.put("consumer." + BOOTSTRAP_SERVERS_CONFIG, "localhost:9021");
        workerProps.put("consumer." + ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.exactlyOnceSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:9021", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override isolation level when exactly-once support is enabled
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));

        workerProps.remove("consumer." + ISOLATION_LEVEL_CONFIG);
        connectorConsumerOverrides.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:489");
        connectorConsumerOverrides.put(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        config = new DistributedConfig(workerProps);
        consumerConfigs = Worker.exactlyOnceSourceOffsetsConsumerConfigs(
                "test", "", config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:489", consumerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override isolation level when exactly-once support is enabled
        assertEquals("read_committed", consumerConfigs.get(ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void testExactlyOnceSourceTaskProducerConfigs() {
        final Map<String, Object> connectorProducerOverrides = new HashMap<>();
        EasyMock.expect(connectorConfig.originalsWithPrefix(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX))
                .andReturn(connectorProducerOverrides)
                .anyTimes();
        PowerMock.replayAll();

        final String groupId = "connect-cluster";
        final String transactionalId = Worker.transactionalId(groupId, TASK_ID.connector(), TASK_ID.task());

        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", "localhost:4761");
        workerProps.put("group.id", groupId);
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", "connect-offsets");
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        Map<String, Object> producerConfigs = Worker.exactlyOnceSourceTaskProducerConfigs(
                TASK_ID, config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:4761", producerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("true", producerConfigs.get(ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(transactionalId, producerConfigs.get(TRANSACTIONAL_ID_CONFIG));

        workerProps.put("producer." + BOOTSTRAP_SERVERS_CONFIG, "localhost:9021");
        workerProps.put("producer." + ENABLE_IDEMPOTENCE_CONFIG, "false");
        workerProps.put("producer." + TRANSACTIONAL_ID_CONFIG, "some-other-transactional-id");
        config = new DistributedConfig(workerProps);
        producerConfigs = Worker.exactlyOnceSourceTaskProducerConfigs(
                TASK_ID, config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:9021", producerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override idempotence or transactional ID for exactly-once source tasks
        assertEquals("true", producerConfigs.get(ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(transactionalId, producerConfigs.get(TRANSACTIONAL_ID_CONFIG));

        workerProps.remove("producer." + ENABLE_IDEMPOTENCE_CONFIG);
        workerProps.remove("producer." + TRANSACTIONAL_ID_CONFIG);
        connectorProducerOverrides.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:489");
        connectorProducerOverrides.put(ENABLE_IDEMPOTENCE_CONFIG, "false");
        connectorProducerOverrides.put(TRANSACTIONAL_ID_CONFIG, "yet-another-transactional-id");
        config = new DistributedConfig(workerProps);
        producerConfigs = Worker.exactlyOnceSourceTaskProducerConfigs(
                TASK_ID, config, connectorConfig, null, allConnectorClientConfigOverridePolicy, CLUSTER_ID);
        assertEquals("localhost:489", producerConfigs.get(BOOTSTRAP_SERVERS_CONFIG));
        // User is not allowed to override idempotence or transactional ID for exactly-once source tasks
        assertEquals("true", producerConfigs.get(ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(transactionalId, producerConfigs.get(TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    public void testOffsetStoreForRegularSourceConnector() {
        expectClusterId();
        expectConverters();
        expectFileConfigProvider();
        expectStartStorage();
        expectStopStorage();
        PowerMock.replayAll();

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "disabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, WorkerTestSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config, we should only use the worker-global offsets store
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific offsets store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific offsets store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config, even with an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should still only use the worker-global offsets store
        connectorStore = worker.offsetStoreForRegularSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());

        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testOffsetStoreForExactlyOnceSourceConnector() {
        expectClusterId();
        expectConverters();
        expectFileConfigProvider();
        expectStartStorage();
        expectStopStorage();
        PowerMock.replayAll();

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, WorkerTestSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config, we should only use a connector-specific offsets store
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's,  we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceConnector(sourceConfig, CONNECTOR_ID, sourceConnector);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testOffsetStoreForRegularSourceTask() {
        expectClusterId();
        expectConverters();
        expectFileConfigProvider();
        expectStartStorage();
        expectStopStorage();

        Map<String, Object> producerProps = new HashMap<>();
        Producer<byte[], byte[]> producer = EasyMock.mock(Producer.class);
        TopicAdmin topicAdmin = EasyMock.mock(TopicAdmin.class);
        final AtomicBoolean topicAdminCreated = new AtomicBoolean(false);
        final Supplier<TopicAdmin> topicAdminSupplier = () -> {
            topicAdminCreated.set(true);
            return topicAdmin;
        };

        PowerMock.replayAll(producer, topicAdmin);

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "disabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, WorkerTestSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        topicAdminCreated.set(false);
        // With no connector-specific offsets topic in the config, we should only use the worker-global store
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdminSupplier);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());
        assertFalse(topicAdminCreated.get());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        topicAdminCreated.set(false);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdminSupplier);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertTrue(topicAdminCreated.get());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        topicAdminCreated.set(false);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdminSupplier);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertTrue(topicAdminCreated.get());

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        topicAdminCreated.set(false);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdminSupplier);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertTrue(topicAdminCreated.get());

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        topicAdminCreated.set(false);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdminSupplier);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());
        assertTrue(topicAdminCreated.get());

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        topicAdminCreated.set(false);
        // With no connector-specific offsets topic in the config and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should still only use the worker-global store
        connectorStore = worker.offsetStoreForRegularSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdminSupplier);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertFalse(connectorStore.hasConnectorSpecificStore());
        assertFalse(topicAdminCreated.get());

        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testOffsetStoreForExactlyOnceSourceTask() {
        expectClusterId();
        expectConverters();
        expectFileConfigProvider();
        expectStartStorage();
        expectStopStorage();

        Map<String, Object> producerProps = new HashMap<>();
        Producer<byte[], byte[]> producer = EasyMock.mock(Producer.class);
        TopicAdmin topicAdmin = EasyMock.mock(TopicAdmin.class);

        PowerMock.replayAll(producer, topicAdmin);

        final String workerOffsetsTopic = "worker-offsets";
        final String workerBootstrapServers = "localhost:4761";
        Map<String, String> workerProps = new HashMap<>(this.workerProps);
        workerProps.put("exactly.once.source.support", "enabled");
        workerProps.put("bootstrap.servers", workerBootstrapServers);
        workerProps.put("group.id", "connect-cluster");
        workerProps.put("config.storage.topic", "connect-configs");
        workerProps.put("offset.storage.topic", workerOffsetsTopic);
        workerProps.put("status.storage.topic", "connect-statuses");
        config = new DistributedConfig(workerProps);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, allConnectorClientConfigOverridePolicy);
        worker.start();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        connectorProps.put(CONNECTOR_CLASS_CONFIG, WorkerTestSourceConnector.class.getName());
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        SourceConnectorConfig sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        // With no connector-specific offsets topic in the config, we should only use a connector-specific offsets store
        ConnectorOffsetBackingStore connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, "connector-offsets-topic");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config (whose name differs from the worker's offsets topic), we should use both a
        // connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.put(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, workerOffsetsTopic);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and no overridden bootstrap.servers
        // for the connector, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, workerBootstrapServers);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that exactly matches the worker's, we should only use a connector-specific store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertFalse(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With a connector-specific offsets topic in the config whose name matches the worker's offsets topic, and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's, we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        connectorProps.remove(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG);
        sourceConfig = new SourceConnectorConfig(plugins, connectorProps, enableTopicCreation);
        // With no connector-specific offsets topic in the config and an overridden bootstrap.servers
        // for the connector that doesn't match the worker's,  we should use both a connector-specific store and the worker-global store
        connectorStore = worker.offsetStoreForExactlyOnceSourceTask(TASK_ID, sourceConfig, sourceConnector.getClass(), producer, producerProps, topicAdmin);
        assertTrue(connectorStore.hasWorkerGlobalStore());
        assertTrue(connectorStore.hasConnectorSpecificStore());

        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testWorkerMetrics() throws Exception {
        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        // Create
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.newConnector(WorkerTestSourceConnector.class.getName()))
                .andReturn(sourceConnector);
        EasyMock.expect(sourceConnector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(CONNECTOR_CLASS_CONFIG, WorkerTestSourceConnector.class.getName());

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

    @Test
    public void testZombieFencing() {
        KafkaFuture<Void> expectedZombieFenceFuture = EasyMock.mock(KafkaFuture.class);
        KafkaFuture<Void> fenceProducersFuture = EasyMock.mock(KafkaFuture.class);
        EasyMock.expect(fenceProducersFuture.whenComplete(anyObject())).andReturn(expectedZombieFenceFuture);
        FenceProducersResult fenceProducersResult = EasyMock.mock(FenceProducersResult.class);
        EasyMock.expect(fenceProducersResult.all()).andReturn(fenceProducersFuture);
        Admin admin = EasyMock.mock(Admin.class);
        EasyMock.expect(admin.fenceProducers(anyObject(), anyObject())).andReturn(fenceProducersResult);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        EasyMock.expect(plugins.delegatingLoader()).andReturn(delegatingLoader);
        EasyMock.expect(delegatingLoader.connectorLoader(WorkerTestSourceConnector.class.getName()))
                .andReturn(pluginLoader);
        EasyMock.expect(Plugins.compareAndSwapLoaders(pluginLoader)).andReturn(delegatingLoader);
        plugins.connectorClass(WorkerTestSourceConnector.class.getName());
        EasyMock.expectLastCall().andReturn(WorkerTestSourceConnector.class);
        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader)).andReturn(pluginLoader);

        expectConverters();
        expectStartStorage();
        expectFileConfigProvider();

        expectClusterId();

        PowerMock.replayAll(admin, fenceProducersResult, fenceProducersFuture, expectedZombieFenceFuture);

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                allConnectorClientConfigOverridePolicy);
        worker.herder = herder;
        worker.start();

        Map<String, String> connectorConfig = anyConnectorConfigMap();
        connectorConfig.put(CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX + RETRY_BACKOFF_MS_CONFIG, "4761");

        AtomicReference<Map<String, Object>> adminConfig = new AtomicReference<>();
        Function<Map<String, Object>, Admin> mockAdminConstructor = actualAdminConfig -> {
            adminConfig.set(actualAdminConfig);
            return admin;
        };

        KafkaFuture<Void> actualZombieFenceFuture =
                worker.fenceZombies(CONNECTOR_ID, 12, connectorConfig, mockAdminConstructor);

        assertEquals(expectedZombieFenceFuture, actualZombieFenceFuture);
        assertNotNull(adminConfig.get());
        assertEquals("Admin should be configured with user-specified overrides",
                "4761",
                adminConfig.get().get(RETRY_BACKOFF_MS_CONFIG)
        );

        PowerMock.verifyAll();
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
                plugins.newInternalConverter(
                        EasyMock.eq(true),
                        EasyMock.anyString(),
                        EasyMock.anyObject()
                )
        ).andReturn(internalKeyConverter);
        EasyMock.expect(
                plugins.newInternalConverter(
                        EasyMock.eq(false),
                        EasyMock.anyString(),
                        EasyMock.anyObject()
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
        props.put(CONNECTOR_CLASS_CONFIG, WorkerTestSourceConnector.class.getName());
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }

    private void expectClusterId() {
        PowerMock.mockStaticPartial(ConnectUtils.class, "lookupKafkaClusterId");
        EasyMock.expect(ConnectUtils.lookupKafkaClusterId(EasyMock.anyObject())).andReturn("test-cluster").anyTimes();
    }

    private void expectNewWorkerSourceTask() throws Exception {
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
                anyObject(ConnectorOffsetBackingStore.class),
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

    private void expectNewWorkerSinkTask(WorkerSinkTask workerTask, SinkTask task) throws Exception {
        PowerMock.expectNew(
                WorkerSinkTask.class, EasyMock.eq(TASK_ID),
                EasyMock.eq(task),
                anyObject(TaskStatus.Listener.class),
                EasyMock.eq(TargetState.STARTED),
                EasyMock.eq(config),
                anyObject(ClusterConfigState.class),
                anyObject(ConnectMetrics.class),
                anyObject(JsonConverter.class),
                anyObject(JsonConverter.class),
                anyObject(JsonConverter.class),
                EasyMock.eq(new TransformationChain<>(Collections.emptyList(), NOOP_OPERATOR)),
                anyObject(Consumer.class),
                EasyMock.eq(pluginLoader),
                anyObject(Time.class),
                anyObject(RetryWithToleranceOperator.class),
                anyObject(WorkerErrantRecordReporter.class),
                anyObject(StatusBackingStore.class))
                .andReturn(workerTask);
    }

    private void expectNewExactlyOnceWorkerSourceTask(ExactlyOnceWorkerSourceTask workerTask, Runnable preProducer, Runnable postProducer) throws Exception {
        PowerMock.expectNew(
                ExactlyOnceWorkerSourceTask.class, EasyMock.eq(TASK_ID),
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
                anyObject(ConnectorOffsetBackingStore.class),
                EasyMock.eq(config),
                anyObject(ClusterConfigState.class),
                anyObject(ConnectMetrics.class),
                EasyMock.eq(pluginLoader),
                anyObject(Time.class),
                anyObject(RetryWithToleranceOperator.class),
                anyObject(StatusBackingStore.class),
                anyObject(SourceConnectorConfig.class),
                anyObject(Executor.class),
                EasyMock.eq(preProducer),
                EasyMock.eq(postProducer))
                .andReturn(workerTask);
    }

    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class WorkerTestSourceConnector extends SourceConnector {

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

    public static class WorkerTestSinkConnector extends SinkConnector {
        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {

        }

        @Override
        public Class<? extends Task> taskClass() {
            return TestSinkTask.class;
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
            return new ConfigDef();
        }
    }

    private static class TestSinkTask extends SinkTask {
        public TestSinkTask() {
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public void put(Collection<SinkRecord> records) {
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

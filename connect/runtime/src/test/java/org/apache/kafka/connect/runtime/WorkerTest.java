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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Connector;
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
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ThreadedTest;
import org.easymock.Capture;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperatorTest.NOOP_OPERATOR;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Worker.class, Plugins.class})
@PowerMockIgnore("javax.management.*")
public class WorkerTest extends ThreadedTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);
    private static final String WORKER_ID = "localhost:8083";
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

    @Mock private Connector connector;
    @Mock private ConnectorContext ctx;
    @Mock private TestSourceTask task;
    @Mock private WorkerSourceTask workerTask;
    @Mock private Converter keyConverter;
    @Mock private Converter valueConverter;
    @Mock private Converter taskKeyConverter;
    @Mock private Converter taskValueConverter;
    @Mock private HeaderConverter taskHeaderConverter;
    @Mock private ExecutorService executorService;
    @MockNice private ConnectorConfig connectorConfig;

    @Before
    public void setup() {
        super.setup();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        config = new StandaloneConfig(workerProps);

        defaultProducerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        defaultProducerConfigs.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultProducerConfigs.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultProducerConfigs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
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

        PowerMock.mockStatic(Plugins.class);
    }

    @Test
    public void testStartAndStopConnector() {
        expectConverters();
        expectStartStorage();

        // Create
        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.newConnector(WorkerTestConnector.class.getName()))
                .andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, WorkerTestConnector.class.getName());

        EasyMock.expect(connector.version()).andReturn("1.0");

        EasyMock.expect(plugins.compareAndSwapLoaders(connector))
                .andReturn(delegatingLoader)
                .times(2);
        connector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader).times(2);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
        try {
            worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
            fail("Should have thrown exception when trying to add connector with same name.");
        } catch (ConnectException e) {
            // expected
        }
        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        worker.stopConnector(CONNECTOR_ID);
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();
        assertStatistics(worker, 0, 0);

        PowerMock.verifyAll();
    }

    @Test
    public void testStartConnectorFailure() {
        expectConverters();
        expectStartStorage();

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "java.util.HashMap"); // Bad connector class name

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader);
        EasyMock.expect(plugins.newConnector(EasyMock.anyString()))
                .andThrow(new ConnectException("Failed to find Connector"));

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader);

        connectorStatusListener.onFailure(
                EasyMock.eq(CONNECTOR_ID),
                EasyMock.<ConnectException>anyObject()
        );
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        assertStatistics(worker, 0, 0);
        assertFalse(worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED));

        assertStartupStatistics(worker, 1, 1, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());

        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 1, 0, 0);
        assertFalse(worker.stopConnector(CONNECTOR_ID));
        assertStatistics(worker, 0, 0);
        assertStartupStatistics(worker, 1, 1, 0, 0);

        PowerMock.verifyAll();
    }

    @Test
    public void testAddConnectorByAlias() {
        expectConverters();
        expectStartStorage();

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.newConnector("WorkerTestConnector")).andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "WorkerTestConnector");

        EasyMock.expect(connector.version()).andReturn("1.0");
        EasyMock.expect(plugins.compareAndSwapLoaders(connector))
                .andReturn(delegatingLoader)
                .times(2);
        connector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader)
                .times(2);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
        assertStatistics(worker, 1, 0);
        assertStartupStatistics(worker, 1, 0, 0, 0);

        worker.stopConnector(CONNECTOR_ID);
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
    public void testAddConnectorByShortAlias() {
        expectConverters();
        expectStartStorage();

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        EasyMock.expect(plugins.newConnector("WorkerTest")).andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "WorkerTest");

        EasyMock.expect(connector.version()).andReturn("1.0");
        EasyMock.expect(plugins.compareAndSwapLoaders(connector))
                .andReturn(delegatingLoader)
                .times(2);
        connector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader)
                .times(2);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
        assertStatistics(worker, 1, 0);

        worker.stopConnector(CONNECTOR_ID);
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

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        worker.stopConnector(CONNECTOR_ID);

        PowerMock.verifyAll();
    }

    @Test
    public void testReconfigureConnectorTasks() {
        expectConverters();
        expectStartStorage();

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(3);
        EasyMock.expect(plugins.newConnector(WorkerTestConnector.class.getName()))
                .andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, WorkerTestConnector.class.getName());

        EasyMock.expect(connector.version()).andReturn("1.0");
        EasyMock.expect(plugins.compareAndSwapLoaders(connector))
                .andReturn(delegatingLoader)
                .times(3);
        connector.initialize(anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        EasyMock.expect(Plugins.compareAndSwapLoaders(delegatingLoader))
                .andReturn(pluginLoader)
                .times(3);

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Reconfigure
        EasyMock.<Class<? extends Task>>expect(connector.taskClass()).andReturn(TestSourceTask.class);
        Map<String, String> taskProps = new HashMap<>();
        taskProps.put("foo", "bar");
        EasyMock.expect(connector.taskConfigs(2)).andReturn(Arrays.asList(taskProps, taskProps));

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
        worker.start();

        assertStatistics(worker, 0, 0);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertStatistics(worker, 1, 0);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
        try {
            worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
            fail("Should have thrown exception when trying to add connector with same name.");
        } catch (ConnectException e) {
            // expected
        }
        Map<String, String> connProps = new HashMap<>(props);
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
        worker.stopConnector(CONNECTOR_ID);
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

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
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
                anyObject(OffsetStorageReader.class),
                anyObject(OffsetStorageWriter.class),
                EasyMock.eq(config),
                anyObject(ClusterConfigState.class),
                anyObject(ConnectMetrics.class),
                anyObject(ClassLoader.class),
                anyObject(Time.class),
                anyObject(RetryWithToleranceOperator.class))
                .andReturn(workerTask);
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

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
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
    public void testStartTaskFailure() {
        expectConverters();
        expectStartStorage();

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

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, noneConnectorClientConfigOverridePolicy);
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

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
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
                anyObject(OffsetStorageReader.class),
                anyObject(OffsetStorageWriter.class),
                anyObject(WorkerConfig.class),
                anyObject(ClusterConfigState.class),
                anyObject(ConnectMetrics.class),
                EasyMock.eq(pluginLoader),
                anyObject(Time.class),
                anyObject(RetryWithToleranceOperator.class))
                .andReturn(workerTask);
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

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
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

        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        Capture<TestConverter> keyConverter = EasyMock.newCapture();
        Capture<TestConfigurableConverter> valueConverter = EasyMock.newCapture();
        Capture<HeaderConverter> headerConverter = EasyMock.newCapture();

        EasyMock.expect(plugins.currentThreadLoader()).andReturn(delegatingLoader).times(2);
        PowerMock.expectNew(
                WorkerSourceTask.class, EasyMock.eq(TASK_ID),
                EasyMock.eq(task),
                anyObject(TaskStatus.Listener.class),
                EasyMock.eq(TargetState.STARTED),
                EasyMock.capture(keyConverter),
                EasyMock.capture(valueConverter),
                EasyMock.capture(headerConverter),
                EasyMock.eq(new TransformationChain<>(Collections.emptyList(), NOOP_OPERATOR)),
                anyObject(KafkaProducer.class),
                anyObject(OffsetStorageReader.class),
                anyObject(OffsetStorageWriter.class),
                anyObject(WorkerConfig.class),
                anyObject(ClusterConfigState.class),
                anyObject(ConnectMetrics.class),
                EasyMock.eq(pluginLoader),
                anyObject(Time.class),
                anyObject(RetryWithToleranceOperator.class))
                .andReturn(workerTask);
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

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), plugins, config, offsetBackingStore, executorService,
                            noneConnectorClientConfigOverridePolicy);
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
            new HashMap<String, Object>());
        PowerMock.replayAll();
        Map<String, String> expectedConfigs = new HashMap<>(defaultProducerConfigs);
        expectedConfigs.put("client.id", "connector-producer-job-0");
        assertEquals(expectedConfigs,
                     Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, config, connectorConfig, null, noneConnectorClientConfigOverridePolicy));
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
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX)).andReturn(
            new HashMap<String, Object>());
        PowerMock.replayAll();
        assertEquals(expectedConfigs,
                     Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy));
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
        Map<String, Object> connConfig = new HashMap<String, Object>();
        connConfig.put("linger.ms", "5000");
        connConfig.put("batch.size", "1000");
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertEquals(expectedConfigs,
                     Worker.producerConfigs(TASK_ID, "connector-producer-" + TASK_ID, configWithOverrides, connectorConfig, null, allConnectorClientConfigOverridePolicy));
    }

    @Test
    public void testConsumerConfigsWithoutOverrides() {
        Map<String, String> expectedConfigs = new HashMap<>(defaultConsumerConfigs);
        expectedConfigs.put("group.id", "connect-test");
        expectedConfigs.put("client.id", "connector-consumer-test-1");
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).andReturn(new HashMap<>());
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), config, connectorConfig,
                                                             null, noneConnectorClientConfigOverridePolicy));
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
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)).andReturn(new HashMap<>());
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
                                                             null, noneConnectorClientConfigOverridePolicy));

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
        Map<String, Object> connConfig = new HashMap<String, Object>();
        connConfig.put("max.poll.records", "5000");
        connConfig.put("max.poll.interval.ms", "1000");
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.consumerConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
                                                             null, allConnectorClientConfigOverridePolicy));
    }

    @Test(expected = ConnectException.class)
    public void testConsumerConfigsClientOverridesWithNonePolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("consumer.auto.offset.reset", "latest");
        props.put("consumer.max.poll.records", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = new HashMap<String, Object>();
        connConfig.put("max.poll.records", "5000");
        connConfig.put("max.poll.interval.ms", "1000");
        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        Worker.consumerConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
                               null, noneConnectorClientConfigOverridePolicy);
    }

    @Test
    public void testAdminConfigsClientOverridesWithAllPolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = new HashMap<String, Object>();
        connConfig.put("metadata.max.age.ms", "10000");

        Map<String, String> expectedConfigs = new HashMap<>();
        expectedConfigs.put("bootstrap.servers", "localhost:9092");
        expectedConfigs.put("client.id", "testid");
        expectedConfigs.put("metadata.max.age.ms", "10000");

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        assertEquals(expectedConfigs, Worker.adminConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
                                                             null, allConnectorClientConfigOverridePolicy));

    }

    @Test(expected = ConnectException.class)
    public void testAdminConfigsClientOverridesWithNonePolicy() {
        Map<String, String> props = new HashMap<>(workerProps);
        props.put("admin.client.id", "testid");
        props.put("admin.metadata.max.age.ms", "5000");
        WorkerConfig configWithOverrides = new StandaloneConfig(props);

        Map<String, Object> connConfig = new HashMap<String, Object>();
        connConfig.put("metadata.max.age.ms", "10000");

        EasyMock.expect(connectorConfig.originalsWithPrefix(ConnectorConfig.CONNECTOR_CLIENT_ADMIN_OVERRIDES_PREFIX))
            .andReturn(connConfig);
        PowerMock.replayAll();
        Worker.adminConfigs(new ConnectorTaskId("test", 1), configWithOverrides, connectorConfig,
                                                          null, noneConnectorClientConfigOverridePolicy);

    }


    private void assertStatistics(Worker worker, int connectors, int tasks) {
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
        return props;
    }

    /* Name here needs to be unique as we are testing the aliasing mechanism */
    public static class WorkerTestConnector extends Connector {

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

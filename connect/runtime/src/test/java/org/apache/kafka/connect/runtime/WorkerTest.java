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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.MockTime;
import org.apache.kafka.connect.util.ThreadedTest;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Worker.class})
@PowerMockIgnore("javax.management.*")
public class WorkerTest extends ThreadedTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);
    private static final String WORKER_ID = "localhost:8083";

    private WorkerConfig config;
    private Worker worker;
    private ConnectorFactory connectorFactory = PowerMock.createMock(ConnectorFactory.class);
    private OffsetBackingStore offsetBackingStore = PowerMock.createMock(OffsetBackingStore.class);
    private TaskStatus.Listener taskStatusListener = PowerMock.createStrictMock(TaskStatus.Listener.class);
    private ConnectorStatus.Listener connectorStatusListener = PowerMock.createStrictMock(ConnectorStatus.Listener.class);

    @Before
    public void setup() {
        super.setup();

        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        config = new StandaloneConfig(workerProps);
    }

    @Test
    public void testStartAndStopConnector() throws Exception {
        expectStartStorage();

        // Create
        Connector connector = PowerMock.createMock(Connector.class);
        ConnectorContext ctx = PowerMock.createMock(ConnectorContext.class);

        EasyMock.expect(connectorFactory.newConnector(WorkerTestConnector.class.getName())).andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, WorkerTestConnector.class.getName());

        connector.initialize(EasyMock.anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
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
        worker.stopConnector(CONNECTOR_ID);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testStartConnectorFailure() throws Exception {
        expectStartStorage();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "java.util.HashMap"); // Bad connector class name

        connectorStatusListener.onFailure(EasyMock.eq(CONNECTOR_ID), EasyMock.<Throwable>anyObject());
        EasyMock.expectLastCall();

        assertFalse(worker.startConnector(CONNECTOR_ID, props, PowerMock.createMock(ConnectorContext.class), connectorStatusListener, TargetState.STARTED));

        assertEquals(Collections.emptySet(), worker.connectorNames());

        assertFalse(worker.stopConnector(CONNECTOR_ID));
    }

    @Test
    public void testAddConnectorByAlias() throws Exception {
        expectStartStorage();

        // Create
        Connector connector = PowerMock.createMock(Connector.class);
        ConnectorContext ctx = PowerMock.createMock(ConnectorContext.class);

        EasyMock.expect(connectorFactory.newConnector("WorkerTestConnector")).andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "WorkerTestConnector");

        connector.initialize(EasyMock.anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());

        worker.stopConnector(CONNECTOR_ID);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testAddConnectorByShortAlias() throws Exception {
        expectStartStorage();

        // Create
        Connector connector = PowerMock.createMock(Connector.class);
        ConnectorContext ctx = PowerMock.createMock(ConnectorContext.class);

        EasyMock.expect(connectorFactory.newConnector("WorkerTest")).andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "WorkerTest");

        connector.initialize(EasyMock.anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        connectorStatusListener.onStartup(CONNECTOR_ID);
        EasyMock.expectLastCall();

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        connectorStatusListener.onShutdown(CONNECTOR_ID);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();

        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.startConnector(CONNECTOR_ID, props, ctx, connectorStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());

        worker.stopConnector(CONNECTOR_ID);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testStopInvalidConnector() {
        expectStartStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();

        worker.stopConnector(CONNECTOR_ID);
    }

    @Test
    public void testReconfigureConnectorTasks() throws Exception {
        expectStartStorage();

        // Create
        Connector connector = PowerMock.createMock(Connector.class);
        ConnectorContext ctx = PowerMock.createMock(ConnectorContext.class);

        EasyMock.expect(connectorFactory.newConnector(WorkerTestConnector.class.getName())).andReturn(connector);
        EasyMock.expect(connector.version()).andReturn("1.0");

        Map<String, String> props = new HashMap<>();
        props.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, WorkerTestConnector.class.getName());

        connector.initialize(EasyMock.anyObject(ConnectorContext.class));
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

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

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
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
        List<Map<String, String>> taskConfigs = worker.connectorTaskConfigs(CONNECTOR_ID, 2, Arrays.asList("foo", "bar"));
        Map<String, String> expectedTaskProps = new HashMap<>();
        expectedTaskProps.put("foo", "bar");
        expectedTaskProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        expectedTaskProps.put(SinkTask.TOPICS_CONFIG, "foo,bar");
        assertEquals(2, taskConfigs.size());
        assertEquals(expectedTaskProps, taskConfigs.get(0));
        assertEquals(expectedTaskProps, taskConfigs.get(1));
        worker.stopConnector(CONNECTOR_ID);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        PowerMock.verifyAll();
    }


    @Test
    public void testAddRemoveTask() throws Exception {
        expectStartStorage();

        // Create
        TestSourceTask task = PowerMock.createMock(TestSourceTask.class);
        WorkerSourceTask workerTask = PowerMock.createMock(WorkerSourceTask.class);
        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(connectorFactory.newTask(TestSourceTask.class)).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");

        PowerMock.expectNew(
                WorkerSourceTask.class, EasyMock.eq(TASK_ID),
                EasyMock.eq(task),
                EasyMock.anyObject(TaskStatus.Listener.class),
                EasyMock.eq(TargetState.STARTED),
                EasyMock.anyObject(JsonConverter.class),
                EasyMock.anyObject(JsonConverter.class),
                EasyMock.anyObject(KafkaProducer.class),
                EasyMock.anyObject(OffsetStorageReader.class),
                EasyMock.anyObject(OffsetStorageWriter.class),
                EasyMock.anyObject(WorkerConfig.class),
                EasyMock.anyObject(Time.class))
                .andReturn(workerTask);
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        workerTask.initialize(new TaskConfig(origProps));
        EasyMock.expectLastCall();
        workerTask.run();
        EasyMock.expectLastCall();

        // Remove
        workerTask.stop();
        EasyMock.expectLastCall();
        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andStubReturn(true);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.startTask(TASK_ID, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(TASK_ID)), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testStartTaskFailure() throws Exception {
        expectStartStorage();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();

        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, "missing.From.This.Workers.Classpath");

        assertFalse(worker.startTask(TASK_ID, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED));

        taskStatusListener.onFailure(EasyMock.eq(TASK_ID), EasyMock.<Throwable>anyObject());
        EasyMock.expectLastCall();

        assertEquals(Collections.emptySet(), worker.taskIds());

        assertFalse(worker.stopAndAwaitTask(TASK_ID));
    }

    @Test
    public void testStopInvalidTask() {
        expectStartStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();

        assertFalse(worker.stopAndAwaitTask(TASK_ID));
    }

    @Test
    public void testCleanupTasksOnStop() throws Exception {
        expectStartStorage();

        // Create
        TestSourceTask task = PowerMock.createMock(TestSourceTask.class);
        WorkerSourceTask workerTask = PowerMock.createMock(WorkerSourceTask.class);
        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(connectorFactory.newTask(TestSourceTask.class)).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");
        
        PowerMock.expectNew(
                WorkerSourceTask.class, EasyMock.eq(TASK_ID),
                EasyMock.eq(task),
                EasyMock.anyObject(TaskStatus.Listener.class),
                EasyMock.eq(TargetState.STARTED),
                EasyMock.anyObject(JsonConverter.class),
                EasyMock.anyObject(JsonConverter.class),
                EasyMock.anyObject(KafkaProducer.class),
                EasyMock.anyObject(OffsetStorageReader.class),
                EasyMock.anyObject(OffsetStorageWriter.class),
                EasyMock.anyObject(WorkerConfig.class),
                EasyMock.anyObject(Time.class))
                .andReturn(workerTask);
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        workerTask.initialize(new TaskConfig(origProps));
        EasyMock.expectLastCall();
        workerTask.run();
        EasyMock.expectLastCall();

        // Remove on Worker.stop()
        workerTask.stop();
        EasyMock.expectLastCall();

        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andReturn(true);
        // Note that in this case we *do not* commit offsets since it's an unclean shutdown
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();
        worker.startTask(TASK_ID, anyConnectorConfigMap(), origProps, taskStatusListener, TargetState.STARTED);
        worker.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testConverterOverrides() throws Exception {
        expectStartStorage();

        TestSourceTask task = PowerMock.createMock(TestSourceTask.class);
        WorkerSourceTask workerTask = PowerMock.createMock(WorkerSourceTask.class);
        EasyMock.expect(workerTask.id()).andStubReturn(TASK_ID);

        EasyMock.expect(connectorFactory.newTask(TestSourceTask.class)).andReturn(task);
        EasyMock.expect(task.version()).andReturn("1.0");

        Capture<TestConverter> keyConverter = EasyMock.newCapture();
        Capture<TestConverter> valueConverter = EasyMock.newCapture();

        PowerMock.expectNew(
                WorkerSourceTask.class, EasyMock.eq(TASK_ID),
                EasyMock.eq(task),
                EasyMock.anyObject(TaskStatus.Listener.class),
                EasyMock.eq(TargetState.STARTED),
                EasyMock.capture(keyConverter),
                EasyMock.capture(valueConverter),
                EasyMock.anyObject(KafkaProducer.class),
                EasyMock.anyObject(OffsetStorageReader.class),
                EasyMock.anyObject(OffsetStorageWriter.class),
                EasyMock.anyObject(WorkerConfig.class),
                EasyMock.anyObject(Time.class))
                .andReturn(workerTask);
        Map<String, String> origProps = new HashMap<>();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        workerTask.initialize(new TaskConfig(origProps));
        EasyMock.expectLastCall();
        workerTask.run();
        EasyMock.expectLastCall();

        // Remove
        workerTask.stop();
        EasyMock.expectLastCall();
        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andStubReturn(true);
        EasyMock.expectLastCall();

        expectStopStorage();

        PowerMock.replayAll();

        worker = new Worker(WORKER_ID, new MockTime(), connectorFactory, config, offsetBackingStore);
        worker.start();
        assertEquals(Collections.emptySet(), worker.taskIds());
        Map<String, String> connProps = anyConnectorConfigMap();
        connProps.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        connProps.put("key.converter.extra.config", "foo");
        connProps.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        connProps.put("value.converter.extra.config", "bar");
        worker.startTask(TASK_ID, connProps, origProps, taskStatusListener, TargetState.STARTED);
        assertEquals(new HashSet<>(Arrays.asList(TASK_ID)), worker.taskIds());
        worker.stopAndAwaitTask(TASK_ID);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        // Validate extra configs got passed through to overridden converters
        assertEquals("foo", keyConverter.getValue().configs.get("extra.config"));
        assertEquals("bar", valueConverter.getValue().configs.get("extra.config"));

        PowerMock.verifyAll();
    }

    private void expectStartStorage() {
        offsetBackingStore.configure(EasyMock.anyObject(WorkerConfig.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();
    }

    private void expectStopStorage() {
        offsetBackingStore.stop();
        EasyMock.expectLastCall();
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
}

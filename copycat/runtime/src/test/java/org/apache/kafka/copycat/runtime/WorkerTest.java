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

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.connector.Connector;
import org.apache.kafka.copycat.connector.ConnectorContext;
import org.apache.kafka.copycat.connector.Task;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.runtime.standalone.StandaloneConfig;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.storage.Converter;
import org.apache.kafka.copycat.storage.OffsetBackingStore;
import org.apache.kafka.copycat.storage.OffsetStorageReader;
import org.apache.kafka.copycat.storage.OffsetStorageWriter;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.apache.kafka.copycat.util.MockTime;
import org.apache.kafka.copycat.util.ThreadedTest;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Worker.class)
@PowerMockIgnore("javax.management.*")
public class WorkerTest extends ThreadedTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final ConnectorTaskId TASK_ID = new ConnectorTaskId("job", 0);

    private WorkerConfig config;
    private Worker worker;
    private OffsetBackingStore offsetBackingStore = PowerMock.createMock(OffsetBackingStore.class);

    @Before
    public void setup() {
        super.setup();

        Properties workerProps = new Properties();
        workerProps.setProperty("key.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("value.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("internal.key.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("internal.value.converter", "org.apache.kafka.copycat.json.JsonConverter");
        workerProps.setProperty("internal.key.converter.schemas.enable", "false");
        workerProps.setProperty("internal.value.converter.schemas.enable", "false");
        config = new StandaloneConfig(workerProps);
    }

    @Test
    public void testAddRemoveConnector() throws Exception {
        offsetBackingStore.configure(EasyMock.anyObject(Map.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();

        // Create
        Connector connector = PowerMock.createMock(Connector.class);
        ConnectorContext ctx = PowerMock.createMock(ConnectorContext.class);

        PowerMock.mockStatic(Worker.class);
        PowerMock.expectPrivate(Worker.class, "instantiateConnector", new Object[]{TestConnector.class}).andReturn(connector);

        Properties props = new Properties();
        props.put(ConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestConnector.class.getName());

        connector.initialize(ctx);
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        offsetBackingStore.stop();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(new MockTime(), config, offsetBackingStore);
        worker.start();

        ConnectorConfig config = new ConnectorConfig(Utils.propsToStringMap(props));
        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.addConnector(config, ctx);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
        try {
            worker.addConnector(config, ctx);
            fail("Should have thrown exception when trying to add connector with same name.");
        } catch (CopycatException e) {
            // expected
        }
        worker.stopConnector(CONNECTOR_ID);
        assertEquals(Collections.emptySet(), worker.connectorNames());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        PowerMock.verifyAll();
    }

    @Test(expected = CopycatException.class)
    public void testStopInvalidConnector() {
        offsetBackingStore.configure(EasyMock.anyObject(Map.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(new MockTime(), config, offsetBackingStore);
        worker.start();

        worker.stopConnector(CONNECTOR_ID);
    }

    @Test
    public void testReconfigureConnectorTasks() throws Exception {
        offsetBackingStore.configure(EasyMock.anyObject(Map.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();

        // Create
        Connector connector = PowerMock.createMock(Connector.class);
        ConnectorContext ctx = PowerMock.createMock(ConnectorContext.class);

        PowerMock.mockStatic(Worker.class);
        PowerMock.expectPrivate(Worker.class, "instantiateConnector", new Object[]{TestConnector.class}).andReturn(connector);

        Properties props = new Properties();
        props.put(ConnectorConfig.TOPICS_CONFIG, "foo,bar");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_ID);
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestConnector.class.getName());

        connector.initialize(ctx);
        EasyMock.expectLastCall();
        connector.start(props);
        EasyMock.expectLastCall();

        // Reconfigure
        EasyMock.<Class<? extends Task>>expect(connector.taskClass()).andReturn(TestSourceTask.class);
        Properties taskProps = new Properties();
        taskProps.setProperty("foo", "bar");
        EasyMock.expect(connector.taskConfigs(2)).andReturn(Arrays.asList(taskProps, taskProps));

        // Remove
        connector.stop();
        EasyMock.expectLastCall();

        offsetBackingStore.stop();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(new MockTime(), config, offsetBackingStore);
        worker.start();

        ConnectorConfig config = new ConnectorConfig(Utils.propsToStringMap(props));
        assertEquals(Collections.emptySet(), worker.connectorNames());
        worker.addConnector(config, ctx);
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_ID)), worker.connectorNames());
        try {
            worker.addConnector(config, ctx);
            fail("Should have thrown exception when trying to add connector with same name.");
        } catch (CopycatException e) {
            // expected
        }
        List<Map<String, String>> taskConfigs = worker.connectorTaskConfigs(CONNECTOR_ID, 2, Arrays.asList("foo", "bar"));
        Properties expectedTaskProps = new Properties();
        expectedTaskProps.setProperty("foo", "bar");
        expectedTaskProps.setProperty(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        expectedTaskProps.setProperty(SinkTask.TOPICS_CONFIG, "foo,bar");
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
        offsetBackingStore.configure(EasyMock.anyObject(Map.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();

        ConnectorTaskId taskId = new ConnectorTaskId("job", 0);

        // Create
        TestSourceTask task = PowerMock.createMock(TestSourceTask.class);
        WorkerSourceTask workerTask = PowerMock.createMock(WorkerSourceTask.class);

        PowerMock.mockStatic(Worker.class);
        PowerMock.expectPrivate(Worker.class, "instantiateTask", new Object[]{TestSourceTask.class}).andReturn(task);

        PowerMock.expectNew(
                WorkerSourceTask.class, EasyMock.eq(taskId), EasyMock.eq(task),
                EasyMock.anyObject(Converter.class),
                EasyMock.anyObject(Converter.class),
                EasyMock.anyObject(KafkaProducer.class),
                EasyMock.anyObject(OffsetStorageReader.class),
                EasyMock.anyObject(OffsetStorageWriter.class),
                EasyMock.anyObject(WorkerConfig.class),
                EasyMock.anyObject(Time.class))
                .andReturn(workerTask);
        Properties origProps = new Properties();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        workerTask.start(origProps);
        EasyMock.expectLastCall();

        // Remove
        workerTask.stop();
        EasyMock.expectLastCall();
        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andStubReturn(true);
        workerTask.close();
        EasyMock.expectLastCall();

        offsetBackingStore.stop();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(new MockTime(), config, offsetBackingStore);
        worker.start();
        assertEquals(Collections.emptySet(), worker.taskIds());
        worker.addTask(taskId, new TaskConfig(Utils.propsToStringMap(origProps)));
        assertEquals(new HashSet<>(Arrays.asList(taskId)), worker.taskIds());
        worker.stopTask(taskId);
        assertEquals(Collections.emptySet(), worker.taskIds());
        // Nothing should be left, so this should effectively be a nop
        worker.stop();

        PowerMock.verifyAll();
    }

    @Test(expected = CopycatException.class)
    public void testStopInvalidTask() {
        offsetBackingStore.configure(EasyMock.anyObject(Map.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(new MockTime(), config, offsetBackingStore);
        worker.start();

        worker.stopTask(TASK_ID);
    }

    @Test
    public void testCleanupTasksOnStop() throws Exception {
        offsetBackingStore.configure(EasyMock.anyObject(Map.class));
        EasyMock.expectLastCall();
        offsetBackingStore.start();
        EasyMock.expectLastCall();

        // Create
        TestSourceTask task = PowerMock.createMock(TestSourceTask.class);
        WorkerSourceTask workerTask = PowerMock.createMock(WorkerSourceTask.class);

        PowerMock.mockStatic(Worker.class);
        PowerMock.expectPrivate(Worker.class, "instantiateTask", new Object[]{TestSourceTask.class}).andReturn(task);

        PowerMock.expectNew(
                WorkerSourceTask.class, EasyMock.eq(TASK_ID), EasyMock.eq(task),
                EasyMock.anyObject(Converter.class),
                EasyMock.anyObject(Converter.class),
                EasyMock.anyObject(KafkaProducer.class),
                EasyMock.anyObject(OffsetStorageReader.class),
                EasyMock.anyObject(OffsetStorageWriter.class),
                EasyMock.anyObject(WorkerConfig.class),
                EasyMock.anyObject(Time.class))
                .andReturn(workerTask);
        Properties origProps = new Properties();
        origProps.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
        workerTask.start(origProps);
        EasyMock.expectLastCall();

        // Remove on Worker.stop()
        workerTask.stop();
        EasyMock.expectLastCall();
        EasyMock.expect(workerTask.awaitStop(EasyMock.anyLong())).andReturn(true);
        // Note that in this case we *do not* commit offsets since it's an unclean shutdown
        workerTask.close();
        EasyMock.expectLastCall();

        offsetBackingStore.stop();
        EasyMock.expectLastCall();

        PowerMock.replayAll();

        worker = new Worker(new MockTime(), config, offsetBackingStore);
        worker.start();
        worker.addTask(TASK_ID, new TaskConfig(Utils.propsToStringMap(origProps)));
        worker.stop();

        PowerMock.verifyAll();
    }


    private static class TestConnector extends Connector {
        @Override
        public void start(Properties props) {

        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Properties> taskConfigs(int maxTasks) {
            return null;
        }

        @Override
        public void stop() {

        }
    }

    private static class TestSourceTask extends SourceTask {
        public TestSourceTask() {
        }

        @Override
        public void start(Properties props) {
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            return null;
        }

        @Override
        public void stop() {
        }
    }
}

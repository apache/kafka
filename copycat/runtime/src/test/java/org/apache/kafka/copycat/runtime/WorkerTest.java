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
import org.apache.kafka.copycat.cli.WorkerConfig;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.storage.*;
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

import java.util.List;
import java.util.Map;
import java.util.Properties;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Worker.class)
@PowerMockIgnore("javax.management.*")
public class WorkerTest extends ThreadedTest {

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
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
        config = new WorkerConfig(workerProps);
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
        PowerMock.expectPrivate(Worker.class, "instantiateTask", TestSourceTask.class.getName()).andReturn(task);

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
        worker.addTask(taskId, TestSourceTask.class.getName(), origProps);
        worker.stopTask(taskId);
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

        worker.stopTask(taskId);
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
        PowerMock.expectPrivate(Worker.class, "instantiateTask", TestSourceTask.class.getName()).andReturn(task);

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
        worker.addTask(taskId, TestSourceTask.class.getName(), origProps);
        worker.stop();

        PowerMock.verifyAll();
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

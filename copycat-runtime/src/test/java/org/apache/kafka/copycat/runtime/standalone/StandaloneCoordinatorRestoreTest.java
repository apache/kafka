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

package org.apache.kafka.copycat.runtime.standalone;

import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.KafkaUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.Properties;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StandaloneCoordinator.class, KafkaUtils.class})
@PowerMockIgnore("javax.management.*")
public class StandaloneCoordinatorRestoreTest extends StandaloneCoordinatorTestBase {
    private File coordinatorConfigFile;

    @Before
    public void setup() throws Exception {
        worker = PowerMock.createMock(Worker.class);
        Properties coordinatorProps = new Properties();
        coordinatorProps.setProperty(StandaloneCoordinator.STORAGE_CONFIG,
                FileConfigStorage.class.getName());
        coordinatorConfigFile = File.createTempFile("test-coordinator-config", null);
        coordinatorConfigFile.delete(); // Delete since we just needed a random file path
        coordinatorProps.setProperty(FileConfigStorage.FILE_CONFIG,
                coordinatorConfigFile.getAbsolutePath());
        coordinator = new StandaloneCoordinator(worker, coordinatorProps);
        createCallback = PowerMock.createMock(Callback.class);

        connectorProps = new Properties();
        connectorProps.setProperty(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        connectorProps.setProperty(SinkConnector.TOPICS_CONFIG, TOPICS_LIST_STR);
        PowerMock.mockStatic(StandaloneCoordinator.class);
        PowerMock.mockStatic(KafkaUtils.class);

        // These can be anything since connectors can pass along whatever they want.
        taskProps = new Properties();
        taskProps.setProperty("foo", "bar");
    }

    @After
    public void tearDown() {
        coordinatorConfigFile.delete();
    }

    @Test
    public void testRestoreConnectors() throws Exception {
        connector = PowerMock.createMock(BogusSourceClass.class);
        expectAdd(BogusSourceClass.class, BogusSourceTask.class, false);
        expectStop();
        // Restarting should recreate the same connector
        expectRestore(BogusSourceClass.class, BogusSourceTask.class);
        expectStop();

        PowerMock.replayAll();

        // One run to seed the config storage with the job
        coordinator.start();
        coordinator.addConnector(connectorProps, createCallback);
        coordinator.stop();

        // Second run should restore the connector
        coordinator.start();
        coordinator.stop();

        PowerMock.verifyAll();
    }
}

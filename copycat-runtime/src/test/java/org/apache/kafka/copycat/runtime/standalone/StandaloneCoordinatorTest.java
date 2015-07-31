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
import org.apache.kafka.copycat.util.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StandaloneCoordinator.class})
@PowerMockIgnore("javax.management.*")
public class StandaloneCoordinatorTest extends StandaloneCoordinatorTestBase {

    @Before
    public void setup() {
        worker = PowerMock.createMock(Worker.class);
        coordinator = new StandaloneCoordinator(worker, new Properties());
        createCallback = PowerMock.createMock(Callback.class);

        connectorProps = new Properties();
        connectorProps.setProperty(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        connectorProps.setProperty(SinkConnector.TOPICS_CONFIG, TOPICS_LIST_STR);
        PowerMock.mockStatic(StandaloneCoordinator.class);

        // These can be anything since connectors can pass along whatever they want.
        taskProps = new Properties();
        taskProps.setProperty("foo", "bar");
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceClass.class);
        expectAdd(BogusSourceClass.class, BogusSourceTask.class, false);
        PowerMock.replayAll();

        coordinator.addConnector(connectorProps, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        connector = PowerMock.createMock(BogusSinkClass.class);
        expectAdd(BogusSinkClass.class, BogusSinkTask.class, true);

        PowerMock.replayAll();

        coordinator.addConnector(connectorProps, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceClass.class);
        expectAdd(BogusSourceClass.class, BogusSourceTask.class, false);
        expectDestroy();
        PowerMock.replayAll();

        coordinator.addConnector(connectorProps, createCallback);
        FutureCallback<Void> futureCb = new FutureCallback<>(new Callback() {
            @Override
            public void onCompletion(Throwable error, Object result) {

            }
        });
        coordinator.deleteConnector(CONNECTOR_NAME, futureCb);
        futureCb.get(1000L, TimeUnit.MILLISECONDS);
        PowerMock.verifyAll();
    }
}

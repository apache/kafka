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

import org.apache.kafka.copycat.connector.Connector;
import org.apache.kafka.copycat.connector.Task;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.source.SourceConnector;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.apache.kafka.copycat.util.FutureCallback;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StandaloneHerder.class})
@PowerMockIgnore("javax.management.*")
public class StandaloneHerderTest {
    private static final String CONNECTOR_NAME = "test";
    private static final String TOPICS_LIST_STR = "topic1,topic2";

    private StandaloneHerder herder;
    @Mock protected Worker worker;
    private Connector connector;
    @Mock protected Callback<String> createCallback;

    private Properties connectorProps;
    private Properties taskProps;

    @Before
    public void setup() {
        worker = PowerMock.createMock(Worker.class);
        herder = new StandaloneHerder(worker);

        connectorProps = new Properties();
        connectorProps.setProperty(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        connectorProps.setProperty(SinkConnector.TOPICS_CONFIG, TOPICS_LIST_STR);
        PowerMock.mockStatic(StandaloneHerder.class);

        // These can be anything since connectors can pass along whatever they want.
        taskProps = new Properties();
        taskProps.setProperty("foo", "bar");
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceClass.class);
        expectAdd(BogusSourceClass.class, BogusSourceTask.class, false);
        PowerMock.replayAll();

        herder.addConnector(connectorProps, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        connector = PowerMock.createMock(BogusSinkClass.class);
        expectAdd(BogusSinkClass.class, BogusSinkTask.class, true);

        PowerMock.replayAll();

        herder.addConnector(connectorProps, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceClass.class);
        expectAdd(BogusSourceClass.class, BogusSourceTask.class, false);
        expectDestroy();
        PowerMock.replayAll();

        herder.addConnector(connectorProps, createCallback);
        FutureCallback<Void> futureCb = new FutureCallback<>(new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {

            }
        });
        herder.deleteConnector(CONNECTOR_NAME, futureCb);
        futureCb.get(1000L, TimeUnit.MILLISECONDS);
        PowerMock.verifyAll();
    }


    private void expectAdd(Class<? extends Connector> connClass,
                             Class<? extends Task> taskClass,
                             boolean sink) throws Exception {
        expectCreate(connClass, taskClass, sink, true);
    }

    private void expectRestore(Class<? extends Connector> connClass,
                                 Class<? extends Task> taskClass) throws Exception {
        // Restore never uses a callback. These tests always use sources
        expectCreate(connClass, taskClass, false, false);
    }

    private void expectCreate(Class<? extends Connector> connClass,
                                Class<? extends Task> taskClass,
                                boolean sink, boolean expectCallback) throws Exception {
        connectorProps.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connClass.getName());

        PowerMock.expectPrivate(StandaloneHerder.class, "instantiateConnector", connClass.getName())
                .andReturn(connector);
        if (expectCallback) {
            createCallback.onCompletion(null, CONNECTOR_NAME);
            PowerMock.expectLastCall();
        }

        connector.initialize(EasyMock.anyObject(StandaloneConnectorContext.class));
        PowerMock.expectLastCall();
        connector.start(new Properties());
        PowerMock.expectLastCall();

        // Just return the connector properties for the individual task we generate by default
        EasyMock.<Class<? extends Task>>expect(connector.getTaskClass()).andReturn(taskClass);

        EasyMock.expect(connector.getTaskConfigs(ConnectorConfig.TASKS_MAX_DEFAULT))
                .andReturn(Arrays.asList(taskProps));
        // And we should instantiate the tasks. For a sink task, we should see added properties for
        // the input topic partitions
        Properties generatedTaskProps = new Properties();
        generatedTaskProps.putAll(taskProps);
        if (sink)
            generatedTaskProps.setProperty(SinkTask.TOPICS_CONFIG, TOPICS_LIST_STR);
        worker.addTask(new ConnectorTaskId(CONNECTOR_NAME, 0), taskClass.getName(), generatedTaskProps);
        PowerMock.expectLastCall();
    }

    private void expectStop() {
        worker.stopTask(new ConnectorTaskId(CONNECTOR_NAME, 0));
        EasyMock.expectLastCall();
        connector.stop();
        EasyMock.expectLastCall();
    }

    private void expectDestroy() {
        expectStop();
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceClass extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

    private abstract class BogusSinkClass extends SinkConnector {
    }

    private abstract class BogusSinkTask extends SourceTask {
    }

}

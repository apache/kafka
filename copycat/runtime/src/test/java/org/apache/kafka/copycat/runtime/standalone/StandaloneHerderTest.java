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
import org.apache.kafka.copycat.errors.AlreadyExistsException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.HerderConnectorContext;
import org.apache.kafka.copycat.runtime.TaskConfig;
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
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
public class StandaloneHerderTest {
    private static final String CONNECTOR_NAME = "test";
    private static final List<String> TOPICS_LIST = Arrays.asList("topic1", "topic2");
    private static final String TOPICS_LIST_STR = "topic1,topic2";
    private static final int DEFAULT_MAX_TASKS = 1;

    private StandaloneHerder herder;
    @Mock protected Worker worker;
    private Connector connector;
    @Mock protected Callback<String> createCallback;

    private Map<String, String> connectorProps;
    private Map<String, String> taskProps;

    @Before
    public void setup() {
        worker = PowerMock.createMock(Worker.class);
        herder = new StandaloneHerder(worker);

        connectorProps = new HashMap<>();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        connectorProps.put(SinkConnector.TOPICS_CONFIG, TOPICS_LIST_STR);

        // These can be anything since connectors can pass along whatever they want.
        taskProps = new HashMap<>();
        taskProps.put("foo", "bar");
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(BogusSourceConnector.class, BogusSourceTask.class, false);
        PowerMock.replayAll();

        herder.addConnector(connectorProps, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorAlreadyExists() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        // First addition should succeed
        expectAdd(BogusSourceConnector.class, BogusSourceTask.class, false);

        // Second should fail
        createCallback.onCompletion(EasyMock.<AlreadyExistsException>anyObject(), EasyMock.<String>isNull());
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.addConnector(connectorProps, createCallback);
        herder.addConnector(connectorProps, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        connector = PowerMock.createMock(BogusSinkConnector.class);
        expectAdd(BogusSinkConnector.class, BogusSinkTask.class, true);

        PowerMock.replayAll();

        herder.addConnector(connectorProps, createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(BogusSourceConnector.class, BogusSourceTask.class, false);
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

    @Test
    public void testCreateAndStop() throws Exception {
        connector = PowerMock.createMock(BogusSourceConnector.class);
        expectAdd(BogusSourceConnector.class, BogusSourceTask.class, false);
        // herder.stop() should stop any running connectors and tasks even if destroyConnector was not invoked
        expectStop();

        PowerMock.replayAll();

        herder.addConnector(connectorProps, createCallback);
        herder.stop();

        PowerMock.verifyAll();
    }

    private void expectAdd(Class<? extends Connector> connClass,
                           Class<? extends Task> taskClass,
                           boolean sink) throws Exception {
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connClass.getName());

        worker.addConnector(EasyMock.eq(new ConnectorConfig(connectorProps)), EasyMock.anyObject(HerderConnectorContext.class));
        PowerMock.expectLastCall();

        createCallback.onCompletion(null, CONNECTOR_NAME);
        PowerMock.expectLastCall();

        // And we should instantiate the tasks. For a sink task, we should see added properties for
        // the input topic partitions
        Map<String, String> generatedTaskProps = new HashMap<>();
        generatedTaskProps.putAll(taskProps);
        generatedTaskProps.put(TaskConfig.TASK_CLASS_CONFIG, taskClass.getName());
        if (sink)
            generatedTaskProps.put(SinkTask.TOPICS_CONFIG, TOPICS_LIST_STR);
        EasyMock.expect(worker.reconfigureConnectorTasks(CONNECTOR_NAME, DEFAULT_MAX_TASKS, TOPICS_LIST))
                .andReturn(Collections.singletonList(generatedTaskProps));

        worker.addTask(new ConnectorTaskId(CONNECTOR_NAME, 0), new TaskConfig(generatedTaskProps));
        PowerMock.expectLastCall();
    }

    private void expectStop() {
        worker.stopTask(new ConnectorTaskId(CONNECTOR_NAME, 0));
        EasyMock.expectLastCall();
        worker.stopConnector(CONNECTOR_NAME);
        EasyMock.expectLastCall();
    }

    private void expectDestroy() {
        expectStop();
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

    private abstract class BogusSinkConnector extends SinkConnector {
    }

    private abstract class BogusSinkTask extends SourceTask {
    }

}

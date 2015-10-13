/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime.distributed;

import org.apache.kafka.copycat.connector.Connector;
import org.apache.kafka.copycat.connector.Task;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.HerderConnectorContext;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.source.SourceConnector;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.storage.KafkaConfigStorage;
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

import java.util.*;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DistributedHerder.class})
@PowerMockIgnore("javax.management.*")
public class DistributedHerderTest {
    private static final List<String> CONNECTOR_NAMES = Arrays.asList("source-test1", "source-test2", "sink-test3");
    private static final List<String> SOURCE_CONNECTOR_NAMES = Arrays.asList("source-test1", "source-test2");
    private static final List<String> SINK_CONNECTOR_NAMES = Arrays.asList("sink-test3");
    private static final String TOPICS_LIST_STR = "topic1,topic2";

    private static final Map<String, String> CONFIG_STORAGE_CONFIG = Collections.singletonMap(KafkaConfigStorage.CONFIG_TOPIC_CONFIG, "config-topic");

    @Mock private KafkaConfigStorage configStorage;
    private DistributedHerder herder;
    @Mock private Worker worker;
    @Mock private Callback<String> createCallback;

    private Map<String, Map<String, String>> connectorProps;
    private Map<String, Class<? extends Connector>> connectorClasses;
    private Map<String, Class<? extends Task>> connectorTaskClasses;
    private Map<String, Connector> connectors;
    private Properties taskProps;

    @Before
    public void setUp() {
        worker = PowerMock.createMock(Worker.class);
        herder = new DistributedHerder(worker, configStorage);

        connectorProps = new HashMap<>();
        connectorClasses = new HashMap<>();
        connectorTaskClasses = new HashMap<>();
        connectors = new HashMap<>();
        for (String connectorName : CONNECTOR_NAMES) {
            Class<? extends Connector> connectorClass = connectorName.contains("source") ? BogusSourceConnector.class : BogusSinkConnector.class;
            Class<? extends Task> taskClass = connectorName.contains("source") ? BogusSourceTask.class : BogusSinkTask.class;
            Connector connector = connectorName.contains("source") ? PowerMock.createMock(BogusSourceConnector.class) : PowerMock.createMock(BogusSinkConnector.class);

            Map<String, String> props = new HashMap<>();
            props.put(ConnectorConfig.NAME_CONFIG, connectorName);
            props.put(SinkConnector.TOPICS_CONFIG, TOPICS_LIST_STR);
            props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());

            connectorProps.put(connectorName, props);
            connectorClasses.put(connectorName, connectorClass);
            connectorTaskClasses.put(connectorName, taskClass);
            connectors.put(connectorName, connector);
        }

        PowerMock.mockStatic(DistributedHerder.class);

        // These can be anything since connectors can pass along whatever they want.
        taskProps = new Properties();
        taskProps.setProperty("foo", "bar");
    }

    @Test
    public void testCreateSourceConnector() throws Exception {
        String connectorName = SOURCE_CONNECTOR_NAMES.get(0);

        expectConfigStorageConfigureStart();
        expectEmptyRestore();
        expectAdd(connectorName);
        PowerMock.replayAll();

        herder.configure(CONFIG_STORAGE_CONFIG);
        herder.start();
        herder.addConnector(connectorProps.get(connectorName), createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateSinkConnector() throws Exception {
        String connectorName = SINK_CONNECTOR_NAMES.get(0);

        expectConfigStorageConfigureStart();
        expectEmptyRestore();
        expectAdd(connectorName);
        PowerMock.replayAll();

        herder.configure(CONFIG_STORAGE_CONFIG);
        herder.start();
        herder.addConnector(connectorProps.get(connectorName), createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testDestroyConnector() throws Exception {
        String connectorName = SOURCE_CONNECTOR_NAMES.get(0);

        expectConfigStorageConfigureStart();
        expectEmptyRestore();
        expectAdd(connectorName);
        expectDestroy(connectorName);
        PowerMock.replayAll();

        herder.configure(CONFIG_STORAGE_CONFIG);
        herder.start();
        herder.addConnector(connectorProps.get(connectorName), createCallback);

        FutureCallback<Void> futureCb = new FutureCallback<>(new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {

            }
        });
        herder.deleteConnector(CONNECTOR_NAMES.get(0), futureCb);
        futureCb.get(1000L, TimeUnit.MILLISECONDS);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateAndStop() throws Exception {
        String connectorName = SOURCE_CONNECTOR_NAMES.get(0);

        expectConfigStorageConfigureStart();
        expectEmptyRestore();
        expectAdd(connectorName);
        PowerMock.replayAll();

        herder.configure(CONFIG_STORAGE_CONFIG);
        herder.start();
        herder.addConnector(connectorProps.get(connectorName), createCallback);

        PowerMock.verifyAll();
    }

    @Test
    public void testRestoreAndStop() throws Exception {
        String restoreConnectorName1 = SOURCE_CONNECTOR_NAMES.get(0);
        String restoreConnectorName2 = SINK_CONNECTOR_NAMES.get(0);
        String additionalConnectorName = SOURCE_CONNECTOR_NAMES.get(1);

        expectConfigStorageConfigureStart();
        expectRestore(Arrays.asList(restoreConnectorName1, restoreConnectorName2));
        expectAdd(additionalConnectorName);
        // Stopping the herder should correctly stop all restored and new connectors
        expectStop(restoreConnectorName1);
        expectStop(restoreConnectorName2);
        expectStop(additionalConnectorName);
        configStorage.stop();
        PowerMock.expectLastCall();

        PowerMock.replayAll();

        herder.configure(CONFIG_STORAGE_CONFIG);
        herder.start();
        herder.addConnector(connectorProps.get(additionalConnectorName), createCallback);
        herder.stop();

        PowerMock.verifyAll();
    }

    private void expectConfigStorageConfigureStart() {
        configStorage.configure(CONFIG_STORAGE_CONFIG);
        PowerMock.expectLastCall();
        configStorage.start();
        PowerMock.expectLastCall();
    }

    private void expectAdd(String connectorName) throws Exception {
        configStorage.putConnectorConfig(connectorName, connectorProps.get(connectorName));
        PowerMock.expectLastCall();
        expectInstantiateConnector(connectorName, true);
    }

    private void expectEmptyRestore() throws Exception {
        expectRestore(Collections.<String>emptyList());
    }

    private void expectRestore(List<String> connectorNames) throws Exception {
        Map<String, Integer> rootConfig = new HashMap<>();
        Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
        for (String connName : connectorNames) {
            rootConfig.put(connName, 0);
            connectorConfigs.put(connName, connectorProps.get(connName));
        }
        EasyMock.expect(configStorage.snapshot())
                .andReturn(new ClusterConfigState(1, rootConfig, connectorConfigs, Collections.EMPTY_MAP, Collections.EMPTY_SET));

        // Restore never uses a callback
        for (String connectorName : connectorNames)
            expectInstantiateConnector(connectorName, false);
    }

    private void expectInstantiateConnector(String connectorName, boolean expectCallback) throws Exception {
        PowerMock.expectPrivate(DistributedHerder.class, "instantiateConnector", connectorClasses.get(connectorName).getName())
                .andReturn(connectors.get(connectorName));
        if (expectCallback) {
            createCallback.onCompletion(null, connectorName);
            PowerMock.expectLastCall();
        }

        Connector connector = connectors.get(connectorName);
        connector.initialize(EasyMock.anyObject(HerderConnectorContext.class));
        PowerMock.expectLastCall();
        connector.start(new Properties());
        PowerMock.expectLastCall();

        // Just return the connector properties for the individual task we generate by default
        EasyMock.<Class<? extends Task>>expect(connector.taskClass()).andReturn(connectorTaskClasses.get(connectorName));

        EasyMock.expect(connector.taskConfigs(ConnectorConfig.TASKS_MAX_DEFAULT))
                .andReturn(Arrays.asList(taskProps));
        // And we should instantiate the tasks. For a sink task, we should see added properties for
        // the input topic partitions
        Properties generatedTaskProps = new Properties();
        generatedTaskProps.putAll(taskProps);
        if (connectorName.contains("sink"))
            generatedTaskProps.setProperty(SinkTask.TOPICS_CONFIG, TOPICS_LIST_STR);
        ConnectorTaskId taskId = new ConnectorTaskId(connectorName, 0);
        worker.addTask(taskId, connectorTaskClasses.get(connectorName).getName(), generatedTaskProps);
        PowerMock.expectLastCall();
    }

    private void expectStop(String connectorName) {
        worker.stopTask(new ConnectorTaskId(connectorName, 0));
        EasyMock.expectLastCall();
        Connector connector = connectors.get(connectorName);
        connector.stop();
        EasyMock.expectLastCall();
    }

    private void expectDestroy(String connectorName) {
        expectStop(connectorName);
        configStorage.putConnectorConfig(connectorName, null);
        PowerMock.expectLastCall();
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

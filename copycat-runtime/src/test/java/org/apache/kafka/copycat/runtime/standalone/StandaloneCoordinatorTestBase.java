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
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.source.SourceConnector;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.easymock.EasyMock;
import org.powermock.api.easymock.PowerMock;

import java.util.Arrays;
import java.util.Properties;

public class StandaloneCoordinatorTestBase {

    protected static final String CONNECTOR_NAME = "test";
    protected static final String TOPICS_LIST_STR = "topic1,topic2";

    protected StandaloneCoordinator coordinator;
    protected Worker worker;
    protected Connector connector;
    protected Callback<String> createCallback;

    protected Properties connectorProps;
    protected Properties taskProps;

    protected void expectAdd(Class<? extends Connector> connClass,
                             Class<? extends Task> taskClass,
                             boolean sink) throws Exception {
        expectCreate(connClass, taskClass, sink, true);
    }

    protected void expectRestore(Class<? extends Connector> connClass,
                                 Class<? extends Task> taskClass) throws Exception {
        // Restore never uses a callback. These tests always use sources
        expectCreate(connClass, taskClass, false, false);
    }

    protected void expectCreate(Class<? extends Connector> connClass,
                                Class<? extends Task> taskClass,
                                boolean sink, boolean expectCallback) throws Exception {
        connectorProps.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connClass.getName());

        PowerMock.expectPrivate(StandaloneCoordinator.class, "instantiateConnector", connClass.getName())
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

    protected void expectStop() throws CopycatException {
        worker.stopTask(new ConnectorTaskId(CONNECTOR_NAME, 0));
        EasyMock.expectLastCall();
        connector.stop();
        EasyMock.expectLastCall();
    }

    protected void expectDestroy() throws CopycatException {
        expectStop();
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    protected abstract class BogusSourceClass extends SourceConnector {
    }

    protected abstract class BogusSourceTask extends SourceTask {
    }

    protected abstract class BogusSinkClass extends SinkConnector {
    }

    protected abstract class BogusSinkTask extends SourceTask {
    }
}

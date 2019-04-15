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
package org.apache.kafka.connect.runtime.health;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(PowerMockRunner.class)
public class ConnectClusterStateImplTest {
  
    @Mock
    protected Herder herder;
    protected ConnectClusterStateImpl connectClusterState;
    protected long herderRequestTimeoutMs = TimeUnit.SECONDS.toMillis(10);
    
    @Before
    public void setUp() {
        connectClusterState = new ConnectClusterStateImpl(herderRequestTimeoutMs, herder);
    }
    
    @Test
    public void connectors() {
        final Collection<String> expectedConnectors = Arrays.asList("sink1", "source1", "source2");
        Capture<Callback<Collection<String>>> callback = EasyMock.newCapture();
        herder.connectors(EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() {
                callback.getValue().onCompletion(null, expectedConnectors);
                return null;
            }
        });
        EasyMock.replay(herder);
        assertEquals(expectedConnectors, connectClusterState.connectors());
    }

    @Test
    public void connectorConfig() {
        final String connName = "sink6";
        final Map<String, String> expectedConfig = Collections.singletonMap("key", "value");
        Capture<Callback<Map<String, String>>> callback = EasyMock.newCapture();
        herder.connectorConfig(EasyMock.eq(connName), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() {
                callback.getValue().onCompletion(null, expectedConfig);
                return null;
            }
        });
        EasyMock.replay(herder);
        assertEquals(expectedConfig, connectClusterState.connectorConfig(connName));
    }

    @Test
    public void taskConfigs() {
        final String connName = "sink9";
        final Map<String, String> taskConfig0 = Collections.singletonMap("k1", "v1");
        final Map<String, String> taskConfig1 = Collections.singletonMap("k4", "v20"); 

        final Map<Integer, Map<String, String>> expectedConfigs = new HashMap<>();
        expectedConfigs.put(0, taskConfig0);
        expectedConfigs.put(1, taskConfig1);

        Capture<Callback<List<TaskInfo>>> callback = EasyMock.newCapture();
        herder.taskConfigs(EasyMock.eq(connName), EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() {
                List<TaskInfo> taskInfos = Arrays.asList(
                    new TaskInfo(new ConnectorTaskId(connName, 0), taskConfig0),
                    new TaskInfo(new ConnectorTaskId(connName, 1), taskConfig1)
                );
                callback.getValue().onCompletion(null, taskInfos);
                return null;
            }
        });

        EasyMock.replay(herder);
        assertEquals(expectedConfigs, connectClusterState.taskConfigs(connName));
    }

    @Test
    public void kafkaClusterId() {
        final String kafkaClusterId = "kclstr";
        EasyMock.expect(herder.kafkaClusterId()).andReturn(kafkaClusterId);
        EasyMock.replay(herder);
        assertEquals(kafkaClusterId, connectClusterState.kafkaClusterId());
    }

    @Test
    public void connectorsFailure() {
        Capture<Callback<Collection<String>>> callback = EasyMock.newCapture();
        herder.connectors(EasyMock.capture(callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() {
                Throwable timeout = new TimeoutException();
                callback.getValue().onCompletion(timeout, null);
                return null;
            }
        });
        EasyMock.replay(herder);
        assertThrows(ConnectException.class, connectClusterState::connectors);
    }
}

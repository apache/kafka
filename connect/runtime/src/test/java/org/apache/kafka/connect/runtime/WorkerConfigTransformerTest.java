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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigChangeCallback;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigProvider;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.replayAll;

@RunWith(PowerMockRunner.class)
public class WorkerConfigTransformerTest {

    @Mock private Herder herder;
    @Mock private Worker worker;
    private WorkerConfigTransformer configTransformer;

    @Before
    public void setup() {
        worker = PowerMock.createMock(Worker.class);
        herder = PowerMock.createMock(Herder.class);
        configTransformer = new WorkerConfigTransformer(worker, Collections.singletonMap("test", new TestConfigProvider()));
    }

    @Test
    public void testReplaceVariable() throws Exception {
        Map<String, String> result = configTransformer.transform("myConnector", Collections.singletonMap("myKey", "${test:testPath:testKey}"));
        assertEquals("testResult", result.get("myKey"));
    }

    @Test
    public void testReplaceVariableWithTTL() throws Exception {
        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.getConnectorConfigReloadAction("myConnector")).andReturn(ConnectorConfig.CONFIG_RELOAD_ACTION_NONE);

        replayAll();

        Map<String, String> result = configTransformer.transform("myConnector", Collections.singletonMap("myKey", "${test:testPath:testKeyWithTTL}"));
        assertEquals("testResultWithTTL", result.get("myKey"));
    }

    @Test
    public void testReplaceVariableWithTTLAndScheduleRestart() throws Exception {
        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.getConnectorConfigReloadAction("myConnector")).andReturn(ConnectorConfig.CONFIG_RELOAD_ACTION_RESTART);
        EasyMock.expect(herder.restartConnector(1L, "myConnector", null)).andReturn(new TestHerderRequestId());

        replayAll();

        Map<String, String> result = configTransformer.transform("myConnector", Collections.singletonMap("myKey", "${test:testPath:testKeyWithTTL}"));
        assertEquals("testResultWithTTL", result.get("myKey"));
    }

    @Test
    public void testReplaceVariableWithTTLFirstCancelThenScheduleRestart() throws Exception {
        HerderRequestId herderRequestId = new TestHerderRequestId();

        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.getConnectorConfigReloadAction("myConnector")).andReturn(ConnectorConfig.CONFIG_RELOAD_ACTION_RESTART);
        EasyMock.expect(herder.restartConnector(1L, "myConnector", null)).andReturn(herderRequestId);

        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.getConnectorConfigReloadAction("myConnector")).andReturn(ConnectorConfig.CONFIG_RELOAD_ACTION_RESTART);
        herder.cancelRequest(herderRequestId);
        EasyMock.expectLastCall();
        EasyMock.expect(herder.restartConnector(10L, "myConnector", null)).andReturn(herderRequestId);

        replayAll();

        Map<String, String> result = configTransformer.transform("myConnector", Collections.singletonMap("myKey", "${test:testPath:testKeyWithTTL}"));
        assertEquals("testResultWithTTL", result.get("myKey"));

        result = configTransformer.transform("myConnector", Collections.singletonMap("myKey", "${test:testPath:testKeyWithLongerTTL}"));
        assertEquals("testResultWithLongerTTL", result.get("myKey"));
    }

    public static class TestConfigProvider implements ConfigProvider {

        public void configure(Map<String, ?> configs) {
        }

        public ConfigData get(String path) {
            return null;
        }

        public ConfigData get(String path, Set<String> keys) {
            if (path.equals("testPath")) {
                if (keys.contains("testKey")) {
                    return new ConfigData(Collections.singletonMap("testKey", "testResult"));
                } else if (keys.contains("testKeyWithTTL")) {
                    return new ConfigData(Collections.singletonMap("testKeyWithTTL", "testResultWithTTL"), 1L);
                } else if (keys.contains("testKeyWithLongerTTL")) {
                    return new ConfigData(Collections.singletonMap("testKeyWithLongerTTL", "testResultWithLongerTTL"), 10L);
                }
            }
            return new ConfigData(Collections.emptyMap());
        }

        public void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
            throw new UnsupportedOperationException();
        }

        public void unsubscribe(String path, Set<String> keys) {
            throw new UnsupportedOperationException();
        }

        public void close() {
        }
    }

    public static class TestHerderRequestId implements HerderRequestId {
    }
}

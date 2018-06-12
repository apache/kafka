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
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.replayAll;

@RunWith(PowerMockRunner.class)
public class WorkerConfigTransformerTest {

    public static final String MY_KEY = "myKey";
    public static final String MY_CONNECTOR = "myConnector";
    public static final String TEST_KEY = "testKey";
    public static final String TEST_PATH = "testPath";
    public static final String TEST_KEY_WITH_TTL = "testKeyWithTTL";
    public static final String TEST_KEY_WITH_LONGER_TTL = "testKeyWithLongerTTL";
    public static final String TEST_RESULT = "testResult";
    public static final String TEST_RESULT_WITH_TTL = "testResultWithTTL";
    public static final String TEST_RESULT_WITH_LONGER_TTL = "testResultWithLongerTTL";

    @Mock private Herder herder;
    @Mock private Worker worker;
    @Mock private HerderRequest requestId;
    private WorkerConfigTransformer configTransformer;

    @Before
    public void setup() {
        worker = PowerMock.createMock(Worker.class);
        herder = PowerMock.createMock(Herder.class);
        configTransformer = new WorkerConfigTransformer(worker, Collections.singletonMap("test", new TestConfigProvider()));
    }

    @Test
    public void testReplaceVariable() throws Exception {
        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKey}"));
        assertEquals(TEST_RESULT, result.get(MY_KEY));
    }

    @Test
    public void testReplaceVariableWithTTL() throws Exception {
        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.connectorConfigReloadAction(MY_CONNECTOR)).andReturn(Herder.ConfigReloadAction.NONE);

        replayAll();

        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithTTL}"));
        assertEquals(TEST_RESULT_WITH_TTL, result.get(MY_KEY));
    }

    @Test
    public void testReplaceVariableWithTTLAndScheduleRestart() throws Exception {
        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.connectorConfigReloadAction(MY_CONNECTOR)).andReturn(Herder.ConfigReloadAction.RESTART);
        EasyMock.expect(herder.restartConnector(1L, MY_CONNECTOR, null)).andReturn(requestId);

        replayAll();

        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithTTL}"));
        assertEquals(TEST_RESULT_WITH_TTL, result.get(MY_KEY));
    }

    @Test
    public void testReplaceVariableWithTTLFirstCancelThenScheduleRestart() throws Exception {
        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.connectorConfigReloadAction(MY_CONNECTOR)).andReturn(Herder.ConfigReloadAction.RESTART);
        EasyMock.expect(herder.restartConnector(1L, MY_CONNECTOR, null)).andReturn(requestId);

        EasyMock.expect(worker.herder()).andReturn(herder);
        EasyMock.expect(herder.connectorConfigReloadAction(MY_CONNECTOR)).andReturn(Herder.ConfigReloadAction.RESTART);
        EasyMock.expectLastCall();
        requestId.cancel();
        EasyMock.expectLastCall();
        EasyMock.expect(herder.restartConnector(10L, MY_CONNECTOR, null)).andReturn(requestId);

        replayAll();

        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithTTL}"));
        assertEquals(TEST_RESULT_WITH_TTL, result.get(MY_KEY));

        result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithLongerTTL}"));
        assertEquals(TEST_RESULT_WITH_LONGER_TTL, result.get(MY_KEY));
    }

    public static class TestConfigProvider implements ConfigProvider {

        public void configure(Map<String, ?> configs) {
        }

        public ConfigData get(String path) {
            return null;
        }

        public ConfigData get(String path, Set<String> keys) {
            if (path.equals(TEST_PATH)) {
                if (keys.contains(TEST_KEY)) {
                    return new ConfigData(Collections.singletonMap(TEST_KEY, TEST_RESULT));
                } else if (keys.contains(TEST_KEY_WITH_TTL)) {
                    return new ConfigData(Collections.singletonMap(TEST_KEY_WITH_TTL, TEST_RESULT_WITH_TTL), 1L);
                } else if (keys.contains(TEST_KEY_WITH_LONGER_TTL)) {
                    return new ConfigData(Collections.singletonMap(TEST_KEY_WITH_LONGER_TTL, TEST_RESULT_WITH_LONGER_TTL), 10L);
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
}

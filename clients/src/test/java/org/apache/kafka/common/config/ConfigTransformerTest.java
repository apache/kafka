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
package org.apache.kafka.common.config;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigTransformerTest {

    private ConfigTransformer configTransformer;

    @Before
    public void setup() {
        configTransformer = new ConfigTransformer(Collections.singletonMap("test", new TestConfigProvider()));
    }

    @Test
    public void testReplaceVariable() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap("myKey", "${test:testPath:testKey}"));
        Map<String, String> data = result.data();
        Map<String, Long> ttls = result.ttls();
        assertEquals("testResult", data.get("myKey"));
        assertTrue(ttls.isEmpty());
    }

    @Test
    public void testReplaceVariableWithTTL() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap("myKey", "${test:testPath:testKeyWithTTL}"));
        Map<String, String> data = result.data();
        Map<String, Long> ttls = result.ttls();
        assertEquals("testResultWithTTL", data.get("myKey"));
        assertEquals(1L, ttls.get("testPath").longValue());
    }

    @Test
    public void testReplaceMultipleVariablesInValue() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap("myKey", "hello, ${test:testPath:testKey}; goodbye, ${test:testPath:testKeyWithTTL}!!!"));
        Map<String, String> data = result.data();
        assertEquals("hello, testResult; goodbye, testResultWithTTL!!!", data.get("myKey"));
    }

    @Test
    public void testNoReplacement() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap("myKey", "${test:testPath:missingKey}"));
        Map<String, String> data = result.data();
        assertEquals("${test:testPath:missingKey}", data.get("myKey"));
    }

    @Test
    public void testSingleLevelOfIndirection() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap("myKey", "${test:testPath:testIndirection}"));
        Map<String, String> data = result.data();
        assertEquals("${test:testPath:testResult}", data.get("myKey"));
    }

    public static class TestConfigProvider implements ConfigProvider {

        public void configure(Map<String, ?> configs) {
        }

        public ConfigData get(String path) {
            return null;
        }

        public ConfigData get(String path, Set<String> keys) {
            Map<String, String> data = new HashMap<>();
            long ttl = Long.MAX_VALUE;
            if (path.equals("testPath")) {
                if (keys.contains("testKey")) {
                    data.put("testKey", "testResult");
                }
                if (keys.contains("testKeyWithTTL")) {
                    data.put("testKeyWithTTL", "testResultWithTTL");
                    ttl = 1L;
                }
                if (keys.contains("testIndirection")) {
                    data.put("testIndirection", "${test:testPath:testResult}");
                }
            }
            return new ConfigData(data, ttl);
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

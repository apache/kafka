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

    public static final String MY_KEY = "myKey";
    public static final String TEST_INDIRECTION = "testIndirection";
    public static final String TEST_KEY = "testKey";
    public static final String TEST_KEY_WITH_TTL = "testKeyWithTTL";
    public static final String TEST_PATH = "testPath";
    public static final String TEST_RESULT = "testResult";
    public static final String TEST_RESULT_WITH_TTL = "testResultWithTTL";

    private ConfigTransformer configTransformer;

    @Before
    public void setup() {
        configTransformer = new ConfigTransformer(Collections.singletonMap("test", new TestConfigProvider()));
    }

    @Test
    public void testReplaceVariable() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap(MY_KEY, "${test:testPath:testKey}"));
        Map<String, String> data = result.data();
        Map<String, Long> ttls = result.ttls();
        assertEquals(TEST_RESULT, data.get(MY_KEY));
        assertTrue(ttls.isEmpty());
    }

    @Test
    public void testReplaceVariableWithTTL() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithTTL}"));
        Map<String, String> data = result.data();
        Map<String, Long> ttls = result.ttls();
        assertEquals(TEST_RESULT_WITH_TTL, data.get(MY_KEY));
        assertEquals(1L, ttls.get(TEST_PATH).longValue());
    }

    @Test
    public void testReplaceMultipleVariablesInValue() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap(MY_KEY, "hello, ${test:testPath:testKey}; goodbye, ${test:testPath:testKeyWithTTL}!!!"));
        Map<String, String> data = result.data();
        assertEquals("hello, testResult; goodbye, testResultWithTTL!!!", data.get(MY_KEY));
    }

    @Test
    public void testNoReplacement() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap(MY_KEY, "${test:testPath:missingKey}"));
        Map<String, String> data = result.data();
        assertEquals("${test:testPath:missingKey}", data.get(MY_KEY));
    }

    @Test
    public void testSingleLevelOfIndirection() throws Exception {
        ConfigTransformerResult result = configTransformer.transform(Collections.singletonMap(MY_KEY, "${test:testPath:testIndirection}"));
        Map<String, String> data = result.data();
        assertEquals("${test:testPath:testResult}", data.get(MY_KEY));
    }

    public static class TestConfigProvider implements ConfigProvider {

        public void configure(Map<String, ?> configs) {
        }

        public ConfigData get(String path) {
            return null;
        }

        public ConfigData get(String path, Set<String> keys) {
            Map<String, String> data = new HashMap<>();
            Long ttl = null;
            if (path.equals(TEST_PATH)) {
                if (keys.contains(TEST_KEY)) {
                    data.put(TEST_KEY, TEST_RESULT);
                }
                if (keys.contains(TEST_KEY_WITH_TTL)) {
                    data.put(TEST_KEY_WITH_TTL, TEST_RESULT_WITH_TTL);
                    ttl = 1L;
                }
                if (keys.contains(TEST_INDIRECTION)) {
                    data.put(TEST_INDIRECTION, "${test:testPath:testResult}");
                }
            }
            return new ConfigData(data, ttl);
        }

        public void close() {
        }
    }

}

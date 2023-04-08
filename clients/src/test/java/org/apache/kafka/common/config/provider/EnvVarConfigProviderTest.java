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
package org.apache.kafka.common.config.provider;

import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;

import static org.apache.kafka.common.config.provider.EnvVarConfigProvider.ALLOWLIST_PATTERN_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EnvVarConfigProviderTest {

    private EnvVarConfigProvider envVarConfigProvider = null;

    @BeforeEach
    public void setup() {
        Map<String, String> testEnvVars = new HashMap<String, String>() {
            {
                put("test_var1", "value1");
                put("secret_var2", "value2");
                put("new_var3", "value3");
                put("not_so_secret_var4", "value4");
            }
        };
        envVarConfigProvider = new EnvVarConfigProvider(testEnvVars);
        envVarConfigProvider.configure(Collections.singletonMap("", ""));
    }

    @Test
    void testGetAllEnvVarsNotEmpty() {
        ConfigData properties = envVarConfigProvider.get("");
        assertNotEquals(0, properties.data().size());
    }

    @Test
    void testGetMultipleKeysAndCompare() {
        ConfigData properties = envVarConfigProvider.get("");
        assertNotEquals(0, properties.data().size());
        assertEquals("value1", properties.data().get("test_var1"));
        assertEquals("value2", properties.data().get("secret_var2"));
        assertEquals("value3", properties.data().get("new_var3"));
        assertEquals("value4", properties.data().get("not_so_secret_var4"));
    }

    @Test
    public void testGetOneKeyWithNullPath() {
        ConfigData config = envVarConfigProvider.get(null, Collections.singleton("secret_var2"));
        Map<String, String> data = config.data();

        assertEquals(1, data.size());
        assertEquals("value2", data.get("secret_var2"));
    }

    @Test
    public void testGetOneKeyWithEmptyPath() {
        ConfigData config = envVarConfigProvider.get("", Collections.singleton("test_var1"));
        Map<String, String> data = config.data();

        assertEquals(1, data.size());
        assertEquals("value1", data.get("test_var1"));
    }

    @Test
    void testGetEnvVarsByKeyList() {
        Set<String> keyList = new HashSet<>(Arrays.asList("test_var1", "secret_var2"));
        Set<String> keys = envVarConfigProvider.get(null, keyList).data().keySet();
        assertEquals(keyList, keys);
    }

    @Test
    void testNotNullPathNonEmptyThrowsException() {
        assertThrows(ConfigException.class, () -> envVarConfigProvider.get("test-path", Collections.singleton("test_var1")));
    }

    @Test void testRegExpEnvVarsSingleEntryKeyList() {
        Map<String, String> testConfigMap = Collections.singletonMap(ALLOWLIST_PATTERN_CONFIG, "secret_.*");
        envVarConfigProvider.configure(testConfigMap);
        Set<String> keyList = Collections.singleton("secret_var2");
        Set<String> keys = envVarConfigProvider.get(null, Collections.singleton("secret_var2")).data().keySet();

        assertEquals(keyList, keys);
    }

    @Test void testRegExpEnvVarsNoKeyList() {
        Map<String, String> testConfigMap = Collections.singletonMap(ALLOWLIST_PATTERN_CONFIG, "secret_.*");
        envVarConfigProvider.configure(testConfigMap);
        Set<String> keys = envVarConfigProvider.get("").data().keySet();

        assertEquals(Collections.singleton("secret_var2"), keys);
    }

}

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

import static org.junit.jupiter.api.Assertions.*;

class EnvVarConfigProviderTest {

    private EnvVarConfigProvider envVarConfigProvider = null;
    @BeforeEach
    public void setup() {
        Map<String, String> testEnvVars = new HashMap<String, String>() {
            {
                put("var1", "value1");
                put("var2", "value2");
                put("var3", "value3");
            }
        };
        envVarConfigProvider = new EnvVarConfigProvider(testEnvVars);
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
        assertEquals("value1", properties.data().get("var1"));
        assertEquals("value2", properties.data().get("var2"));
        assertEquals("value3", properties.data().get("var3"));
    }

    @Test
    public void testGetOneKeyWithNullPath() {
        ConfigData config = envVarConfigProvider.get(null, Collections.singleton("var2"));
        Map<String, String> data = config.data();

        assertEquals(1, data.size());
        assertEquals("value2", data.get("var2"));
    }

    @Test
    public void testGetOneKeyWithEmptyPath() {
        ConfigData config = envVarConfigProvider.get("", Collections.singleton("var1"));
        Map<String, String> data = config.data();

        assertEquals(1, data.size());
        assertEquals("value1", data.get("var1"));
    }

    @Test
    void testGetWhitelistedEnvVars() {
        Set<String> whiteList = new HashSet<>(Arrays.asList("var1", "var2"));
        Set<String> keys = envVarConfigProvider.get(null, whiteList).data().keySet();
        assertEquals(whiteList, keys);
    }
    @Test
    void testNotNullPathNonEmptyThrowsException() {
        assertThrows(ConfigException.class, () -> {
            envVarConfigProvider.get( "test-path", Collections.singleton("var1"));
        });
    }

}
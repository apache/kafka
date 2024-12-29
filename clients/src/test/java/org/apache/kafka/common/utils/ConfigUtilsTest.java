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

package org.apache.kafka.common.utils;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ConfigUtilsTest {

    private static final ConfigDef CONFIG = new ConfigDef().
        define("myPassword", Type.PASSWORD, Importance.HIGH, "").
        define("myString", Type.STRING, Importance.HIGH, "").
        define("myInt", Type.INT, Importance.HIGH, "").
        define("myString2", Type.STRING, Importance.HIGH, "");

    @Test
    public void testConfigMapToRedactedStringForEmptyMap() {
        assertEquals("{}", ConfigUtils.
            configMapToRedactedString(Collections.emptyMap(), CONFIG));
    }

    @Test
    public void testConfigMapToRedactedStringWithSecrets() {
        Map<String, Object> testMap1 = new HashMap<>();
        testMap1.put("myString", "whatever");
        testMap1.put("myInt", 123);
        testMap1.put("myPassword", "foosecret");
        testMap1.put("myString2", null);
        testMap1.put("myUnknown", 456);
        assertEquals("{myInt=123, myPassword=(redacted), myString=\"whatever\", myString2=null, myUnknown=(redacted)}",
            ConfigUtils.configMapToRedactedString(testMap1, CONFIG));
    }

    @Test
    public void testGetBoolean() {
        String key = "test.key";
        boolean defaultValue = true;

        Map<String, Object> config = new HashMap<>();
        config.put("some.other.key", false);
        assertEquals(defaultValue, ConfigUtils.getBoolean(config, key, defaultValue));

        config = new HashMap<>();
        config.put(key, false);
        assertFalse(ConfigUtils.getBoolean(config, key, defaultValue));

        config = new HashMap<>();
        config.put(key, "false");
        assertFalse(ConfigUtils.getBoolean(config, key, defaultValue));

        config = new HashMap<>();
        config.put(key, "not-a-boolean");
        assertFalse(ConfigUtils.getBoolean(config, key, defaultValue));

        config = new HashMap<>();
        config.put(key, 5);
        assertEquals(defaultValue, ConfigUtils.getBoolean(config, key, defaultValue));
    }
}

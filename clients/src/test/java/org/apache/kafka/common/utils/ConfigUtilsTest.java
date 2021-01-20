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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConfigUtilsTest {

    @Test
    public void testTranslateDeprecated() {
        Map<String, Object> config = new HashMap<>();
        config.put("foo.bar", "baz");
        config.put("foo.bar.deprecated", "quux");
        config.put("chicken", "1");
        config.put("rooster", "2");
        config.put("hen", "3");
        config.put("heifer", "moo");
        config.put("blah", "blah");
        config.put("unexpected.non.string.object", 42);
        Map<String, Object> newConfig = ConfigUtils.translateDeprecatedConfigs(config, new String[][]{
            {"foo.bar", "foo.bar.deprecated"},
            {"chicken", "rooster", "hen"},
            {"cow", "beef", "heifer", "steer"}
        });
        assertEquals("baz", newConfig.get("foo.bar"));
        assertNull(newConfig.get("foobar.deprecated"));
        assertEquals("1", newConfig.get("chicken"));
        assertNull(newConfig.get("rooster"));
        assertNull(newConfig.get("hen"));
        assertEquals("moo", newConfig.get("cow"));
        assertNull(newConfig.get("beef"));
        assertNull(newConfig.get("heifer"));
        assertNull(newConfig.get("steer"));
        assertNull(config.get("cow"));
        assertEquals("blah", config.get("blah"));
        assertEquals("blah", newConfig.get("blah"));
        assertEquals(42, newConfig.get("unexpected.non.string.object"));
        assertEquals(42, config.get("unexpected.non.string.object"));

    }

    @Test
    public void testAllowsNewKey() {
        Map<String, String> config = new HashMap<>();
        config.put("foo.bar", "baz");
        Map<String, String> newConfig = ConfigUtils.translateDeprecatedConfigs(config, new String[][]{
            {"foo.bar", "foo.bar.deprecated"},
            {"chicken", "rooster", "hen"},
            {"cow", "beef", "heifer", "steer"}
        });
        assertNotNull(newConfig);
        assertEquals("baz", newConfig.get("foo.bar"));
        assertNull(newConfig.get("foo.bar.deprecated"));
    }

    @Test
    public void testAllowDeprecatedNulls() {
        Map<String, String> config = new HashMap<>();
        config.put("foo.bar.deprecated", null);
        config.put("foo.bar", "baz");
        Map<String, String> newConfig = ConfigUtils.translateDeprecatedConfigs(config, new String[][]{
            {"foo.bar", "foo.bar.deprecated"}
        });
        assertNotNull(newConfig);
        assertEquals("baz", newConfig.get("foo.bar"));
        assertNull(newConfig.get("foo.bar.deprecated"));
    }

    @Test
    public void testAllowNullOverride() {
        Map<String, String> config = new HashMap<>();
        config.put("foo.bar.deprecated", "baz");
        config.put("foo.bar", null);
        Map<String, String> newConfig = ConfigUtils.translateDeprecatedConfigs(config, new String[][]{
            {"foo.bar", "foo.bar.deprecated"}
        });
        assertNotNull(newConfig);
        assertNull(newConfig.get("foo.bar"));
        assertNull(newConfig.get("foo.bar.deprecated"));
    }

    @Test
    public void testNullMapEntriesWithoutAliasesDoNotThrowNPE() {
        Map<String, String> config = new HashMap<>();
        config.put("other", null);
        Map<String, String> newConfig = ConfigUtils.translateDeprecatedConfigs(config, new String[][]{
            {"foo.bar", "foo.bar.deprecated"}
        });
        assertNotNull(newConfig);
        assertNull(newConfig.get("other"));
    }

    @Test
    public void testDuplicateSynonyms() {
        Map<String, String> config = new HashMap<>();
        config.put("foo.bar", "baz");
        config.put("foo.bar.deprecated", "derp");
        Map<String, String> newConfig = ConfigUtils.translateDeprecatedConfigs(config, new String[][]{
            {"foo.bar", "foo.bar.deprecated"},
            {"chicken", "foo.bar.deprecated"}
        });
        assertNotNull(newConfig);
        assertEquals("baz", newConfig.get("foo.bar"));
        assertEquals("derp", newConfig.get("chicken"));
        assertNull(newConfig.get("foo.bar.deprecated"));
    }

    @Test
    public void testMultipleDeprecations() {
        Map<String, String> config = new HashMap<>();
        config.put("foo.bar.deprecated", "derp");
        config.put("foo.bar.even.more.deprecated", "very old configuration");
        Map<String, String> newConfig = ConfigUtils.translateDeprecatedConfigs(config, new String[][]{
            {"foo.bar", "foo.bar.deprecated", "foo.bar.even.more.deprecated"}
        });
        assertNotNull(newConfig);
        assertEquals("derp", newConfig.get("foo.bar"));
        assertNull(newConfig.get("foo.bar.deprecated"));
        assertNull(newConfig.get("foo.bar.even.more.deprecated"));
    }
}

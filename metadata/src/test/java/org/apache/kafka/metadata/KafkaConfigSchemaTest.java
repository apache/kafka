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

package org.apache.kafka.metadata;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class KafkaConfigSchemaTest {
    public static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("foo.bar", ConfigDef.Type.LIST, "1", ConfigDef.Importance.HIGH, "foo bar").
            define("baz", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
            define("quux", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "quux").
            define("quuux", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "quuux"));
        CONFIGS.put(TOPIC, new ConfigDef().
            define("abc", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "abc").
            define("def", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "def").
            define("ghi", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, "ghi").
            define("xyz", ConfigDef.Type.PASSWORD, "thedefault", ConfigDef.Importance.HIGH, "xyz"));
    }

    @Test
    public void testIsSplittable() {
        KafkaConfigSchema schema = new KafkaConfigSchema(CONFIGS);
        assertTrue(schema.isSplittable(BROKER, "foo.bar"));
        assertFalse(schema.isSplittable(BROKER, "baz"));
        assertFalse(schema.isSplittable(BROKER, "foo.baz.quux"));
        assertFalse(schema.isSplittable(TOPIC, "baz"));
        assertTrue(schema.isSplittable(TOPIC, "abc"));
    }

    @Test
    public void testGetConfigValueDefault() {
        KafkaConfigSchema schema = new KafkaConfigSchema(CONFIGS);
        assertEquals("1", schema.getDefault(BROKER, "foo.bar"));
        assertEquals(null, schema.getDefault(BROKER, "foo.baz.quux"));
        assertEquals(null, schema.getDefault(TOPIC, "abc"));
        assertEquals("true", schema.getDefault(TOPIC, "ghi"));
    }

    @Test
    public void testIsSensitive() {
        KafkaConfigSchema schema = new KafkaConfigSchema(CONFIGS);
        assertFalse(schema.isSensitive(BROKER, "foo.bar"));
        assertTrue(schema.isSensitive(BROKER, "quuux"));
        assertTrue(schema.isSensitive(BROKER, "unknown.config.key"));
        assertFalse(schema.isSensitive(TOPIC, "abc"));
    }
}

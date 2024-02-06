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

package org.apache.kafka.image.node.printer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class MetadataNodeRedactionCriteriaTest {
    private static final MetadataNodeRedactionCriteria.Strict STRICT;

    private static final MetadataNodeRedactionCriteria.Normal NORMAL;

    private static final MetadataNodeRedactionCriteria.Disabled DISABLED;

    static {
        Map<ConfigResource.Type, ConfigDef> configs = new HashMap<>();
        configs.put(BROKER, new ConfigDef().
                define("non.secret", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
                define("secret.config", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "baz"));
        configs.put(TOPIC, new ConfigDef().
                define("topic.secret.config", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "abc"));
        KafkaConfigSchema schema = new KafkaConfigSchema(configs, Collections.emptyMap());
        STRICT = MetadataNodeRedactionCriteria.Strict.INSTANCE;
        NORMAL = new MetadataNodeRedactionCriteria.Normal(schema);
        DISABLED = MetadataNodeRedactionCriteria.Disabled.INSTANCE;
    }

    @Test
    public void testStrictRedactsScram() {
        assertTrue(STRICT.shouldRedactScram());
    }

    @Test
    public void testNormalRedactsScram() {
        assertTrue(NORMAL.shouldRedactScram());
    }

    @Test
    public void testDisabledDoesNotRedactScram() {
        assertFalse(DISABLED.shouldRedactScram());
    }

    @Test
    public void testStrictRedactsDelegationToken() {
        assertTrue(STRICT.shouldRedactDelegationToken());
    }

    @Test
    public void testNormalRedactsDelegationToken() {
        assertTrue(NORMAL.shouldRedactDelegationToken());
    }

    @Test
    public void testDisabledDoesNotRedactDelegationToken() {
        assertFalse(DISABLED.shouldRedactDelegationToken());
    }

    @Test
    public void testStrictRedactsNonSensitiveConfig() {
        assertTrue(STRICT.shouldRedactConfig(BROKER, "non.secret"));
    }

    @Test
    public void testNormalDoesNotRedactNonSensitiveConfig() {
        assertFalse(NORMAL.shouldRedactConfig(BROKER, "non.secret"));
    }

    @Test
    public void testDisabledDoesNotRedactNonSensitiveConfig() {
        assertFalse(DISABLED.shouldRedactConfig(BROKER, "non.secret"));
    }

    @Test
    public void testStrictRedactsSensitiveConfig() {
        assertTrue(STRICT.shouldRedactConfig(BROKER, "secret.config"));
    }

    @Test
    public void testNormalDoesRedactsSensitiveConfig() {
        assertTrue(NORMAL.shouldRedactConfig(BROKER, "secret.config"));
    }

    @Test
    public void testDisabledDoesNotRedactSensitiveConfig() {
        assertFalse(DISABLED.shouldRedactConfig(BROKER, "secret.config"));
    }

    @Test
    public void testStrictRedactsUnknownConfig() {
        assertTrue(STRICT.shouldRedactConfig(BROKER, "unknown.config"));
    }

    @Test
    public void testNormalDoesRedactsUnknownConfig() {
        assertTrue(NORMAL.shouldRedactConfig(BROKER, "unknown.config"));
    }

    @Test
    public void testDisabledDoesNotRedactUnknownConfig() {
        assertFalse(DISABLED.shouldRedactConfig(BROKER, "unknown.config"));
    }
}

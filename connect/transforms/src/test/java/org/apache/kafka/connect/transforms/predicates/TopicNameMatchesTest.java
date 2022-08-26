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
package org.apache.kafka.connect.transforms.predicates;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicNameMatchesTest {

    @Test
    public void testPatternRequiredInConfig() {
        Map<String, String> props = new HashMap<>();
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("Missing required configuration \"pattern\""));
    }

    @Test
    public void testPatternMayNotBeEmptyInConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("pattern", "");
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("String must be non-empty"));
    }

    @Test
    public void testPatternIsValidRegexInConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("pattern", "[");
        ConfigException e = assertThrows(ConfigException.class, () -> config(props));
        assertTrue(e.getMessage().contains("Invalid regex"));
    }

    @Test
    public void testConfig() {
        TopicNameMatches<SourceRecord> predicate = new TopicNameMatches<>();
        predicate.config().validate(Collections.singletonMap("pattern", "my-prefix-.*"));

        List<ConfigValue> configs = predicate.config().validate(Collections.singletonMap("pattern", "*"));
        List<String> errorMsgs = configs.get(0).errorMessages();
        assertEquals(1, errorMsgs.size());
        assertTrue(errorMsgs.get(0).contains("Invalid regex"));
    }

    @Test
    public void testTest() {
        TopicNameMatches<SourceRecord> predicate = new TopicNameMatches<>();
        predicate.configure(Collections.singletonMap("pattern", "my-prefix-.*"));

        assertTrue(predicate.test(recordWithTopicName("my-prefix-")));
        assertTrue(predicate.test(recordWithTopicName("my-prefix-foo")));
        assertFalse(predicate.test(recordWithTopicName("x-my-prefix-")));
        assertFalse(predicate.test(recordWithTopicName("x-my-prefix-foo")));
        assertFalse(predicate.test(recordWithTopicName("your-prefix-")));
        assertFalse(predicate.test(recordWithTopicName("your-prefix-foo")));
        assertFalse(predicate.test(new SourceRecord(null, null, null, null, null)));

    }

    private SimpleConfig config(Map<String, String> props) {
        return new SimpleConfig(TopicNameMatches.CONFIG_DEF, props);
    }

    private SourceRecord recordWithTopicName(String topicName) {
        return new SourceRecord(null, null, topicName, null, null);
    }
}

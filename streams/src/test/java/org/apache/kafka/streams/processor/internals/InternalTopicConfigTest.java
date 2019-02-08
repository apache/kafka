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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InternalTopicConfigTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfNameIsNull() {
        new RepartitionTopicConfig(null, Collections.<String, String>emptyMap());
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldThrowIfNameIsInvalid() {
        new RepartitionTopicConfig("foo bar baz", Collections.<String, String>emptyMap());
    }

    @Test
    public void shouldAugmentRetentionMsWithWindowedChangelog() {
        final WindowedChangelogTopicConfig topicConfig = new WindowedChangelogTopicConfig("name", Collections.<String, String>emptyMap());
        topicConfig.setRetentionMs(10);
        assertEquals("30", topicConfig.getProperties(Collections.<String, String>emptyMap(), 20).get(TopicConfig.RETENTION_MS_CONFIG));
    }

    @Test
    public void shouldUseSuppliedConfigs() {
        final Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", "1000");
        configs.put("retention.bytes", "10000");

        final UnwindowedChangelogTopicConfig topicConfig = new UnwindowedChangelogTopicConfig("name", configs);

        final Map<String, String> properties = topicConfig.getProperties(Collections.<String, String>emptyMap(), 0);
        assertEquals("1000", properties.get("retention.ms"));
        assertEquals("10000", properties.get("retention.bytes"));
    }

    @Test
    public void shouldUseSuppliedConfigsForRepartitionConfig() {
        final Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", "1000");
        final RepartitionTopicConfig topicConfig = new RepartitionTopicConfig("name", configs);
        assertEquals("1000", topicConfig.getProperties(Collections.<String, String>emptyMap(), 0).get(TopicConfig.RETENTION_MS_CONFIG));
    }
}
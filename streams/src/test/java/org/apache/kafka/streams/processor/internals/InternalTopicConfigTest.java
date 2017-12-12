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
import org.apache.kafka.streams.processor.internals.InternalTopicConfig.InternalTopicType;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InternalTopicConfigTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfNameIsNull() {
        new InternalTopicConfig(null, InternalTopicType.REPARTITION, Collections.<String, String>emptyMap());
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldThrowIfNameIsInvalid() {
        new InternalTopicConfig("foo bar baz", InternalTopicType.REPARTITION, Collections.<String, String>emptyMap());
    }

    @Test
    public void shouldAugmentRetentionMsWithWindowedChangelog() {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                        InternalTopicType.WINDOWED_STORE_CHANGELOG,
                                                                        Collections.<String, String>emptyMap());
        topicConfig.setRetentionMs(10);
        assertEquals("30", topicConfig.toProperties(20).get(TopicConfig.RETENTION_MS_CONFIG));
    }

    @Test
    public void shouldThrowWhenSetRetentionMsWithUnWindowedChangelog() {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                        InternalTopicType.UNWINDOWED_STORE_CHANGELOG,
                                                                        Collections.<String, String>emptyMap());
        try {
            topicConfig.setRetentionMs(10);

            fail("We should have thrown the IllegalStateException.");
        } catch (final IllegalStateException e) {
            // this is good
        }
    }

    @Test
    public void shouldThrowWhenSetRetentionMsWithRepartition() {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                        InternalTopicType.REPARTITION,
                                                                        Collections.<String, String>emptyMap());
        try {
            topicConfig.setRetentionMs(10);

            fail("We should have thrown the IllegalStateException.");
        } catch (final IllegalStateException e) {
            // this is good
        }
    }


    @Test
    public void shouldUseSuppliedConfigs() {
        final Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", "1000");
        configs.put("retention.bytes", "10000");

        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                        InternalTopicType.UNWINDOWED_STORE_CHANGELOG,
                                                                        configs);

        final Map<String, String> properties = topicConfig.toProperties(0);
        assertEquals("1000", properties.get("retention.ms"));
        assertEquals("10000", properties.get("retention.bytes"));
    }
}
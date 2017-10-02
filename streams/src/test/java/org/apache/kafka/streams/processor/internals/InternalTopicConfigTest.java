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

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InternalTopicConfigTest {

    @Test
    public void shouldHaveCompactionPropSetIfSupplied() {
        final Properties properties = new InternalTopicConfig("name",
                                                              Collections.singleton(InternalTopicConfig.CleanupPolicy.compact),
                                                              Collections.<String, String>emptyMap()).toProperties(0);
        assertEquals("compact", properties.getProperty(InternalTopicManager.CLEANUP_POLICY_PROP));
    }


    @Test(expected = NullPointerException.class)
    public void shouldThrowIfNameIsNull() {
        new InternalTopicConfig(null, Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), Collections.<String, String>emptyMap());
    }

    @Test(expected = InvalidTopicException.class)
    public void shouldThrowIfNameIsInvalid() {
        new InternalTopicConfig("foo bar baz", Collections.singleton(InternalTopicConfig.CleanupPolicy.compact), Collections.<String, String>emptyMap());
    }

    @Test
    public void shouldConfigureRetentionMsWithAdditionalRetentionWhenCompactAndDelete() {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                        Utils.mkSet(InternalTopicConfig.CleanupPolicy.compact, InternalTopicConfig.CleanupPolicy.delete),
                                                                        Collections.<String, String>emptyMap());
        final int additionalRetentionMs = 20;
        topicConfig.setRetentionMs(10);
        final Properties properties = topicConfig.toProperties(additionalRetentionMs);
        assertEquals("30", properties.getProperty(InternalTopicManager.RETENTION_MS));
    }

    @Test
    public void shouldNotConfigureRetentionMsWhenCompact() {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                        Collections.singleton(InternalTopicConfig.CleanupPolicy.compact),
                                                                        Collections.<String, String>emptyMap());
        topicConfig.setRetentionMs(10);
        final Properties properties = topicConfig.toProperties(0);
        assertNull(null, properties.getProperty(InternalTopicManager.RETENTION_MS));
    }

    @Test
    public void shouldNotConfigureRetentionMsWhenDelete() {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                        Collections.singleton(InternalTopicConfig.CleanupPolicy.delete),
                                                                        Collections.<String, String>emptyMap());
        topicConfig.setRetentionMs(10);
        final Properties properties = topicConfig.toProperties(0);
        assertNull(null, properties.getProperty(InternalTopicManager.RETENTION_MS));
    }


    @Test
    public void shouldBeCompactedIfCleanupPolicyCompactOrCompactAndDelete() {
        assertTrue(new InternalTopicConfig("name",
                                           Collections.singleton(InternalTopicConfig.CleanupPolicy.compact),
                                           Collections.<String, String>emptyMap()).isCompacted());
        assertTrue(new InternalTopicConfig("name", Utils.mkSet(InternalTopicConfig.CleanupPolicy.compact,
                                                               InternalTopicConfig.CleanupPolicy.delete),
                                           Collections.<String, String>emptyMap()).isCompacted());
    }

    @Test
    public void shouldNotBeCompactedWhenCleanupPolicyIsDelete() {
        assertFalse(new InternalTopicConfig("name",
                                            Collections.singleton(InternalTopicConfig.CleanupPolicy.delete),
                                            Collections.<String, String>emptyMap()).isCompacted());
    }

    @Test
    public void shouldUseCleanupPolicyFromConfigIfSupplied() {
        final InternalTopicConfig config = new InternalTopicConfig("name",
                                                                   Collections.singleton(InternalTopicConfig.CleanupPolicy.delete),
                                                                   Collections.singletonMap("cleanup.policy", "compact"));

        final Properties properties = config.toProperties(0);
        assertEquals("compact", properties.getProperty("cleanup.policy"));
    }

    @Test
    public void shouldHavePropertiesSuppliedByUser() {
        final Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", "1000");
        configs.put("retention.bytes", "10000");

        final InternalTopicConfig topicConfig = new InternalTopicConfig("name",
                                                                 Collections.singleton(InternalTopicConfig.CleanupPolicy.delete),
                                                                 configs);

        final Properties properties = topicConfig.toProperties(0);
        assertEquals("1000", properties.getProperty("retention.ms"));
        assertEquals("10000", properties.getProperty("retention.bytes"));
    }
}
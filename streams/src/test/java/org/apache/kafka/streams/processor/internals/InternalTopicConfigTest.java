/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InternalTopicConfigTest {

    @Test
    public void shouldHaveNoPropertiesWhenNothingConfigure() throws Exception {
        final Properties props = new InternalTopicConfig("name").toProperties(1);
        assertEquals(0, props.size());
    }

    @Test
    public void shouldHaveCompactionPropSetIfSupplied() throws Exception {
        final Properties properties = new InternalTopicConfig("name", "compact").toProperties(1);
        assertEquals("compact", properties.getProperty(InternalTopicManager.CLEANUP_POLICY_PROP));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfInvalidCleanUpPolicy() throws Exception {
        new InternalTopicConfig("name", "blah");
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfNameIsNull() throws Exception {
        new InternalTopicConfig(null);
    }

    @Test
    public void shouldConfigureRetentionMsWithAdditionalRetentionWhenCompactAndDelete() throws Exception {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name", "compact,delete");
        topicConfig.setRetentionMs(10);
        final Properties properties = topicConfig.toProperties(20);
        assertEquals("30", properties.getProperty(InternalTopicManager.RETENTION_MS));
    }

    @Test
    public void shouldNotConfigureRetentionMsWhenCompact() throws Exception {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name", "compact");
        topicConfig.setRetentionMs(10);
        final Properties properties = topicConfig.toProperties(0);
        assertNull(null, properties.getProperty(InternalTopicManager.RETENTION_MS));
    }

    @Test
    public void shouldNotConfigureRetentionMsWhenDelete() throws Exception {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name", "delete");
        topicConfig.setRetentionMs(10);
        final Properties properties = topicConfig.toProperties(0);
        assertNull(null, properties.getProperty(InternalTopicManager.RETENTION_MS));
    }

    @Test
    public void shouldNotConfigureRetentionMsWhenNoPolicyConfigured() throws Exception {
        final InternalTopicConfig topicConfig = new InternalTopicConfig("name");
        topicConfig.setRetentionMs(10);
        final Properties properties = topicConfig.toProperties(0);
        assertNull(null, properties.getProperty(InternalTopicManager.RETENTION_MS));
    }

    @Test
    public void shouldBeCompactedIfCleanupPolicyCompactOrCompactAndDelete() throws Exception {
        assertTrue(new InternalTopicConfig("name", "compact").isCompacted());
        assertTrue(new InternalTopicConfig("name", "compact,delete").isCompacted());
    }

    @Test
    public void shouldNotBeCompactedWhenCleanupPolicyIsNullOrDelete() throws Exception {
        assertFalse(new InternalTopicConfig("name").isCompacted());
        assertFalse(new InternalTopicConfig("name", "delete").isCompacted());
    }
}
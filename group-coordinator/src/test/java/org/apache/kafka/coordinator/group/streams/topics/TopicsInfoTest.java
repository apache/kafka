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
package org.apache.kafka.coordinator.group.streams.topics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicsInfoTest {

    @Test
    public void testConstructorAndGetters() {
        Set<String> repartitionSinkTopics = mkSet("repartitionSinkTopic1", "repartitionSinkTopic2");
        Set<String> sourceTopics = mkSet("sourceTopic1", "sourceTopic2");
        Map<String, InternalTopicConfig> repartitionSourceTopics = new HashMap<>();
        Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
        Collection<Set<String>> copartitionGroups = new ArrayList<>();

        TopicsInfo topicsInfo = new TopicsInfo(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics, copartitionGroups);

        assertEquals(repartitionSinkTopics, topicsInfo.repartitionSinkTopics());
        assertEquals(sourceTopics, topicsInfo.sourceTopics());
        assertEquals(repartitionSourceTopics, topicsInfo.repartitionSourceTopics());
        assertEquals(stateChangelogTopics, topicsInfo.stateChangelogTopics());
        assertEquals(copartitionGroups, topicsInfo.copartitionGroups());
    }

    @Test
    public void testSetters() {
        TopicsInfo topicsInfo = new TopicsInfo();

        Set<String> repartitionSinkTopics = mkSet("repartitionSinkTopic1", "repartitionSinkTopic2");
        topicsInfo.setRepartitionSinkTopics(repartitionSinkTopics);
        assertEquals(repartitionSinkTopics, topicsInfo.repartitionSinkTopics());

        Set<String> sourceTopics = mkSet("sourceTopic1", "sourceTopic2");
        topicsInfo.setSourceTopics(sourceTopics);
        assertEquals(sourceTopics, topicsInfo.sourceTopics());

        Map<String, InternalTopicConfig> repartitionSourceTopics = new HashMap<>();
        topicsInfo.setRepartitionSourceTopics(repartitionSourceTopics);
        assertEquals(repartitionSourceTopics, topicsInfo.repartitionSourceTopics());

        Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
        topicsInfo.setStateChangelogTopics(stateChangelogTopics);
        assertEquals(stateChangelogTopics, topicsInfo.stateChangelogTopics());

        Collection<Set<String>> copartitionGroups = new ArrayList<>();
        topicsInfo.setCopartitionGroups(copartitionGroups);
        assertEquals(copartitionGroups, topicsInfo.copartitionGroups());
    }

    @Test
    public void testNonSourceChangelogTopics() {
        Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
        stateChangelogTopics.put("changelogTopic1", new InternalTopicConfig("changelogTopic1"));
        stateChangelogTopics.put("sourceTopic1", new InternalTopicConfig("sourceTopic1"));

        TopicsInfo topicsInfo = new TopicsInfo(
            Collections.emptySet(),
            Collections.singleton("sourceTopic1"),
            Collections.emptyMap(),
            stateChangelogTopics,
            Collections.emptyList()
        );

        Set<InternalTopicConfig> nonSourceChangelogTopics = topicsInfo.nonSourceChangelogTopics();
        assertEquals(1, nonSourceChangelogTopics.size());
        assertTrue(nonSourceChangelogTopics.contains(new InternalTopicConfig("changelogTopic1")));
    }

    @Test
    public void testEquals() {
        Set<String> repartitionSinkTopics = new HashSet<>(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2"));
        Set<String> sourceTopics = new HashSet<>(Arrays.asList("sourceTopic1", "sourceTopic2"));
        Map<String, InternalTopicConfig> repartitionSourceTopics = new HashMap<>();
        Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
        Collection<Set<String>> copartitionGroups = new ArrayList<>();

        TopicsInfo topicsInfo1 = new TopicsInfo(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics, copartitionGroups);
        TopicsInfo topicsInfo2 = new TopicsInfo(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics, copartitionGroups);

        assertEquals(topicsInfo1, topicsInfo2);
    }

    @Test
    public void testHashCode() {
        Set<String> repartitionSinkTopics = new HashSet<>(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2"));
        Set<String> sourceTopics = new HashSet<>(Arrays.asList("sourceTopic1", "sourceTopic2"));
        Map<String, InternalTopicConfig> repartitionSourceTopics = new HashMap<>();
        Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
        Collection<Set<String>> copartitionGroups = new ArrayList<>();

        TopicsInfo topicsInfo1 = new TopicsInfo(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics, copartitionGroups);
        TopicsInfo topicsInfo2 = new TopicsInfo(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics, copartitionGroups);

        assertEquals(topicsInfo1.hashCode(), topicsInfo2.hashCode());
    }

    @Test
    public void testToString() {
        Set<String> repartitionSinkTopics = new HashSet<>(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2"));
        Set<String> sourceTopics = new HashSet<>(Arrays.asList("sourceTopic1", "sourceTopic2"));
        Map<String, InternalTopicConfig> repartitionSourceTopics = new HashMap<>();
        Map<String, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
        Collection<Set<String>> copartitionGroups = new ArrayList<>();

        TopicsInfo topicsInfo = new TopicsInfo(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics, copartitionGroups);

        String expectedString = "TopicsInfo{" +
            "repartitionSinkTopics=" + repartitionSinkTopics +
            ", sourceTopics=" + sourceTopics +
            ", stateChangelogTopics=" + stateChangelogTopics +
            ", repartitionSourceTopics=" + repartitionSourceTopics +
            ", copartitionGroups=" + copartitionGroups +
            '}';

        assertEquals(expectedString, topicsInfo.toString());
    }
}
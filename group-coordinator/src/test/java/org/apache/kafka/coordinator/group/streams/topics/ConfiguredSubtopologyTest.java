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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfiguredSubtopologyTest {

    @Test
    public void testConstructorAndGetters() {
        Set<String> repartitionSinkTopics = Set.of("repartitionSinkTopic1", "repartitionSinkTopic2");
        Set<String> sourceTopics = Set.of("sourceTopic1", "sourceTopic2");
        Map<String, ConfiguredInternalTopic> repartitionSourceTopics = new HashMap<>();
        Map<String, ConfiguredInternalTopic> stateChangelogTopics = new HashMap<>();

        ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics);

        assertEquals(repartitionSinkTopics, configuredSubtopology.repartitionSinkTopics());
        assertEquals(sourceTopics, configuredSubtopology.sourceTopics());
        assertEquals(repartitionSourceTopics, configuredSubtopology.repartitionSourceTopics());
        assertEquals(stateChangelogTopics, configuredSubtopology.stateChangelogTopics());
    }

    @Test
    public void testSetters() {
        ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology();

        Set<String> repartitionSinkTopics = Set.of("repartitionSinkTopic1", "repartitionSinkTopic2");
        configuredSubtopology.setRepartitionSinkTopics(repartitionSinkTopics);
        assertEquals(repartitionSinkTopics, configuredSubtopology.repartitionSinkTopics());

        Set<String> sourceTopics = Set.of("sourceTopic1", "sourceTopic2");
        configuredSubtopology.setSourceTopics(sourceTopics);
        assertEquals(sourceTopics, configuredSubtopology.sourceTopics());

        Map<String, ConfiguredInternalTopic> repartitionSourceTopics = new HashMap<>();
        configuredSubtopology.setRepartitionSourceTopics(repartitionSourceTopics);
        assertEquals(repartitionSourceTopics, configuredSubtopology.repartitionSourceTopics());

        Map<String, ConfiguredInternalTopic> stateChangelogTopics = new HashMap<>();
        configuredSubtopology.setStateChangelogTopics(stateChangelogTopics);
        assertEquals(stateChangelogTopics, configuredSubtopology.stateChangelogTopics());
    }

    @Test
    public void testNonSourceChangelogTopics() {
        Map<String, ConfiguredInternalTopic> stateChangelogTopics = new HashMap<>();
        stateChangelogTopics.put("changelogTopic1", new ConfiguredInternalTopic("changelogTopic1"));
        stateChangelogTopics.put("sourceTopic1", new ConfiguredInternalTopic("sourceTopic1"));

        ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology(
            Collections.emptySet(),
            Collections.singleton("sourceTopic1"),
            Collections.emptyMap(),
            stateChangelogTopics
        );

        Set<ConfiguredInternalTopic> nonSourceChangelogTopics = configuredSubtopology.nonSourceChangelogTopics();
        assertEquals(1, nonSourceChangelogTopics.size());
        assertTrue(nonSourceChangelogTopics.contains(new ConfiguredInternalTopic("changelogTopic1")));
    }

    @Test
    public void testEquals() {
        Set<String> repartitionSinkTopics = new HashSet<>(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2"));
        Set<String> sourceTopics = new HashSet<>(Arrays.asList("sourceTopic1", "sourceTopic2"));
        Map<String, ConfiguredInternalTopic> repartitionSourceTopics = new HashMap<>();
        Map<String, ConfiguredInternalTopic> stateChangelogTopics = new HashMap<>();

        ConfiguredSubtopology configuredSubtopology1 = new ConfiguredSubtopology(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics);
        ConfiguredSubtopology configuredSubtopology2 = new ConfiguredSubtopology(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics);

        assertEquals(configuredSubtopology1, configuredSubtopology2);
    }

    @Test
    public void testHashCode() {
        Set<String> repartitionSinkTopics = new HashSet<>(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2"));
        Set<String> sourceTopics = new HashSet<>(Arrays.asList("sourceTopic1", "sourceTopic2"));
        Map<String, ConfiguredInternalTopic> repartitionSourceTopics = new HashMap<>();
        Map<String, ConfiguredInternalTopic> stateChangelogTopics = new HashMap<>();

        ConfiguredSubtopology configuredSubtopology1 = new ConfiguredSubtopology(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics);
        ConfiguredSubtopology configuredSubtopology2 = new ConfiguredSubtopology(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics);

        assertEquals(configuredSubtopology1.hashCode(), configuredSubtopology2.hashCode());
    }

    @Test
    public void testToString() {
        Set<String> repartitionSinkTopics = new HashSet<>(Arrays.asList("repartitionSinkTopic1", "repartitionSinkTopic2"));
        Set<String> sourceTopics = new HashSet<>(Arrays.asList("sourceTopic1", "sourceTopic2"));
        Map<String, ConfiguredInternalTopic> repartitionSourceTopics = new HashMap<>();
        Map<String, ConfiguredInternalTopic> stateChangelogTopics = new HashMap<>();

        ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology(repartitionSinkTopics, sourceTopics, repartitionSourceTopics, stateChangelogTopics);

        String expectedString = "ConfiguredSubtopology{" +
            "repartitionSinkTopics=" + repartitionSinkTopics +
            ", sourceTopics=" + sourceTopics +
            ", stateChangelogTopics=" + stateChangelogTopics +
            ", repartitionSourceTopics=" + repartitionSourceTopics +
            '}';

        assertEquals(expectedString, configuredSubtopology.toString());
    }
}
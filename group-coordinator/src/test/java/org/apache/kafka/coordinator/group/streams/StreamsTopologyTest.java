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
package org.apache.kafka.coordinator.group.streams;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.topics.InternalTopicConfig;
import org.apache.kafka.coordinator.group.streams.topics.TopicsInfo;
import org.junit.jupiter.api.Test;

public class StreamsTopologyTest {

    @Test
    public void streamsTopologyIdShouldBeCorrect() {
        StreamsTopology topology = new StreamsTopology("topology-id", Collections.emptyMap());
        assertEquals("topology-id", topology.topologyId());
    }

    @Test
    public void subtopologiesShouldBeCorrect() {
        Map<String, TopicsInfo> subtopologies = mkMap(
            mkEntry("subtopology-1", new TopicsInfo()),
            mkEntry("subtopology-2", new TopicsInfo())
        );
        StreamsTopology topology = new StreamsTopology("topology-id", subtopologies);
        assertEquals(subtopologies, topology.subtopologies());
    }

    @Test
    public void topicSubscriptionShouldBeCorrect() {
        Map<String, TopicsInfo> subtopologies = mkMap(
            mkEntry("subtopology-1", new TopicsInfo()
                .setSourceTopics(Arrays.asList("source-topic-1", "source-topic-2"))
                .setRepartitionSourceTopics(mkMap(
                    mkEntry("repartition-topic-1", new InternalTopicConfig("repartition-topic-1")),
                    mkEntry("repartition-topic-2", new InternalTopicConfig("repartition-topic-2"))
                ))
            ),
            mkEntry("subtopology-2", new TopicsInfo()
                .setSourceTopics(Arrays.asList("source-topic-3", "source-topic-4"))
                .setRepartitionSourceTopics(mkMap(
                    mkEntry("repartition-topic-3", new InternalTopicConfig("repartition-topic-3")),
                    mkEntry("repartition-topic-4", new InternalTopicConfig("repartition-topic-4"))
                ))
            )
        );
        StreamsTopology topology = new StreamsTopology("topology-id", subtopologies);
        Set<String> expectedTopics = new HashSet<>(Arrays.asList(
            "source-topic-1", "source-topic-2", "repartition-topic-1", "repartition-topic-2",
            "source-topic-3", "source-topic-4", "repartition-topic-3", "repartition-topic-4"
        ));
        assertEquals(expectedTopics, topology.topicSubscription());
    }

    @Test
    public void fromRecordShouldCreateCorrectTopology() {
        StreamsGroupTopologyValue record = new StreamsGroupTopologyValue()
            .setTopologyId("topology-id")
            .setTopology(Arrays.asList(
                new StreamsGroupTopologyValue.Subtopology().setSubtopologyId("subtopology-1"),
                new StreamsGroupTopologyValue.Subtopology().setSubtopologyId("subtopology-2")
            ));
        StreamsTopology topology = StreamsTopology.fromRecord(record);
        assertEquals("topology-id", topology.topologyId());
        assertEquals(2, topology.subtopologies().size());
        assertTrue(topology.subtopologies().containsKey("subtopology-1"));
        assertTrue(topology.subtopologies().containsKey("subtopology-2"));
    }

    @Test
    public void equalsShouldReturnTrueForEqualTopologies() {
        Map<String, TopicsInfo> subtopologies = mkMap(
            mkEntry("subtopology-1", new TopicsInfo()),
            mkEntry("subtopology-2", new TopicsInfo())
        );
        StreamsTopology topology1 = new StreamsTopology("topology-id", subtopologies);
        StreamsTopology topology2 = new StreamsTopology("topology-id", subtopologies);
        assertEquals(topology1, topology2);
    }

    @Test
    public void equalsShouldReturnFalseForDifferentTopologies() {
        Map<String, TopicsInfo> subtopologies1 = mkMap(
            mkEntry("subtopology-1", new TopicsInfo())
        );
        Map<String, TopicsInfo> subtopologies2 = mkMap(
            mkEntry("subtopology-2", new TopicsInfo())
        );
        StreamsTopology topology1 = new StreamsTopology("topology-id-1", subtopologies1);
        StreamsTopology topology2 = new StreamsTopology("topology-id-2", subtopologies2);
        assertNotEquals(topology1, topology2);
    }

    @Test
    public void hashCodeShouldBeConsistentWithEquals() {
        Map<String, TopicsInfo> subtopologies = mkMap(
            mkEntry("subtopology-1", new TopicsInfo()),
            mkEntry("subtopology-2", new TopicsInfo())
        );
        StreamsTopology topology1 = new StreamsTopology("topology-id", subtopologies);
        StreamsTopology topology2 = new StreamsTopology("topology-id", subtopologies);
        assertEquals(topology1.hashCode(), topology2.hashCode());
    }

    @Test
    public void toStringShouldReturnCorrectRepresentation() {
        Map<String, TopicsInfo> subtopologies = mkMap(
            mkEntry("subtopology-1", new TopicsInfo()),
            mkEntry("subtopology-2", new TopicsInfo())
        );
        StreamsTopology topology = new StreamsTopology("topology-id", subtopologies);
        String expectedString = "StreamsTopology{topologyId=topology-id, subtopologies=" + subtopologies + "}";
        assertEquals(expectedString, topology.toString());
    }

    @Test
    public void toStreamsGroupDescribeTopologyShouldReturnCorrectSubtopologies() {
        Map<String, TopicsInfo> subtopologies = mkMap(
            mkEntry("subtopology-1", new TopicsInfo()
                .setSourceTopics(Collections.singletonList("source-topic-1"))
                .setRepartitionSinkTopics(Collections.singletonList("sink-topic-1"))
                .setRepartitionSourceTopics(
                    Collections.singletonMap("repartition-topic-1", new InternalTopicConfig("repartition-topic-1")))
                .setStateChangelogTopics(
                    Collections.singletonMap("changelog-topic-1", new InternalTopicConfig("changelog-topic-1")))
            ),
            mkEntry("subtopology-2", new TopicsInfo()
                .setSourceTopics(Collections.singletonList("source-topic-2"))
                .setRepartitionSinkTopics(Collections.singletonList("sink-topic-2"))
                .setRepartitionSourceTopics(
                    Collections.singletonMap("repartition-topic-2", new InternalTopicConfig("repartition-topic-2")))
                .setStateChangelogTopics(
                    Collections.singletonMap("changelog-topic-2", new InternalTopicConfig("changelog-topic-2")))
            )
        );
        StreamsTopology topology = new StreamsTopology("topology-id", subtopologies);
        List<StreamsGroupDescribeResponseData.Subtopology> result = topology.toStreamsGroupDescribeTopology();
        assertEquals(2, result.size());
        assertEquals(Collections.emptyList(), result.get(0).sourceTopicRegex());
        assertEquals("subtopology-1", result.get(0).subtopologyId());
        assertEquals(Collections.singletonList("source-topic-1"), result.get(0).sourceTopics());
        assertEquals(Collections.singletonList("sink-topic-1"), result.get(0).repartitionSinkTopics());
        assertEquals("repartition-topic-1", result.get(0).repartitionSourceTopics().get(0).name());
        assertEquals("changelog-topic-1", result.get(0).stateChangelogTopics().get(0).name());
        assertEquals(Collections.emptyList(), result.get(1).sourceTopicRegex());
        assertEquals("subtopology-2", result.get(1).subtopologyId());
        assertEquals(Collections.singletonList("source-topic-2"), result.get(1).sourceTopics());
        assertEquals(Collections.singletonList("sink-topic-2"), result.get(1).repartitionSinkTopics());
        assertEquals("repartition-topic-2", result.get(1).repartitionSourceTopics().get(0).name());
        assertEquals("changelog-topic-2", result.get(1).stateChangelogTopics().get(0).name());
    }
}

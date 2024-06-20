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

import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Immutable topology metadata.
 */
public class StreamsTopology {

    private final byte[] topologyHash;

    private final Map<String, Subtopology> subtopologies;

    public StreamsTopology(final byte[] topologyHash, final Map<String, Subtopology> subtopologies) {
        this.topologyHash = topologyHash;
        this.subtopologies = subtopologies;
    }

    public byte[] topologyHash() {
        return topologyHash;
    }

    public Map<String, Subtopology> subtopologies() {
        return subtopologies;
    }

    public Set<String> topicSubscription() {
        return subtopologies.values().stream()
            .flatMap(x -> Stream.concat(x.sourceTopics().stream(), x.repartitionSourceTopics().stream().map(
                TopicInfo::name))).collect(
                Collectors.toSet());
    }

    public static StreamsTopology fromRecord(
        StreamsGroupTopologyValue record
    ) {
        return new StreamsTopology(
            record.topologyHash(),
            record.topology().stream().collect(Collectors.toMap(Subtopology::subtopology, x -> x))
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsTopology that = (StreamsTopology) o;
        return Objects.deepEquals(topologyHash, that.topologyHash) && Objects.equals(subtopologies, that.subtopologies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(topologyHash), subtopologies);
    }

    @Override
    public String toString() {
        return "StreamsTopology{" +
            "topologyHash=" + Arrays.toString(topologyHash) +
            ", subtopologies=" + subtopologies +
            '}';
    }
}

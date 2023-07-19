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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the state of an instance (process) in a {@link KafkaStreams} application.
 * It contains the user supplied {@link HostInfo} that can be used by developers to build
 * APIs and services to connect to other instances, the Set of state stores available on
 * the instance and the Set of {@link TopicPartition}s available on the instance.
 * NOTE: This is a point in time view. It may change when rebalances happen.
 */
public class StreamsMetadataImpl implements StreamsMetadata {

    private final HostInfo hostInfo;

    private final Set<String> stateStoreNames;

    private final Set<TopicPartition> topicPartitions;

    private final Set<String> standbyStateStoreNames;

    private final Set<TopicPartition> standbyTopicPartitions;

    private final String topologyName;

    public StreamsMetadataImpl(final HostInfo hostInfo,
                               final Set<String> stateStoreNames,
                               final Set<TopicPartition> topicPartitions,
                               final Set<String> standbyStoreNames,
                               final Set<TopicPartition> standbyTopicPartitions) {
        this(
            hostInfo,
            stateStoreNames,
            topicPartitions,
            standbyStoreNames,
            standbyTopicPartitions,
            null
        );
    }

    public StreamsMetadataImpl(final HostInfo hostInfo,
                               final Set<String> stateStoreNames,
                               final Set<TopicPartition> topicPartitions,
                               final Set<String> standbyStoreNames,
                               final Set<TopicPartition> standbyTopicPartitions,
                               final String topologyName) {
        this.hostInfo = hostInfo;
        this.stateStoreNames = Collections.unmodifiableSet(stateStoreNames);
        this.topicPartitions = Collections.unmodifiableSet(topicPartitions);
        this.standbyTopicPartitions = Collections.unmodifiableSet(standbyTopicPartitions);
        this.standbyStateStoreNames = Collections.unmodifiableSet(standbyStoreNames);
        this.topologyName = topologyName;
    }

    /**
     * The value of {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG} configured for the streams
     * instance, which is typically host/port
     *
     * @return {@link HostInfo} corresponding to the streams instance
     */
    @Override
    public HostInfo hostInfo() {
        return hostInfo;
    }

    /**
     * State stores owned by the instance as an active replica
     *
     * @return set of active state store names
     */
    @Override
    public Set<String> stateStoreNames() {
        return stateStoreNames;
    }

    /**
     * Topic partitions consumed by the instance as an active replica
     *
     * @return set of active topic partitions
     */
    @Override
    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    /**
     * (Source) Topic partitions for which the instance acts as standby.
     *
     * @return set of standby topic partitions
     */
    @Override
    public Set<TopicPartition> standbyTopicPartitions() {
        return standbyTopicPartitions;
    }

    /**
     * State stores owned by the instance as a standby replica
     *
     * @return set of standby state store names
     */
    @Override
    public Set<String> standbyStateStoreNames() {
        return standbyStateStoreNames;
    }

    @Override
    public String host() {
        return hostInfo.host();
    }

    @SuppressWarnings("unused")
    @Override
    public int port() {
        return hostInfo.port();
    }

    public String topologyName() {
        return topologyName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StreamsMetadataImpl that = (StreamsMetadataImpl) o;
        return Objects.equals(hostInfo, that.hostInfo)
            && Objects.equals(stateStoreNames, that.stateStoreNames)
            && Objects.equals(topicPartitions, that.topicPartitions)
            && Objects.equals(standbyStateStoreNames, that.standbyStateStoreNames)
            && Objects.equals(standbyTopicPartitions, that.standbyTopicPartitions)
            && Objects.equals(topologyName, that.topologyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostInfo, stateStoreNames, topicPartitions, standbyStateStoreNames, standbyTopicPartitions);
    }

    @Override
    public String toString() {
        final String str =
            "StreamsMetadata {" +
                "hostInfo=" + hostInfo +
                ", stateStoreNames=" + stateStoreNames +
                ", topicPartitions=" + topicPartitions +
                ", standbyStateStoreNames=" + standbyStateStoreNames +
                ", standbyTopicPartitions=" + standbyTopicPartitions;
        if (topologyName == null) {
            return str + '}';
        } else {
            return str +
                ", topologyName=" + topologyName +
                '}';
        }
    }
}

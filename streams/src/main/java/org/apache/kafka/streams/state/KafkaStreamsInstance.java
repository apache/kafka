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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Set;

/**
 * Represents the state of an instance (process) in a {@link KafkaStreams} application.
 * It contains the user supplied {@link HostInfo} that can be used by developers to build
 * APIs and services to connect to other instances, the Set of state stores available on
 * the instance and the Set of {@link TopicPartition}s available on the instance.
 * NOTE: This is a point in time view. It may change when rebalances happen.
 */
public class KafkaStreamsInstance {
    private final HostInfo hostInfo;
    private final Set<String> stateStores;
    private final Set<TopicPartition> topicPartitions;

    public KafkaStreamsInstance(final HostInfo hostInfo,
                                final Set<String> stateStores,
                                final Set<TopicPartition> topicPartitions) {

        this.hostInfo = hostInfo;
        this.stateStores = stateStores;
        this.topicPartitions = topicPartitions;
    }

    public HostInfo getHostInfo() {
        return hostInfo;
    }

    public Set<String> getStateStoreNames() {
        return stateStores;
    }

    public Set<TopicPartition> getTopicPartitions() {
        return topicPartitions;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final KafkaStreamsInstance that = (KafkaStreamsInstance) o;

        if (!hostInfo.equals(that.hostInfo)) return false;
        if (!stateStores.equals(that.stateStores)) return false;
        return topicPartitions.equals(that.topicPartitions);

    }

    @Override
    public int hashCode() {
        int result = hostInfo.hashCode();
        result = 31 * result + stateStores.hashCode();
        result = 31 * result + topicPartitions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "KafkaStreamsInstance{" +
                "hostInfo=" + hostInfo +
                ", stateStores=" + stateStores +
                ", topicPartitions=" + topicPartitions +
                '}';
    }
}

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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collections;
import java.util.Set;

/**
 * Represents the state of an instance (process) in a {@link KafkaStreams} application.
 * It contains the user supplied {@link HostInfo} that can be used by developers to build
 * APIs and services to connect to other instances, the Set of state stores available on
 * the instance and the Set of {@link TopicPartition}s available on the instance.
 * NOTE: This is a point in time view. It may change when rebalances happen.
 */
@InterfaceStability.Evolving
public class StreamsMetadata {
    /**
     * Sentinel to indicate that the StreamsMetadata is currently unavailable. This can occur during rebalance
     * operations.
     */
    public final static StreamsMetadata NOT_AVAILABLE = new StreamsMetadata(new HostInfo("unavailable", -1),
                                                                                Collections.<String>emptySet(),
                                                                                Collections.<TopicPartition>emptySet(),
                                                                                Collections.<TaskId>emptySet());

    private final HostInfo hostInfo;
    private final Set<String> stateStoreNames;
    private final Set<TaskId> taskIds;
    private final Set<TopicPartition> partitionsForHost;

    public StreamsMetadata(final HostInfo hostInfo,
                           final Set<String> stateStoreNames,
                           final Set<TopicPartition> partitionsForHost,
                           final Set<TaskId> taskIds) {

        this.hostInfo = hostInfo;
        this.stateStoreNames = stateStoreNames;
        this.partitionsForHost = partitionsForHost;
        this.taskIds = taskIds;
    }

    public HostInfo hostInfo() {
        return hostInfo;
    }

    public Set<TaskId> taskIds() {
        return taskIds;
    }

    public Set<String> stateStoreNames() {
        return stateStoreNames;
    }

    public Set<TopicPartition> topicPartitions() {
        return partitionsForHost;
    }

    public String host() {
        return hostInfo.host();
    }

    public int port() {
        return hostInfo.port();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsMetadata that = (StreamsMetadata) o;
        if (!hostInfo.equals(that.hostInfo)) return false;
        if (!stateStoreNames.equals(that.stateStoreNames)) return false;
        if (taskIds != null)
            return taskIds.equals(that.taskIds);
        else
            return partitionsForHost.equals(that.partitionsForHost);

    }

    @Override
    public int hashCode() {
        int result = hostInfo.hashCode();
        result = 31 * result + stateStoreNames.hashCode();
        result = 31 * result + taskIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StreamsMetadata{" +
                "hostInfo=" + hostInfo +
                ", stateStoreNames=" + stateStoreNames +
                ", taskIds=" + taskIds +
                '}';
    }
}

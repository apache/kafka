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
package org.apache.kafka.streams;

import org.apache.kafka.streams.state.HostInfo;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents all the metadata related to a key, where a particular key resides in a {@link KafkaStreams} application.
 * It contains the active {@link HostInfo} and a set of standby {@link HostInfo}s, denoting the instances where the key resides.
 * It also contains the partition number where the key belongs, which could be useful when used in conjunction with other APIs.
 * e.g: Relating with lags for that store partition.
 * NOTE: This is a point in time view. It may change as rebalances happen.
 */
public class KeyQueryMetadata {
    /**
     * Sentinel to indicate that the KeyQueryMetadata is currently unavailable. This can occur during rebalance
     * operations.
     */
    public static final KeyQueryMetadata NOT_AVAILABLE =
        new KeyQueryMetadata(HostInfo.unavailable(), Collections.emptySet(), -1);

    private final HostInfo activeHost;

    private final Set<HostInfo> standbyHosts;

    private final int partition;

    public KeyQueryMetadata(final HostInfo activeHost, final Set<HostInfo> standbyHosts, final int partition) {
        this.activeHost = activeHost;
        this.standbyHosts = standbyHosts;
        this.partition = partition;
    }

    /**
     * Get the active Kafka Streams instance for given key.
     *
     * @return active instance's {@link HostInfo}
     * @deprecated Use {@link #activeHost()} instead.
     */
    @Deprecated
    public HostInfo getActiveHost() {
        return activeHost;
    }

    /**
     * Get the Kafka Streams instances that host the key as standbys.
     *
     * @return set of standby {@link HostInfo} or a empty set, if no standbys are configured
     * @deprecated Use {@link #standbyHosts()} instead.
     */
    @Deprecated
    public Set<HostInfo> getStandbyHosts() {
        return standbyHosts;
    }

    /**
     * Get the store partition corresponding to the key.
     *
     * @return store partition number
     * @deprecated Use {@link #partition()} instead.
     */
    @Deprecated
    public int getPartition() {
        return partition;
    }

    /**
     * Get the active Kafka Streams instance for given key.
     *
     * @return active instance's {@link HostInfo}
     */
    public HostInfo activeHost() {
        return activeHost;
    }

    /**
     * Get the Kafka Streams instances that host the key as standbys.
     *
     * @return set of standby {@link HostInfo} or a empty set, if no standbys are configured
     */
    public Set<HostInfo> standbyHosts() {
        return standbyHosts;
    }

    /**
     * Get the store partition corresponding to the key.
     *
     * @return store partition number
     */
    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeyQueryMetadata)) {
            return false;
        }
        final KeyQueryMetadata keyQueryMetadata = (KeyQueryMetadata) obj;
        return Objects.equals(keyQueryMetadata.activeHost, activeHost)
            && Objects.equals(keyQueryMetadata.standbyHosts, standbyHosts)
            && Objects.equals(keyQueryMetadata.partition, partition);
    }

    @Override
    public String toString() {
        return "KeyQueryMetadata {" +
                "activeHost=" + activeHost +
                ", standbyHosts=" + standbyHosts +
                ", partition=" + partition +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(activeHost, standbyHosts, partition);
    }
}

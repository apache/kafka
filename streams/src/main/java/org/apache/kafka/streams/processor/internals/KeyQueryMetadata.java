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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents all the metadata where a particular key resides in a {@link KafkaStreams} application.
 * It contains the user supplied {@link HostInfo} and the Set of standby Hosts where the key would be found in case an application
 * has more than one standby. It also contains the partition number where the key belongs, this information would be useful in
 * developing another apis on top, to fetch Offset Lag or Time based lag for the partition where the key belongs.
 * NOTE: This is a point in time view. It may change when rebalances happen.
 */
public class KeyQueryMetadata {
    /**
     * Sentinel to indicate that the KeyQueryMetadata is currently unavailable. This can occur during rebalance
     * operations.
     */
    public final static KeyQueryMetadata NOT_AVAILABLE = new KeyQueryMetadata(new HostInfo("unavailable", -1),
            Collections.emptySet(),
            -1);

    // Active streams instance for key
    private final HostInfo activeHost;
    // Streams instances that host the key as standbys
    private final Set<HostInfo> standbyHosts;

    // Store partition corresponding to the key.
    private final int partition;

    public KeyQueryMetadata(final HostInfo activeHost, final Set<HostInfo> standbyHosts, final int partition) {
        this.activeHost = activeHost;
        this.standbyHosts = standbyHosts;
        this.partition = partition;
    }

    public HostInfo getActiveHost() {
        return activeHost;
    }

    public Set<HostInfo> getStandbyHosts() {
        return standbyHosts;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof KeyQueryMetadata)) {
            return false;
        }
        KeyQueryMetadata keyQueryMetadata = (KeyQueryMetadata) obj;
        return Objects.equals(keyQueryMetadata.activeHost, activeHost) && Objects.equals(keyQueryMetadata.standbyHosts, standbyHosts) && Objects.equals(keyQueryMetadata.partition, partition);
    }

    @Override
    public String toString() {
        return "KeyQueryMetadata{" +
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

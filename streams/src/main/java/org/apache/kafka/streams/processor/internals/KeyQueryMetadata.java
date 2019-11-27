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

import org.apache.kafka.streams.state.HostInfo;

import java.util.Collections;
import java.util.Set;

public class KeyQueryMetadata {
    /**
     * Sentinel to indicate that the StreamsMetadata is currently unavailable. This can occur during rebalance
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
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final KeyQueryMetadata that = (KeyQueryMetadata) obj;
        if (activeHost != that.activeHost) {
            return false;
        }
        if (!standbyHosts.equals(that.standbyHosts)) {
            return false;
        }
        return partition == that.partition;
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
        int result = activeHost.hashCode();
        result = 31 * result + standbyHosts.hashCode();
        result = 31 * result + partition;
        return result;
    }
}

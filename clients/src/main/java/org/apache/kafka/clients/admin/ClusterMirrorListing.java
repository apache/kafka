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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A lightweight listing of a cluster mirror.
 * <p>
 * This class provides basic information about a cluster mirror discovered via
 * {@link AdminClient#listClusterMirrors(ListClusterMirrorsOptions)}.
 * Use {@link AdminClient#describeClusterMirrors(String[], DescribeClusterMirrorsOptions)}
 * to retrieve detailed information about a mirror, including leader states and authorization metadata.
 * <p>
 * The mirror name uniquely identifies the mirror within the destination cluster.
 * The source bootstrap addresses and cluster ID identify the remote cluster being mirrored.
 * Topic names are included if the mirror is configured to mirror specific topics,
 * and may be filtered based on request options.
 */
@InterfaceStability.Evolving
public class ClusterMirrorListing {
    private final String mirrorName;
    private final String sourceBootstrap;
    private final String sourceClusterId;
    private final List<String> topicNames;

    public ClusterMirrorListing(String mirrorName, String sourceBootstrap, String sourceClusterId, List<String> topicNames) {
        this.mirrorName = mirrorName;
        this.sourceBootstrap = sourceBootstrap;
        this.sourceClusterId = sourceClusterId;
        this.topicNames = topicNames;
    }

    public ClusterMirrorListing(String mirrorName, String sourceBootstrap, String sourceClusterId) {
        this(mirrorName, sourceBootstrap, sourceClusterId, Collections.emptyList());
    }

    public ClusterMirrorListing(String mirrorName, String sourceBootstrap) {
        this(mirrorName, sourceBootstrap, "", Collections.emptyList());
    }

    /**
     * The name of the cluster mirror. This is unique within the destination cluster.
     */
    public String mirrorName() {
        return mirrorName;
    }

    /**
     * The bootstrap servers of the source cluster being mirrored.
     */
    public String sourceBootstrap() {
        return sourceBootstrap;
    }

    /**
     * The cluster ID of the source cluster being mirrored.
     */
    public String sourceClusterId() {
        return sourceClusterId;
    }

    /**
     * The topic names configured for this mirror.
     * <p>
     * This list may be filtered based on options provided to the list request.
     * If a filter was specified in {@link ListClusterMirrorsOptions}, only topics
     * matching the desired state criteria are included.
     */
    public List<String> topicNames() {
        return topicNames;
    }

    @Override
    public String toString() {
        return "ClusterMirrorListing(mirrorName='" + mirrorName + "', sourceBootstrap='" + sourceBootstrap
                + "', sourceClusterId='" + sourceClusterId + "', topicNames=" + topicNames + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(mirrorName, sourceBootstrap, sourceClusterId, topicNames);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterMirrorListing)) return false;
        ClusterMirrorListing that = (ClusterMirrorListing) o;
        return Objects.equals(mirrorName, that.mirrorName) &&
               Objects.equals(sourceBootstrap, that.sourceBootstrap) &&
               Objects.equals(sourceClusterId, that.sourceClusterId) &&
               Objects.equals(topicNames, that.topicNames);
    }
}

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
package org.apache.kafka.common;


import java.util.Objects;

/**
 * The <code>ClusterResource</code> class encapsulates metadata for a Kafka cluster.
 */
public class ClusterResource {

    private final String clusterId;

    /**
     * Create {@link ClusterResource} with a cluster id. Note that cluster id may be {@code null} if the
     * metadata request was sent to a broker without support for cluster ids. The first version of Kafka
     * to support cluster id is 0.10.1.0.
     * @param clusterId
     */
    public ClusterResource(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Return the cluster id. Note that it may be {@code null} if the metadata request was sent to a broker without
     * support for cluster ids. The first version of Kafka to support cluster id is 0.10.1.0.
     */
    public String clusterId() {
        return clusterId;
    }

    @Override
    public String toString() {
        return "ClusterResource(clusterId=" + clusterId + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterResource that = (ClusterResource) o;
        return Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId);
    }
}

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

package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;


/**
 * Convenience tool for multi-cluster environments. Wraps {@link MirrorClient}
 * <p>
 * Properties passed to these methods are used to construct internal {@link Admin} and {@link Consumer} clients.
 * Sub-configs like "admin.xyz" are also supported. For example:
 * <pre>
 *     bootstrap.servers = host1:9092
 *     consumer.client.id = mm2-client
 * </pre>
 * @see MirrorClientConfig for additional properties used by the internal MirrorClient.
 */
public final class RemoteClusterUtils {

    // utility class
    private RemoteClusterUtils() {}

    /**
     * Finds the shortest number of hops from an upstream cluster.
     * Returns -1 if the cluster is unreachable.
     */
    public static int replicationHops(Map<String, Object> properties, String upstreamClusterAlias)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.replicationHops(upstreamClusterAlias);
        }
    }

    /**
     * Finds all heartbeats topics
     */
    public static Set<String> heartbeatTopics(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.heartbeatTopics();
        }
    }

    /**
     * Finds all checkpoints topics
     */
    public static Set<String> checkpointTopics(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.checkpointTopics();
        }
    }

    /**
     * Finds all upstream clusters
     */
    public static Set<String> upstreamClusters(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.upstreamClusters();
        }
    }

    /**
     * Translates a remote consumer group's offsets into corresponding local offsets. Topics are automatically
     *  renamed according to the configured {@link ReplicationPolicy}.
     *  @param properties Map of properties to instantiate a {@link MirrorClient}
     *  @param remoteClusterAlias The alias of the remote cluster
     *  @param consumerGroupId The group ID of remote consumer group
     *  @param timeout The maximum time to block when consuming from the checkpoints topic
     */
    public static Map<TopicPartition, OffsetAndMetadata> translateOffsets(Map<String, Object> properties,
            String remoteClusterAlias, String consumerGroupId, Duration timeout)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.remoteConsumerOffsets(consumerGroupId, remoteClusterAlias, timeout);
        }
    }
}

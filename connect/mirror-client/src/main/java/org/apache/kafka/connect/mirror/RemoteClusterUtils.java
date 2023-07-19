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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.time.Duration;


/** Convenience methods for multi-cluster environments. Wraps {@link MirrorClient}
 *  <p>
 *  Properties passed to these methods are used to construct internal Admin and Consumer clients.
 *  Sub-configs like "admin.xyz" are also supported. For example:
 *  </p>
 *  <pre>
 *      bootstrap.servers = host1:9092
 *      consumer.client.id = mm2-client
 *  </pre>
 *  <p>
 *  @see MirrorClientConfig for additional properties used by the internal MirrorClient.
 *  </p>
 */
public final class RemoteClusterUtils {

    // utility class
    private RemoteClusterUtils() {}

    /** Find shortest number of hops from an upstream cluster.
     *  Returns -1 if the cluster is unreachable */ 
    public static int replicationHops(Map<String, Object> properties, String upstreamClusterAlias)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.replicationHops(upstreamClusterAlias);
        }
    }

    /** Find all heartbeat topics */
    public static Set<String> heartbeatTopics(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.heartbeatTopics();
        }
    }

    /** Find all checkpoint topics */
    public static Set<String> checkpointTopics(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.checkpointTopics();
        }
    }

    /** Find all upstream clusters */
    public static Set<String> upstreamClusters(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.upstreamClusters();
        }
    }

    /** Translate a remote consumer group's offsets into corresponding local offsets. Topics are automatically
     *  renamed according to the ReplicationPolicy.
     *  @param properties {@link MirrorClientConfig} properties to instantiate a {@link MirrorClient}
     *  @param consumerGroupId group ID of remote consumer group
     *  @param remoteClusterAlias alias of remote cluster
     *  @param timeout timeout
     */
    public static Map<TopicPartition, OffsetAndMetadata> translateOffsets(Map<String, Object> properties,
            String remoteClusterAlias, String consumerGroupId, Duration timeout)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.remoteConsumerOffsets(consumerGroupId, remoteClusterAlias, timeout);
        }
    }
}

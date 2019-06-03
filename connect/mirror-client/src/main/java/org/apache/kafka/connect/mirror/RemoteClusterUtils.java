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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/** Convenience methods for multi-cluster environments. */
public final class RemoteClusterUtils {
    private static final Logger log = LoggerFactory.getLogger(RemoteClusterUtils.class);

    // utility class
    private RemoteClusterUtils() {}

    public static int replicationHops(Map<String, Object> properties, String upstreamClusterAlias)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.replicationHops(upstreamClusterAlias);
        }
    }

    public static Set<String> heartbeatTopics(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.heartbeatTopics();
        }
    }

    public static Set<String> checkpointTopics(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.checkpointTopics();
        }
    }

    public static Set<String> upstreamClusters(Map<String, Object> properties)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.upstreamClusters();
        }
    }

    public static Map<TopicPartition, OffsetAndMetadata> translateOffsets(Map<String, Object> properties,
            String targetClusterAlias, String consumerGroupId, Duration timeout)
            throws InterruptedException, TimeoutException {
        try (MirrorClient client = new MirrorClient(properties)) {
            return client.remoteConsumerOffsets(consumerGroupId, targetClusterAlias, timeout);
        }
    }
}

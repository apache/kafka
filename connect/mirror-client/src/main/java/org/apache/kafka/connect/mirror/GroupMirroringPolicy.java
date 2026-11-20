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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceAudience;

import java.util.Map;

/**
 * An interface used by the MirrorMaker connectors to rename consumer groups between source and target clusters.
 * <p>
 * Analogous to {@link ReplicationPolicy} for topics, this allows operators to define a custom naming
 * convention for consumer groups on the target cluster. For example, a source group {@code "my-app"}
 * could be mirrored as {@code "source.my-app"} on the target cluster.
 * <p>
 * Implementations must be thread-safe and may optionally implement {@link Configurable} to receive
 * connector configuration.
 */
@InterfaceAudience.Public
public interface GroupMirroringPolicy extends Configurable {

    /**
     * Returns the name that the given source consumer group should use on the target cluster.
     * <p>
     * This name is used when emitting checkpoints and when syncing consumer group offsets to the target.
     *
     * @param sourceClusterAlias the alias of the source cluster
     * @param group the consumer group ID on the source cluster
     * @return the consumer group ID to use on the target cluster
     */
    String targetGroupId(String sourceClusterAlias, String group);

    /**
     * Returns the source consumer group ID for a given group on the target cluster.
     * <p>
     * This is the inverse of {@link #targetGroupId(String, String)} and is used when translating
     * checkpoint records back to source consumer group IDs. Returns {@code null} if the given group
     * is not a mirrored group or the mapping cannot be determined.
     * <p>
     * This method is reserved for future use by {@code MirrorClient} to reverse-map checkpoint
     * consumer group IDs back to their source names. It is not called by the connector runtime yet.
     *
     * @param sourceClusterAlias the alias of the source cluster
     * @param targetGroup the consumer group ID on the target cluster
     * @return the original consumer group ID on the source cluster, or {@code null}
     */
    String sourceGroupId(String sourceClusterAlias, String targetGroup);

    /**
     * Called to provide configuration to this policy instance.
     * <p>
     * Default implementation is a no-op; override if your policy needs configuration.
     */
    @Override
    default void configure(Map<String, ?> configs) {
        // no-op by default
    }
}

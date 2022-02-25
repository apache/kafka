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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** IdentityReplicationPolicy does not rename remote topics. This is useful for migrating
  * from legacy MM1, or for any use-case involving one-way replication.
  *
  * N.B. MirrorMaker is not able to prevent cycles when using this class, so take care that
  * your replication topology is acyclic. If migrating from MirrorMaker v1, this will likely
  * already be the case.
  */
public class IdentityReplicationPolicy extends DefaultReplicationPolicy {
    private static final Logger log = LoggerFactory.getLogger(IdentityReplicationPolicy.class);

    public static final String SOURCE_CLUSTER_ALIAS_CONFIG = "source.cluster.alias";

    private String sourceClusterAlias = null;

    @Override
    public void configure(Map<String, ?> props) {
        super.configure(props);
        if (props.containsKey(SOURCE_CLUSTER_ALIAS_CONFIG)) {
            sourceClusterAlias = (String) props.get(SOURCE_CLUSTER_ALIAS_CONFIG);
            log.info("Using source cluster alias `{}`.", sourceClusterAlias);
        }
    }

    /** Unlike DefaultReplicationPolicy, IdentityReplicationPolicy does not include the source
      * cluster alias in the remote topic name. Instead, topic names are unchanged.
      *
      * In the special case of heartbeats, we defer to DefaultReplicationPolicy.
      */
    @Override
    public String formatRemoteTopic(String sourceClusterAlias, String topic) {
        if (looksLikeHeartbeat(topic)) {
            return super.formatRemoteTopic(sourceClusterAlias, topic);
        } else {
            return topic;
        }
    }

    /** Unlike DefaultReplicationPolicy, IdentityReplicationPolicy cannot know the source of
      * a remote topic based on its name alone. If `source.cluster.alias` is provided,
      * `topicSource` will return that.
      *
      * In the special case of heartbeats, we defer to DefaultReplicationPolicy.
      */
    @Override
    public String topicSource(String topic) {
        if (looksLikeHeartbeat(topic)) {
            return super.topicSource(topic);
        } else {
            return sourceClusterAlias;
        }
    }

    /** Since any topic may be a "remote topic", this just returns `topic`.
      *
      * In the special case of heartbeats, we defer to DefaultReplicationPolicy.
      */
    @Override
    public String upstreamTopic(String topic) {
        if (looksLikeHeartbeat(topic)) {
            return super.upstreamTopic(topic);
        } else {
            return topic;
        }
    }

    private boolean looksLikeHeartbeat(String topic) {
        return topic != null && topic.endsWith(heartbeatsTopic());
    }
}

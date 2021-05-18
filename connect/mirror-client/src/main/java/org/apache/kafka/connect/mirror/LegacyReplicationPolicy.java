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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** LegacyReplicationPolicy attempts to mimic MirrorMaker v1, and thus by default does not
  * rename topics. In order to support active/active replication with careful configuration,
  * topics can be renamed with an optional suffix, e.g. "topic1.replica".
  *
  * LegacyReplicationPolicy cannot, in general, detect whether a topic is remote or not, nor
  * which cluster a topic may have been replicated from. If a `remote.topic.suffix` is
  * provided, we use that to distinguish remote topics. Otherwise we consider all topics as
  * remote topics. If a `source.cluster.alias` is provided, we use that as the source of
  * any remote topics. Otherwise we return null for `topicSource()`.
  *
  * Paradoxically, then, the default behavior of LegacyReplicationPolicy is to consider all
  * topics to be remote but with no known source.
  *
  * N.B. MirrorMaker is not able to prevent cycles when using this class, so take care that
  * your replication topology is acyclic. If migrating from MirrorMaker v1, this will likely
  * already be the case.
  */
public class LegacyReplicationPolicy implements ReplicationPolicy, Configurable {
    private static final Logger log = LoggerFactory.getLogger(LegacyReplicationPolicy.class);

    public static final String REMOTE_TOPIC_SUFFIX_CONFIG = "replication.policy.remote.topic.suffix";
    public static final String SOURCE_CLUSTER_ALIAS_CONFIG = "source.cluster.alias";

    private String remoteTopicSuffix = "";
    private String sourceClusterAlias = null;

    public LegacyReplicationPolicy() {
    }

    // Visible for testing
    LegacyReplicationPolicy(String remoteTopicSuffix, String sourceClusterAlias) {
        this.remoteTopicSuffix = remoteTopicSuffix;
        this.sourceClusterAlias = sourceClusterAlias;
    }

    @Override
    public void configure(Map<String, ?> props) {
        if (props.containsKey(REMOTE_TOPIC_SUFFIX_CONFIG)) {
            remoteTopicSuffix = (String) props.get(REMOTE_TOPIC_SUFFIX_CONFIG);
            log.info("Using custom remote topic suffix `{}`.", remoteTopicSuffix);
        }
        if (props.containsKey(SOURCE_CLUSTER_ALIAS_CONFIG)) {
            sourceClusterAlias = (String) props.get(SOURCE_CLUSTER_ALIAS_CONFIG);
            log.info("Using source cluster alias `{}`.", sourceClusterAlias);
        }
    }

    /** Unlike DefaultReplicationPolicy, LegacyReplicationPolicy does not include the source
      * cluster alias in the remote topic name. By default, topic names are unchanged. If
      * a `remote.topic.suffix` is provided, topic are renamed accordingly.
      */
    @Override
    public String formatRemoteTopic(String sourceClusterAlias, String topic) {
        return topic + remoteTopicSuffix;
    }

    /** Unlike DefaultReplicationPolicy, LegacyReplicationPolicy cannot know the source of
      * a remote topic based on its name alone. If `source.cluster.alias` is provided,
      * `topicSource` will return that for anything that looks like a remote topic. By default,
      * all topics look like remote topics.
      */
    @Override
    public String topicSource(String topic) {
        if (topic.endsWith(remoteTopicSuffix)) {
            return sourceClusterAlias;
        } else {
            return null;
        }
    }

    /** Unlike DefaultReplicationPolicy, LegacyReplicationPolicy cannot distinguish remote
      * topics from regular topics by default. If `remote.topic.suffix` is provided, this
      * method will strip that and return the original topic. By default, `topic` is returned.
      */
    @Override
    public String upstreamTopic(String topic) {
        if (topic.endsWith(remoteTopicSuffix)) {
            return topic.substring(0, topic.length() - remoteTopicSuffix.length());
        } else {
            return topic;
        }
    }

    /** If remote.topic.suffix is provided, treat suffixed topics as internal topics.
      * This prevents replicated topics from being further replicated when using
      * LegacyReplicationPolicy.
      */
    @Override
    public boolean isInternalTopic(String topic) {
        return ReplicationPolicy.super.isInternalTopic(topic)
            || (!remoteTopicSuffix.isEmpty() && topic.endsWith(remoteTopicSuffix));
    }
}

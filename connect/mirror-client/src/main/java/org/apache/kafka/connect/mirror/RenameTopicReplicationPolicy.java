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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.mirror.MirrorClientConfig.REPLICATION_POLICY_TOPICS_MAP;

/** Defines remote topics name based on configuration  "us-west.topic1". The separator is customizable and defaults to a period. */
public class RenameTopicReplicationPolicy extends DefaultReplicationPolicy {

    private static final Logger log = LoggerFactory.getLogger(RenameTopicReplicationPolicy.class);
    private final Map<String, String> topicMap = new HashMap<String, String>();

    @Override
    public void configure(Map<String, ?> props) {
        super.configure(props);
        if (props.containsKey(REPLICATION_POLICY_TOPICS_MAP)) {
            String configMap = (String) props.get(REPLICATION_POLICY_TOPICS_MAP);
            log.info("Using custom remote topic renaming: '{}'", configMap);
            String[] topicAssignments = configMap.split(";");
            for (String topicAssignment : topicAssignments) {
                String[] topicsArray = topicAssignment.split(",");
                if (topicsArray.length == 2) {
                    topicMap.put(topicsArray[0], topicsArray[1]);
                } else {
                    log.warn("Malformed arguments were passed in '{}'", topicAssignment);
                }
            }
        }
    }

    @Override
    public String formatRemoteTopic(String sourceClusterAlias, String topic) {
        String targetTopic = topicMap.containsKey(topic) ?  topicMap.get(topic) : topic;
        return super.formatRemoteTopic(sourceClusterAlias, targetTopic);
    }
}

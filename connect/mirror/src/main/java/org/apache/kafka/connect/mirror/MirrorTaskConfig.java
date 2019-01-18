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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

public class MirrorTaskConfig extends MirrorConnectorConfig {

    public MirrorTaskConfig(Map<?, ?> props) {
        super(TASK_CONFIG_DEF, props);
    }

    Set<TopicPartition> taskTopicPartitions() {
        return getList(TASK_TOPIC_PARTITIONS).stream()
            .map(MirrorUtils::decodeTopicPartition)
            .collect(Collectors.toSet());
    }

    Set<String> taskConsumerGroups() {
        return new HashSet<>(getList(TASK_CONSUMER_GROUPS));
    } 

    protected static final ConfigDef TASK_CONFIG_DEF = CONNECTOR_CONFIG_DEF
        .define(
            TASK_TOPIC_PARTITIONS,
            ConfigDef.Type.LIST,
            null,
            ConfigDef.Importance.LOW,
            TASK_TOPIC_PARTITIONS_DOC)
        .define(
            TASK_CONSUMER_GROUPS,
            ConfigDef.Type.LIST,
            null,
            ConfigDef.Importance.LOW,
            TASK_CONSUMER_GROUPS_DOC);
}

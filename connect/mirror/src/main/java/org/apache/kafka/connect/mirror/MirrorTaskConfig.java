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
import java.util.List;
import java.util.HashSet;
import java.util.Collections;
import java.util.stream.Collectors;

public class MirrorTaskConfig extends MirrorConnectorConfig {

    private static final String TASK_TOPIC_PARTITIONS_DOC = "Topic-partitions assigned to this task to replicate.";
    private static final String TASK_CONSUMER_GROUPS_DOC = "Consumer groups assigned to this task to replicate.";

    public MirrorTaskConfig(Map<String, String> props) {
        super(TASK_CONFIG_DEF, props);
    }

    Set<TopicPartition> taskTopicPartitions() {
        List<String> fields = getList(TASK_TOPIC_PARTITIONS);
        if (fields == null || fields.isEmpty()) {
            return Collections.emptySet();
        }
        return fields.stream()
            .map(MirrorUtils::decodeTopicPartition)
            .collect(Collectors.toSet());
    }

    Set<String> taskConsumerGroups() {
        List<String> fields = getList(TASK_CONSUMER_GROUPS);
        if (fields == null || fields.isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(fields);
    } 

    MirrorMetrics metrics() {
        MirrorMetrics metrics = new MirrorMetrics(this);
        metricsReporters().forEach(metrics::addReporter);
        return metrics;
    }
 
    protected static final ConfigDef TASK_CONFIG_DEF = new ConfigDef(CONNECTOR_CONFIG_DEF)
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

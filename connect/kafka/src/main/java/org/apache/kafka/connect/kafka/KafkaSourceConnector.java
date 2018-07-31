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
package org.apache.kafka.connect.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;


/**
 * KafkaConnector is a Kafka Connect Connector implementation that generates tasks
 * to ingest messages from a source kafka cluster
 */

public class KafkaSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceConnector.class);
    private KafkaSourceConnectorConfig connectorConfig;

    private PartitionMonitor partitionMonitor;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> config) throws ConfigException {
        LOG.info("Connector: start()");
        connectorConfig = new KafkaSourceConnectorConfig(config);
        LOG.info("Starting Partition Monitor to monitor source kafka cluster partitions");
        partitionMonitor = new PartitionMonitor(context, connectorConfig);
        partitionMonitor.start();
    }


    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSourceTask.class;
    }


    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> leaderTopicPartitions = partitionMonitor.getCurrentLeaderTopicPartitions()
            .stream()
            .map(LeaderTopicPartition::toString)
            .sorted() // Potential task performance/overhead improvement by roughly grouping tasks and leaders
            .collect(Collectors.toList());
        int taskCount = Math.min(maxTasks, leaderTopicPartitions.size());
        if (taskCount < 1) {
            LOG.warn("No tasks to start.");
            return new ArrayList<>();
        }
        return ConnectorUtils.groupPartitions(leaderTopicPartitions, taskCount)
            .stream()
            .map(leaderTopicPartitionsGroup -> {
                Map<String, String> taskConfig = new HashMap<>();
                taskConfig.putAll(connectorConfig.allAsStrings());
                taskConfig.put(KafkaSourceConnectorConfig.TASK_LEADER_TOPIC_PARTITION_CONFIG, String.join(",", leaderTopicPartitionsGroup));
                return taskConfig;
            })
            .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        LOG.info("Connector received stop(). Cleaning Up.");
        partitionMonitor.shutdown();
        LOG.info("Connector stopped.");
    }

    @Override
    public ConfigDef config() {
        LOG.info("Connector: config()");
        return KafkaSourceConnectorConfig.config;
    }
    
}

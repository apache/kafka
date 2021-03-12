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

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replicate data, configuration, and ACLs between clusters.
 *
 *  @see MirrorConnectorConfig for supported config properties.
 */
public class MirrorSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceConnector.class);

    private Scheduler scheduler;
    private MirrorConnectorConfig config;
    private String connectorName;
    private List<TopicPartition> knownSourceTopicPartitions = Collections.emptyList();
    private MirrorConnectorCommon connectorCommon;
    public MirrorSourceConnector() {
        // nop
    }

    // visible for testing
    MirrorSourceConnector(List<TopicPartition> knownSourceTopicPartitions, MirrorConnectorConfig config) {
        this.knownSourceTopicPartitions = knownSourceTopicPartitions;
        this.config = config;
        connectorCommon = new MirrorConnectorCommon();
        connectorCommon.setKnownSourceTopicPartitions(knownSourceTopicPartitions);
    }

    
    @Override
    public void start(Map<String, String> props) {
        long start = System.currentTimeMillis();
        config = new MirrorConnectorConfig(props);
        if (!config.enabled()) {
            return;
        }
        scheduler = new Scheduler(MirrorSourceConnector.class, config.adminTimeout());
        connectorCommon = new MirrorConnectorCommon(config, context, scheduler);
        connectorName = config.connectorName();
        knownSourceTopicPartitions = connectorCommon.getKnownSourceTopicPartitions();
        log.info("Started {} with {} topic-partitions.", connectorName, knownSourceTopicPartitions.size());
        log.info("Starting {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        if (!config.enabled()) {
            return;
        }
        connectorCommon.stop();
        log.info("Stopping {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorSourceTask.class;
    }

    // divide topic-partitions among tasks
    // since each mirrored topic has different traffic and number of partitions, to balance the load
    // across all mirrormaker instances (workers), 'roundrobin' helps to evenly assign all
    // topic-partition to the tasks, then the tasks are further distributed to workers.
    // For example, 3 tasks to mirror 3 topics with 8, 2 and 2 partitions respectively.
    // 't1' denotes 'task 1', 't0p5' denotes 'topic 0, partition 5'
    // t1 -> [t0p0, t0p3, t0p6, t1p1]
    // t2 -> [t0p1, t0p4, t0p7, t2p0]
    // t3 -> [t0p2, t0p5, t1p0, t2p1]
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        knownSourceTopicPartitions = connectorCommon.getKnownSourceTopicPartitions();
        if (!config.enabled() || knownSourceTopicPartitions.isEmpty()) {
            return Collections.emptyList();
        }
        int numTasks = Math.min(maxTasks, knownSourceTopicPartitions.size());
        List<List<TopicPartition>> roundRobinByTask = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            roundRobinByTask.add(new ArrayList<>());
        }
        int count = 0;
        for (TopicPartition partition : knownSourceTopicPartitions) {
            int index = count % numTasks;
            roundRobinByTask.get(index).add(partition);
            count++;
        }

        return roundRobinByTask.stream().map(config::taskConfigForTopicPartitions)
            .collect(Collectors.toList());
    }

    @Override
    public ConfigDef config() {
        return MirrorConnectorConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1";
    }
}

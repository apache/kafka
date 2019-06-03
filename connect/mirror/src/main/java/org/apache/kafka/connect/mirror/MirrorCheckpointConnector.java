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
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorCheckpointConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorCheckpointConnector.class);

    private Scheduler scheduler;
    private MirrorConnectorConfig config;
    private GroupFilter groupFilter;
    private AdminClient sourceAdminClient;
    private ReplicationPolicy replicationPolicy;
    private SourceAndTarget sourceAndTarget;
    private String connectorName;
    private List<String> knownConsumerGroups = Collections.emptyList();

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorConnectorConfig(props);
        if (!config.enabled()) {
            return;
        }
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        groupFilter = config.groupFilter();
        replicationPolicy = config.replicationPolicy();
        sourceAdminClient = AdminClient.create(config.sourceAdminConfig());
        scheduler = new Scheduler(MirrorCheckpointConnector.class);
        scheduler.execute(this::loadInitialConsumerGroups, "loading initial consumer groups");
        scheduler.scheduleRepeatingDelayed(this::refreshConsumerGroups, config.refreshGroupsInterval(),
            "refreshing consumer groups");
        log.info("Started {} with {} consumer groups.", connectorName, knownConsumerGroups.size());
    }

    @Override
    public void stop() {
        if (!config.enabled()) {
            return;
        }
        scheduler.shutdown();
        synchronized (sourceAdminClient) {
            sourceAdminClient.close();
        } 
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorCheckpointTask.class;
    }

    // divide consumer groups among tasks
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (!config.enabled() || knownConsumerGroups.isEmpty()) {
            return Collections.emptyList();
        }
        int numTasks = Math.min(maxTasks, knownConsumerGroups.size());
        return ConnectorUtils.groupPartitions(knownConsumerGroups, numTasks).stream()
            .map(config::taskConfigForConsumerGroups)
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

    private void refreshConsumerGroups()
            throws InterruptedException, ExecutionException {
        List<String> consumerGroups = findConsumerGroups();
        Set<String> newConsumerGroups = new HashSet<>();
        newConsumerGroups.addAll(consumerGroups);
        newConsumerGroups.removeAll(knownConsumerGroups);
        Set<String> deadConsumerGroups = new HashSet<>();
        deadConsumerGroups.addAll(knownConsumerGroups);
        deadConsumerGroups.removeAll(consumerGroups);
        if (!newConsumerGroups.isEmpty() || !deadConsumerGroups.isEmpty()) {
            log.info("Found {} consumer groups for {}. {} are new. {} were removed. Previously had {}.",
                consumerGroups.size(), sourceAndTarget, newConsumerGroups.size(), deadConsumerGroups.size(),
                knownConsumerGroups.size());
            knownConsumerGroups = consumerGroups;
            context.requestTaskReconfiguration();
        }
    }

    private void loadInitialConsumerGroups()
            throws InterruptedException, ExecutionException {
        knownConsumerGroups = findConsumerGroups();
    }

    private List<String> findConsumerGroups()
            throws InterruptedException, ExecutionException {
        return listConsumerGroups().stream()
                .filter(x -> !x.isSimpleConsumerGroup())
                .map(x -> x.groupId())
                .filter(this::shouldReplicate)
                .collect(Collectors.toList());
    }

    private Collection<ConsumerGroupListing> listConsumerGroups()
            throws InterruptedException, ExecutionException {
        synchronized (sourceAdminClient) {
            return sourceAdminClient.listConsumerGroups().valid().get();
        }
    }

    boolean shouldReplicate(String group) {
        return groupFilter.shouldReplicateGroup(group);
    }
}

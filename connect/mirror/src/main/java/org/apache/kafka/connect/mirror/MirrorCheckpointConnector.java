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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.mirror.Checkpoint.CONSUMER_GROUP_ID_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.TOPIC_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.adminCall;

/** Replicate consumer group state between clusters. Emits checkpoint records.
 *
 *  @see MirrorCheckpointConfig for supported config properties.
 */
public class MirrorCheckpointConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorCheckpointConnector.class);

    private Scheduler scheduler;
    private MirrorCheckpointConfig config;
    private TopicFilter topicFilter;
    private GroupFilter groupFilter;
    private Admin sourceAdminClient;
    private Admin targetAdminClient;
    private SourceAndTarget sourceAndTarget;
    private Set<String> knownConsumerGroups = Collections.emptySet();

    public MirrorCheckpointConnector() {
        // nop
    }

    // visible for testing
    MirrorCheckpointConnector(Set<String> knownConsumerGroups, MirrorCheckpointConfig config) {
        this.knownConsumerGroups = knownConsumerGroups;
        this.config = config;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorCheckpointConfig(props);
        if (!config.enabled()) {
            return;
        }
        String connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        topicFilter = config.topicFilter();
        groupFilter = config.groupFilter();
        sourceAdminClient = config.forwardingAdmin(config.sourceAdminConfig("checkpoint-source-admin"));
        targetAdminClient = config.forwardingAdmin(config.targetAdminConfig("checkpoint-target-admin"));
        scheduler = new Scheduler(getClass(), config.entityLabel(), config.adminTimeout());
        scheduler.execute(this::createInternalTopics, "creating internal topics");
        scheduler.execute(this::loadInitialConsumerGroups, "loading initial consumer groups");
        scheduler.scheduleRepeatingDelayed(this::refreshConsumerGroups, config.refreshGroupsInterval(),
                "refreshing consumer groups");
        log.info("Started {} with {} consumer groups.", connectorName, knownConsumerGroups.size());
        log.debug("Started {} with consumer groups: {}", connectorName, knownConsumerGroups);
    }

    @Override
    public void stop() {
        if (!config.enabled()) {
            return;
        }
        Utils.closeQuietly(scheduler, "scheduler");
        Utils.closeQuietly(topicFilter, "topic filter");
        Utils.closeQuietly(groupFilter, "group filter");
        Utils.closeQuietly(sourceAdminClient, "source admin client");
        Utils.closeQuietly(targetAdminClient, "target admin client");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorCheckpointTask.class;
    }

    // divide consumer groups among tasks
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // if the replication is disabled, known consumer group is empty, or checkpoint emission is
        // disabled by setting 'emit.checkpoints.enabled' to false, the interval of checkpoint emission
        // will be negative and no 'MirrorCheckpointTask' will be created
        if (!config.enabled() || knownConsumerGroups.isEmpty()
                || config.emitCheckpointsInterval().isNegative()) {
            return Collections.emptyList();
        }
        int numTasks = Math.min(maxTasks, knownConsumerGroups.size());
        List<List<String>> groupsPartitioned = ConnectorUtils.groupPartitions(new ArrayList<>(knownConsumerGroups), numTasks);
        return IntStream.range(0, numTasks)
                .mapToObj(i -> config.taskConfigForConsumerGroups(groupsPartitioned.get(i), i))
                .collect(Collectors.toList());
    }

    @Override
    public ConfigDef config() {
        return MirrorCheckpointConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<Map<String, ?>, Map<String, ?>> offsets) {
        for (Map.Entry<Map<String, ?>, Map<String, ?>> offsetEntry : offsets.entrySet()) {
            Map<String, ?> sourceOffset = offsetEntry.getValue();
            if (sourceOffset == null) {
                // We allow tombstones for anything; if there's garbage in the offsets for the connector, we don't
                // want to prevent users from being able to clean it up using the REST API
                continue;
            }

            Map<String, ?> sourcePartition = offsetEntry.getKey();
            if (sourcePartition == null) {
                throw new ConnectException("Source partitions may not be null");
            }

            MirrorUtils.validateSourcePartitionString(sourcePartition, CONSUMER_GROUP_ID_KEY);
            MirrorUtils.validateSourcePartitionString(sourcePartition, TOPIC_KEY);
            MirrorUtils.validateSourcePartitionPartition(sourcePartition);

            MirrorUtils.validateSourceOffset(sourcePartition, sourceOffset, true);
        }

        // We don't actually use these offsets in the task class, so no additional effort is required beyond just validating
        // the format of the user-supplied offsets
        return true;
    }

    private void refreshConsumerGroups()
            throws InterruptedException, ExecutionException {
        Set<String> consumerGroups = findConsumerGroups();
        Set<String> newConsumerGroups = new HashSet<>(consumerGroups);
        newConsumerGroups.removeAll(knownConsumerGroups);
        Set<String> deadConsumerGroups = new HashSet<>(knownConsumerGroups);
        deadConsumerGroups.removeAll(consumerGroups);
        if (!newConsumerGroups.isEmpty() || !deadConsumerGroups.isEmpty()) {
            log.info("Found {} consumer groups for {}. {} are new. {} were removed. Previously had {}.",
                    consumerGroups.size(), sourceAndTarget, newConsumerGroups.size(), deadConsumerGroups.size(),
                    knownConsumerGroups.size());
            log.debug("Found new consumer groups: {}", newConsumerGroups);
            knownConsumerGroups = consumerGroups;
            context.requestTaskReconfiguration();
        }
    }

    private void loadInitialConsumerGroups()
            throws InterruptedException, ExecutionException {
        knownConsumerGroups = findConsumerGroups();
    }

    Set<String> findConsumerGroups()
            throws InterruptedException, ExecutionException {
        List<String> filteredGroups = listConsumerGroups().stream()
                .map(ConsumerGroupListing::groupId)
                .filter(this::shouldReplicateByGroupFilter)
                .collect(Collectors.toList());

        Set<String> checkpointGroups = new HashSet<>();
        Set<String> irrelevantGroups = new HashSet<>();

        for (String group : filteredGroups) {
            Set<String> consumedTopics = listConsumerGroupOffsets(group).keySet().stream()
                    .map(TopicPartition::topic)
                    .filter(this::shouldReplicateByTopicFilter)
                    .collect(Collectors.toSet());
            // Only perform checkpoints for groups that have offsets for at least one topic that's accepted
            // by the topic filter.
            if (consumedTopics.isEmpty()) {
                irrelevantGroups.add(group);
            } else {
                checkpointGroups.add(group);
            }
        }

        log.debug("Ignoring the following groups which do not have any offsets for topics that are accepted by " +
                        "the topic filter: {}", irrelevantGroups);
        return checkpointGroups;
    }

    Collection<ConsumerGroupListing> listConsumerGroups()
            throws InterruptedException, ExecutionException {
        return adminCall(
                () -> sourceAdminClient.listConsumerGroups().valid().get(),
                () -> "list consumer groups on " + config.sourceClusterAlias() + " cluster"
        );
    }

    private void createInternalTopics() {
        MirrorUtils.createSinglePartitionCompactedTopic(
                config.checkpointsTopic(),
                config.checkpointsTopicReplicationFactor(),
                targetAdminClient
        );
    }

    Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String group)
            throws InterruptedException, ExecutionException {
        return adminCall(
                () -> sourceAdminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(),
                () -> String.format("list offsets for consumer group %s on %s cluster", group,
                        config.sourceClusterAlias())
        );
    }

    boolean shouldReplicateByGroupFilter(String group) {
        return groupFilter.shouldReplicateGroup(group);
    }

    boolean shouldReplicateByTopicFilter(String topic) {
        return topicFilter.shouldReplicateTopic(topic);
    }
}

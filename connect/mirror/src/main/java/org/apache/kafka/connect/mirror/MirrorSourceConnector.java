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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsOptions;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replicate data, configuration, and ACLs between clusters.
 *
 *  @see MirrorConnectorConfig for supported config properties.
 */
public class MirrorSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceConnector.class);
    private static final ResourcePatternFilter ANY_TOPIC = new ResourcePatternFilter(ResourceType.TOPIC,
        null, PatternType.ANY);
    private static final AclBindingFilter ANY_TOPIC_ACL = new AclBindingFilter(ANY_TOPIC, AccessControlEntryFilter.ANY);

    private Scheduler scheduler;
    private MirrorConnectorConfig config;
    private SourceAndTarget sourceAndTarget;
    private String connectorName;
    private TopicFilter topicFilter;
    private ConfigPropertyFilter configPropertyFilter;
    private List<TopicPartition> knownTopicPartitions = Collections.emptyList();
    private Set<String> knownTargetTopics = Collections.emptySet();
    private ReplicationPolicy replicationPolicy;
    private int replicationFactor;
    private AdminClient sourceAdminClient;
    private AdminClient targetAdminClient;

    public MirrorSourceConnector() {
        // nop
    }

    // visible for testing
    MirrorSourceConnector(SourceAndTarget sourceAndTarget, ReplicationPolicy replicationPolicy,
            TopicFilter topicFilter, ConfigPropertyFilter configPropertyFilter) {
        this.sourceAndTarget = sourceAndTarget;
        this.replicationPolicy = replicationPolicy;
        this.topicFilter = topicFilter;
        this.configPropertyFilter = configPropertyFilter;
    } 

    @Override
    public void start(Map<String, String> props) {
        long start = System.currentTimeMillis();
        config = new MirrorConnectorConfig(props);
        if (!config.enabled()) {
            return;
        }
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        topicFilter = config.topicFilter();
        configPropertyFilter = config.configPropertyFilter();
        replicationPolicy = config.replicationPolicy();
        replicationFactor = config.replicationFactor();
        sourceAdminClient = AdminClient.create(config.sourceAdminConfig());
        targetAdminClient = AdminClient.create(config.targetAdminConfig());
        scheduler = new Scheduler(MirrorSourceConnector.class, config.adminTimeout());
        scheduler.execute(this::createOffsetSyncsTopic, "creating upstream offset-syncs topic");
        scheduler.execute(this::loadTopicPartitions, "loading initial set of topic-partitions");
        scheduler.execute(this::createTopicPartitions, "creating downstream topic-partitions");
        scheduler.execute(this::refreshKnownTargetTopics, "refreshing known target topics");
        scheduler.scheduleRepeating(this::syncTopicAcls, config.syncTopicAclsInterval(), "syncing topic ACLs");
        scheduler.scheduleRepeating(this::syncTopicConfigs, config.syncTopicConfigsInterval(),
            "syncing topic configs");
        scheduler.scheduleRepeatingDelayed(this::refreshTopicPartitions, config.refreshTopicsInterval(),
            "refreshing topics");
        log.info("Started {} with {} topic-partitions.", connectorName, knownTopicPartitions.size());
        log.info("Starting {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        if (!config.enabled()) {
            return;
        }
        Utils.closeQuietly(scheduler, "scheduler");
        Utils.closeQuietly(topicFilter, "topic filter");
        Utils.closeQuietly(configPropertyFilter, "config property filter");
        Utils.closeQuietly(sourceAdminClient, "source admin client");
        Utils.closeQuietly(targetAdminClient, "target admin client");
        log.info("Stopping {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorSourceTask.class;
    }

    // divide topic-partitions among tasks
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (!config.enabled() || knownTopicPartitions.isEmpty()) {
            return Collections.emptyList();
        }
        int numTasks = Math.min(maxTasks, knownTopicPartitions.size());
        return ConnectorUtils.groupPartitions(knownTopicPartitions, numTasks).stream()
            .map(config::taskConfigForTopicPartitions)
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

    private List<TopicPartition> findTopicPartitions()
            throws InterruptedException, ExecutionException {
        Set<String> topics = listTopics(sourceAdminClient).stream()
            .filter(this::shouldReplicateTopic)
            .collect(Collectors.toSet());
        return describeTopics(topics).stream()
            .flatMap(MirrorSourceConnector::expandTopicDescription)
            .collect(Collectors.toList());
    }

    private void refreshTopicPartitions()
            throws InterruptedException, ExecutionException {
        List<TopicPartition> topicPartitions = findTopicPartitions();
        Set<TopicPartition> newTopicPartitions = new HashSet<>();
        newTopicPartitions.addAll(topicPartitions);
        newTopicPartitions.removeAll(knownTopicPartitions);
        Set<TopicPartition> deadTopicPartitions = new HashSet<>();
        deadTopicPartitions.addAll(knownTopicPartitions);
        deadTopicPartitions.removeAll(topicPartitions);
        if (!newTopicPartitions.isEmpty() || !deadTopicPartitions.isEmpty()) {
            log.info("Found {} topic-partitions on {}. {} are new. {} were removed. Previously had {}.",
                    topicPartitions.size(), sourceAndTarget.source(), newTopicPartitions.size(), 
                    deadTopicPartitions.size(), knownTopicPartitions.size());
            log.trace("Found new topic-partitions: {}", newTopicPartitions);
            knownTopicPartitions = topicPartitions;
            knownTargetTopics = findExistingTargetTopics(); 
            createTopicPartitions();
            context.requestTaskReconfiguration();
        }
    }

    private void loadTopicPartitions()
            throws InterruptedException, ExecutionException {
        knownTopicPartitions = findTopicPartitions();
        knownTargetTopics = findExistingTargetTopics(); 
    }

    private void refreshKnownTargetTopics()
            throws InterruptedException, ExecutionException {
        knownTargetTopics = findExistingTargetTopics();
    }

    private Set<String> findExistingTargetTopics()
            throws InterruptedException, ExecutionException {
        return listTopics(targetAdminClient).stream()
            .filter(x -> sourceAndTarget.source().equals(replicationPolicy.topicSource(x)))
            .collect(Collectors.toSet());
    }

    private Set<String> topicsBeingReplicated() {
        return knownTopicPartitions.stream()
            .map(x -> x.topic())
            .distinct()
            .filter(x -> knownTargetTopics.contains(formatRemoteTopic(x)))
            .collect(Collectors.toSet());
    }

    private void syncTopicAcls()
            throws InterruptedException, ExecutionException {
        List<AclBinding> bindings = listTopicAclBindings().stream()
            .filter(x -> x.pattern().resourceType() == ResourceType.TOPIC)
            .filter(x -> x.pattern().patternType() == PatternType.LITERAL)
            .filter(this::shouldReplicateAcl)
            .filter(x -> shouldReplicateTopic(x.pattern().name()))
            .map(this::targetAclBinding)
            .collect(Collectors.toList());
        updateTopicAcls(bindings);
    }

    private void syncTopicConfigs()
            throws InterruptedException, ExecutionException {
        Map<String, Config> sourceConfigs = describeTopicConfigs(topicsBeingReplicated());
        Map<String, Config> targetConfigs = sourceConfigs.entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> targetConfig(x.getValue())));
        updateTopicConfigs(targetConfigs);
    }

    private void createOffsetSyncsTopic() {
        MirrorUtils.createSinglePartitionCompactedTopic(config.offsetSyncsTopic(), config.offsetSyncsTopicReplicationFactor(), config.sourceAdminConfig());
    }

    private void createTopicPartitions()
            throws InterruptedException, ExecutionException {
        Map<String, Long> partitionCounts = knownTopicPartitions.stream()
            .collect(Collectors.groupingBy(x -> x.topic(), Collectors.counting())).entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> x.getValue()));
        List<NewTopic> newTopics = partitionCounts.entrySet().stream()
            .filter(x -> !knownTargetTopics.contains(x.getKey()))
            .map(x -> new NewTopic(x.getKey(), x.getValue().intValue(), (short) replicationFactor))
            .collect(Collectors.toList());
        Map<String, NewPartitions> newPartitions = partitionCounts.entrySet().stream()
            .filter(x -> knownTargetTopics.contains(x.getKey()))
            .collect(Collectors.toMap(x -> x.getKey(), x -> NewPartitions.increaseTo(x.getValue().intValue())));
        targetAdminClient.createTopics(newTopics, new CreateTopicsOptions()).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not create topic {}.", k, e);
            } else {
                log.info("Created remote topic {} with {} partitions.", k, partitionCounts.get(k));
            }
        }));
        targetAdminClient.createPartitions(newPartitions).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e instanceof InvalidPartitionsException) {
                // swallow, this is normal
            } else if (e != null) {
                log.warn("Could not create topic-partitions for {}.", k, e);
            } else {
                log.info("Increased size of {} to {} partitions.", k, partitionCounts.get(k));
            }
        }));
    }

    private Set<String> listTopics(AdminClient adminClient)
            throws InterruptedException, ExecutionException {
        return adminClient.listTopics().names().get();
    }

    private Collection<AclBinding> listTopicAclBindings()
            throws InterruptedException, ExecutionException {
        return sourceAdminClient.describeAcls(ANY_TOPIC_ACL).values().get();
    }

    private Collection<TopicDescription> describeTopics(Collection<String> topics)
            throws InterruptedException, ExecutionException {
        return sourceAdminClient.describeTopics(topics).all().get().values();
    }

    @SuppressWarnings("deprecation")
    // use deprecated alterConfigs API for broker compatibility back to 0.11.0
    private void updateTopicConfigs(Map<String, Config> topicConfigs)
            throws InterruptedException, ExecutionException {
        Map<ConfigResource, Config> configs = topicConfigs.entrySet().stream()
            .collect(Collectors.toMap(x ->
                new ConfigResource(ConfigResource.Type.TOPIC, x.getKey()), x -> x.getValue()));
        log.trace("Syncing configs for {} topics.", configs.size());
        targetAdminClient.alterConfigs(configs).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not alter configuration of topic {}.", k.name(), e);
            }
        }));
    }

    private void updateTopicAcls(List<AclBinding> bindings)
            throws InterruptedException, ExecutionException {
        log.trace("Syncing {} topic ACL bindings.", bindings.size());
        targetAdminClient.createAcls(bindings).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not sync ACL of topic {}.", k.pattern().name(), e);
            }
        }));
    }

    private static Stream<TopicPartition> expandTopicDescription(TopicDescription description) {
        String topic = description.name();
        return description.partitions().stream()
            .map(x -> new TopicPartition(topic, x.partition()));
    }

    private Map<String, Config> describeTopicConfigs(Set<String> topics)
            throws InterruptedException, ExecutionException {
        Set<ConfigResource> resources = topics.stream()
            .map(x -> new ConfigResource(ConfigResource.Type.TOPIC, x))
            .collect(Collectors.toSet());
        return sourceAdminClient.describeConfigs(resources).all().get().entrySet().stream()
            .collect(Collectors.toMap(x -> x.getKey().name(), x -> x.getValue()));
    }

    Config targetConfig(Config sourceConfig) {
        List<ConfigEntry> entries = sourceConfig.entries().stream()
            .filter(x -> !x.isDefault() && !x.isReadOnly() && !x.isSensitive())
            .filter(x -> x.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
            .filter(x -> shouldReplicateTopicConfigurationProperty(x.name()))
            .collect(Collectors.toList());
        return new Config(entries);
    }

    private static AccessControlEntry downgradeAllowAllACL(AccessControlEntry entry) {
        return new AccessControlEntry(entry.principal(), entry.host(), AclOperation.READ, entry.permissionType());
    }

    AclBinding targetAclBinding(AclBinding sourceAclBinding) {
        String targetTopic = formatRemoteTopic(sourceAclBinding.pattern().name());
        final AccessControlEntry entry;
        if (sourceAclBinding.entry().permissionType() == AclPermissionType.ALLOW
                && sourceAclBinding.entry().operation() == AclOperation.ALL) {
            entry = downgradeAllowAllACL(sourceAclBinding.entry());
        } else {
            entry = sourceAclBinding.entry();
        }
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, targetTopic, PatternType.LITERAL), entry);
    }

    boolean shouldReplicateTopic(String topic) {
        return (topicFilter.shouldReplicateTopic(topic) || isHeartbeatTopic(topic))
            && !replicationPolicy.isInternalTopic(topic) && !isCycle(topic);
    }

    boolean shouldReplicateAcl(AclBinding aclBinding) {
        return !(aclBinding.entry().permissionType() == AclPermissionType.ALLOW
            && aclBinding.entry().operation() == AclOperation.WRITE);
    }

    boolean shouldReplicateTopicConfigurationProperty(String property) {
        return configPropertyFilter.shouldReplicateConfigProperty(property);
    }

    // Recurse upstream to detect cycles, i.e. whether this topic is already on the target cluster
    boolean isCycle(String topic) {
        String source = replicationPolicy.topicSource(topic);
        if (source == null) {
            return false;
        } else if (source.equals(sourceAndTarget.target())) {
            return true;
        } else {
            return isCycle(replicationPolicy.upstreamTopic(topic));
        }
    }

    // e.g. heartbeats, us-west.heartbeats
    boolean isHeartbeatTopic(String topic) {
        return MirrorClientConfig.HEARTBEATS_TOPIC.equals(replicationPolicy.originalTopic(topic));
    }

    String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceAndTarget.source(), topic);
    }
}

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
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

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
    private ReplicationPolicy replicationPolicy;
    private AdminClient sourceAdminClient;
    private AdminClient targetAdminClient;

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorConnectorConfig(props);
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        topicFilter = config.topicFilter();
        configPropertyFilter = config.configPropertyFilter();
        replicationPolicy = config.replicationPolicy();
        sourceAdminClient = AdminClient.create(config.sourceAdminConfig());
        targetAdminClient = AdminClient.create(config.targetAdminConfig());
        log.info("Starting {} for {}.", connectorName, sourceAndTarget);
        scheduler = new Scheduler(MirrorSourceConnector.class);
        scheduler.execute(this::loadTopicPartitions, "loading initial set of topic-partitions");
        scheduler.scheduleRepeating(this::syncTopicAcls, config.syncTopicAclsInterval(), "syncing topic ACLs");
        scheduler.scheduleRepeating(this::syncTopicConfigs, config.syncTopicConfigsInterval(), "syncing topic configs");
        scheduler.scheduleRepeatingDelayed(this::refreshTopicPartitions, config.refreshTopicsInterval(),
            "refreshing topics");
        log.info("Started {} with {} topic-partitions.", connectorName, knownTopicPartitions.size());
    }

    @Override
    public void stop() {
        log.info("Stopping {}.", connectorName);
        scheduler.shutdown();
        synchronized (sourceAdminClient) {
            sourceAdminClient.close();
        }
        synchronized (targetAdminClient) {
            targetAdminClient.close();
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorSourceTask.class;
    }

    // divide topic-partitions among tasks
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (knownTopicPartitions.isEmpty()) {
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
        return "WIP";
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
            knownTopicPartitions = topicPartitions;
            context.requestTaskReconfiguration();
        }
    }

    private void loadTopicPartitions()
            throws InterruptedException, ExecutionException {
        knownTopicPartitions = findTopicPartitions();
    }

    Set<String> topicsToReplicate() {
        return knownTopicPartitions.stream()
            .map(x -> x.topic())
            .distinct()
            .collect(Collectors.toSet());
    }

    private void syncTopicAcls()
            throws InterruptedException, ExecutionException {
        List<AclBinding> bindings = listTopicAclBindings().stream()
            .filter(x -> x.pattern().resourceType() == ResourceType.TOPIC)
            .filter(x -> x.pattern().patternType() == PatternType.LITERAL)
            .filter(x -> shouldReplicateTopic(x.pattern().name()))
            .map(this::targetAclBinding)
            .collect(Collectors.toList());
        updateTopicAcls(bindings);
    }

    private void syncTopicConfigs()
            throws InterruptedException, ExecutionException {
        Map<String, Config> sourceConfigs = describeTopicConfigs(topicsToReplicate());
        Map<String, Config> targetConfigs = sourceConfigs.entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> targetConfig(x.getValue())));
        updateTopicConfigs(targetConfigs);
    }

    private static Set<String> listTopics(AdminClient adminClient)
            throws InterruptedException, ExecutionException {
        synchronized (adminClient) {
            return adminClient.listTopics().names().get();
        }
    }

    private Collection<AclBinding> listTopicAclBindings()
            throws InterruptedException, ExecutionException {
        synchronized (sourceAdminClient) {
            return sourceAdminClient.describeAcls(ANY_TOPIC_ACL).values().get();
        }
    }

    private Collection<TopicDescription> describeTopics(Collection<String> topics)
            throws InterruptedException, ExecutionException {
        synchronized (sourceAdminClient) {
            return sourceAdminClient.describeTopics(topics).all().get().values();
        }
    }

    private void updateTopicConfigs(Map<String, Config> topicConfigs)
            throws InterruptedException, ExecutionException {
        Map<ConfigResource, Config> configs = topicConfigs.entrySet().stream()
            .filter(x -> shouldReplicateTopicConfigurationProperty(x.getKey()))
            .collect(Collectors.toMap(x -> new ConfigResource(ConfigResource.Type.TOPIC, x.getKey()), x -> x.getValue()));
        log.info("Syncing configs for {} topics.", configs.size());
        synchronized (targetAdminClient) {
            targetAdminClient.alterConfigs(configs).values().forEach((k, v) -> v.whenComplete((x, e) -> {
                if (e != null) {
                    log.warn("Could not alter configuration of topic {}.", k.name(), e);
                }
            }));
        }
    }

    private void updateTopicAcls(List<AclBinding> bindings)
            throws InterruptedException, ExecutionException {
        log.info("Syncing {} topic ACL bindings.", bindings.size());
        synchronized (targetAdminClient) {
            targetAdminClient.createAcls(bindings).values().forEach((k, v) -> v.whenComplete((x, e) -> {
                if (e != null) {
                    log.warn("Could not sync ACL of topic {}.", k.pattern().name(), e);
                }
            }));
        }
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
        synchronized (sourceAdminClient) {
            return sourceAdminClient.describeConfigs(resources).all().get().entrySet().stream()
                .collect(Collectors.toMap(x -> x.getKey().name(), x -> x.getValue()));
        }
    }

    Config targetConfig(Config sourceConfig) {
        List<ConfigEntry> entries = sourceConfig.entries().stream()
            .filter(x -> !x.isDefault() && !x.isReadOnly() && !x.isSensitive())
            .collect(Collectors.toList());
        return new Config(entries);
    }

    AclBinding targetAclBinding(AclBinding sourceAclBinding) {
        String targetTopic = formatRemoteTopic(sourceAclBinding.pattern().name());
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, targetTopic, PatternType.LITERAL),
            sourceAclBinding.entry());
    }

    boolean shouldReplicateTopic(String topic) {
        return topicFilter.shouldReplicateTopic(topic) && !isCycle(topic);
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

    String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceAndTarget.source(), topic);
    }
}

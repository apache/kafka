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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.OFFSET_SYNCS_CLIENT_ROLE_PREFIX;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.OFFSET_SYNCS_TOPIC_CONFIG_PREFIX;
import static org.apache.kafka.connect.mirror.MirrorSourceConfig.SYNC_TOPIC_ACLS_ENABLED;
import static org.apache.kafka.connect.mirror.MirrorUtils.SOURCE_CLUSTER_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.TOPIC_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.adminCall;

/** Replicate data, configuration, and ACLs between clusters.
 *
 *  @see MirrorSourceConfig for supported config properties.
 */
public class MirrorSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceConnector.class);
    private static final ResourcePatternFilter ANY_TOPIC = new ResourcePatternFilter(ResourceType.TOPIC,
        null, PatternType.ANY);
    private static final AclBindingFilter ANY_TOPIC_ACL = new AclBindingFilter(ANY_TOPIC, AccessControlEntryFilter.ANY);
    private static final String READ_COMMITTED = IsolationLevel.READ_COMMITTED.toString();
    private static final String EXACTLY_ONCE_SUPPORT_CONFIG = "exactly.once.support";

    private final AtomicBoolean noAclAuthorizer = new AtomicBoolean(false);

    private Scheduler scheduler;
    private MirrorSourceConfig config;
    private SourceAndTarget sourceAndTarget;
    private String connectorName;
    private TopicFilter topicFilter;
    private ConfigPropertyFilter configPropertyFilter;
    private List<TopicPartition> knownSourceTopicPartitions = Collections.emptyList();
    private List<TopicPartition> knownTargetTopicPartitions = Collections.emptyList();
    private ReplicationPolicy replicationPolicy;
    private int replicationFactor;
    private Admin sourceAdminClient;
    private Admin targetAdminClient;
    private boolean heartbeatsReplicationEnabled;

    public MirrorSourceConnector() {
        // nop
    }

    // visible for testing
    MirrorSourceConnector(List<TopicPartition> knownSourceTopicPartitions, MirrorSourceConfig config) {
        this.knownSourceTopicPartitions = knownSourceTopicPartitions;
        this.config = config;
    }

    // visible for testing
    MirrorSourceConnector(SourceAndTarget sourceAndTarget, ReplicationPolicy replicationPolicy,
            TopicFilter topicFilter, ConfigPropertyFilter configPropertyFilter) {
        this(sourceAndTarget, replicationPolicy, topicFilter, configPropertyFilter, true);
    }

    // visible for testing
    MirrorSourceConnector(SourceAndTarget sourceAndTarget, ReplicationPolicy replicationPolicy,
            TopicFilter topicFilter, ConfigPropertyFilter configPropertyFilter, boolean heartbeatsReplicationEnabled) {
        this.sourceAndTarget = sourceAndTarget;
        this.replicationPolicy = replicationPolicy;
        this.topicFilter = topicFilter;
        this.configPropertyFilter = configPropertyFilter;
        this.heartbeatsReplicationEnabled = heartbeatsReplicationEnabled;
    }

    // visible for testing
    MirrorSourceConnector(Admin sourceAdminClient, Admin targetAdminClient, MirrorSourceConfig config) {
        this.sourceAdminClient = sourceAdminClient;
        this.targetAdminClient = targetAdminClient;
        this.config = config;
    }

    @Override
    public void start(Map<String, String> props) {
        long start = System.currentTimeMillis();
        config = new MirrorSourceConfig(props);
        if (!config.enabled()) {
            return;
        }
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        topicFilter = config.topicFilter();
        configPropertyFilter = config.configPropertyFilter();
        replicationPolicy = config.replicationPolicy();
        replicationFactor = config.replicationFactor();
        sourceAdminClient = config.forwardingAdmin(config.sourceAdminConfig("replication-source-admin"));
        targetAdminClient = config.forwardingAdmin(config.targetAdminConfig("replication-target-admin"));
        heartbeatsReplicationEnabled = config.heartbeatsReplicationEnabled();

        scheduler = new Scheduler(getClass(), config.entityLabel(), config.adminTimeout());
        scheduler.execute(this::createOffsetSyncsTopic, "creating upstream offset-syncs topic");
        scheduler.execute(this::loadTopicPartitions, "loading initial set of topic-partitions");
        scheduler.execute(this::computeAndCreateTopicPartitions, "creating downstream topic-partitions");
        scheduler.execute(this::refreshKnownTargetTopics, "refreshing known target topics");
        scheduler.scheduleRepeating(this::syncTopicAcls, config.syncTopicAclsInterval(), "syncing topic ACLs");
        scheduler.scheduleRepeating(this::syncTopicConfigs, config.syncTopicConfigsInterval(),
            "syncing topic configs");
        scheduler.scheduleRepeatingDelayed(this::refreshTopicPartitions, config.refreshTopicsInterval(),
            "refreshing topics");
        log.info("Started {} with {} topic-partitions.", connectorName, knownSourceTopicPartitions.size());
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
        return IntStream.range(0, numTasks)
                .mapToObj(i -> config.taskConfigForTopicPartitions(roundRobinByTask.get(i), i))
                .collect(Collectors.toList());
    }

    @Override
    public ConfigDef config() {
        return MirrorSourceConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public org.apache.kafka.common.config.Config validate(Map<String, String> props) {
        List<ConfigValue> configValues = super.validate(props).configValues();
        validateExactlyOnceConfigs(props, configValues);
        validateEmitOffsetSyncConfigs(props, configValues);

        return new org.apache.kafka.common.config.Config(configValues);
    }

    private static void validateEmitOffsetSyncConfigs(Map<String, String> props, List<ConfigValue> configValues) {
        boolean offsetSyncsConfigured = props.keySet().stream()
                .anyMatch(conf -> conf.startsWith(OFFSET_SYNCS_CLIENT_ROLE_PREFIX) || conf.startsWith(OFFSET_SYNCS_TOPIC_CONFIG_PREFIX));

        if ("false".equals(props.get(MirrorConnectorConfig.EMIT_OFFSET_SYNCS_ENABLED)) && offsetSyncsConfigured) {
            ConfigValue emitOffsetSyncs = configValues.stream().filter(prop -> MirrorConnectorConfig.EMIT_OFFSET_SYNCS_ENABLED.equals(prop.name()))
                    .findAny()
                    .orElseGet(() -> {
                        ConfigValue result = new ConfigValue(MirrorConnectorConfig.EMIT_OFFSET_SYNCS_ENABLED);
                        configValues.add(result);
                        return result;
                    });
            emitOffsetSyncs.addErrorMessage("MirrorSourceConnector can't setup offset-syncs feature while " +
                    MirrorConnectorConfig.EMIT_OFFSET_SYNCS_ENABLED + " set to false");
        }
    }

    private void validateExactlyOnceConfigs(Map<String, String> props, List<ConfigValue> configValues) {
        if ("required".equals(props.get(EXACTLY_ONCE_SUPPORT_CONFIG))) {
            if (!consumerUsesReadCommitted(props)) {
                ConfigValue exactlyOnceSupport = configValues.stream()
                        .filter(cv -> EXACTLY_ONCE_SUPPORT_CONFIG.equals(cv.name()))
                        .findAny()
                        .orElseGet(() -> {
                            ConfigValue result = new ConfigValue(EXACTLY_ONCE_SUPPORT_CONFIG);
                            configValues.add(result);
                            return result;
                        });
                // The Connect framework will already generate an error for this property if we return ExactlyOnceSupport.UNSUPPORTED
                // from our exactlyOnceSupport method, but it will be fairly generic
                // We add a second error message here to give users more insight into why this specific connector can't support exactly-once
                // guarantees with the given configuration
                exactlyOnceSupport.addErrorMessage(
                        "MirrorSourceConnector can only provide exactly-once guarantees when its source consumer is configured with "
                                + ConsumerConfig.ISOLATION_LEVEL_CONFIG + " set to '" + READ_COMMITTED + "'; "
                                + "otherwise, records from aborted and uncommitted transactions will be replicated from the "
                                + "source cluster to the target cluster."
                );
            }
        }
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        return consumerUsesReadCommitted(props)
                ? ExactlyOnceSupport.SUPPORTED
                : ExactlyOnceSupport.UNSUPPORTED;
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

            MirrorUtils.validateSourcePartitionString(sourcePartition, SOURCE_CLUSTER_KEY);
            MirrorUtils.validateSourcePartitionString(sourcePartition, TOPIC_KEY);
            MirrorUtils.validateSourcePartitionPartition(sourcePartition);

            MirrorUtils.validateSourceOffset(sourcePartition, sourceOffset, false);
        }

        // We never commit offsets with our source consumer, so no additional effort is required beyond just validating
        // the format of the user-supplied offsets
        return true;
    }

    private boolean consumerUsesReadCommitted(Map<String, String> props) {
        Object consumerIsolationLevel = MirrorSourceConfig.sourceConsumerConfig(props)
                .get(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        return Objects.equals(READ_COMMITTED, consumerIsolationLevel);
    }

    // visible for testing
    List<TopicPartition> findSourceTopicPartitions()
            throws InterruptedException, ExecutionException {
        Set<String> topics = listTopics(sourceAdminClient).stream()
            .filter(this::shouldReplicateTopic)
            .collect(Collectors.toSet());
        return describeTopics(sourceAdminClient, topics).stream()
            .flatMap(MirrorSourceConnector::expandTopicDescription)
            .collect(Collectors.toList());
    }

    // visible for testing
    List<TopicPartition> findTargetTopicPartitions()
            throws InterruptedException, ExecutionException {
        Set<String> topics = listTopics(targetAdminClient).stream()
            .filter(t -> sourceAndTarget.source().equals(replicationPolicy.topicSource(t)))
            .filter(t -> !t.equals(config.checkpointsTopic()))
            .collect(Collectors.toSet());
        return describeTopics(targetAdminClient, topics).stream()
                .flatMap(MirrorSourceConnector::expandTopicDescription)
                .collect(Collectors.toList());
    }

    // visible for testing
    void refreshTopicPartitions()
            throws InterruptedException, ExecutionException {

        List<TopicPartition> sourceTopicPartitions = findSourceTopicPartitions();
        List<TopicPartition> targetTopicPartitions = findTargetTopicPartitions();

        Set<TopicPartition> sourceTopicPartitionsSet = new HashSet<>(sourceTopicPartitions);
        Set<TopicPartition> knownSourceTopicPartitionsSet = new HashSet<>(knownSourceTopicPartitions);

        Set<TopicPartition> upstreamTargetTopicPartitions = targetTopicPartitions.stream()
                .map(x -> new TopicPartition(replicationPolicy.upstreamTopic(x.topic()), x.partition()))
                .collect(Collectors.toSet());

        Set<TopicPartition> missingInTarget = new HashSet<>(sourceTopicPartitions);
        missingInTarget.removeAll(upstreamTargetTopicPartitions);

        knownTargetTopicPartitions = targetTopicPartitions;

        // Detect if topic-partitions were added or deleted from the source cluster
        // or if topic-partitions are missing from the target cluster
        if (!knownSourceTopicPartitionsSet.equals(sourceTopicPartitionsSet) || !missingInTarget.isEmpty()) {

            Set<TopicPartition> newTopicPartitions = new HashSet<>(sourceTopicPartitions);
            newTopicPartitions.removeAll(knownSourceTopicPartitionsSet);

            Set<TopicPartition> deletedTopicPartitions = knownSourceTopicPartitionsSet;
            deletedTopicPartitions.removeAll(sourceTopicPartitionsSet);

            log.info("Found {} new topic-partitions on {}. " +
                     "Found {} deleted topic-partitions on {}. " +
                     "Found {} topic-partitions missing on {}.",
                     newTopicPartitions.size(), sourceAndTarget.source(),
                     deletedTopicPartitions.size(), sourceAndTarget.source(),
                     missingInTarget.size(), sourceAndTarget.target());

            log.trace("Found new topic-partitions on {}: {}", sourceAndTarget.source(), newTopicPartitions);
            log.trace("Found deleted topic-partitions on {}: {}", sourceAndTarget.source(), deletedTopicPartitions);
            log.trace("Found missing topic-partitions on {}: {}", sourceAndTarget.target(), missingInTarget);

            knownSourceTopicPartitions = sourceTopicPartitions;
            computeAndCreateTopicPartitions();
            context.requestTaskReconfiguration();
        }
    }

    private void loadTopicPartitions()
            throws InterruptedException, ExecutionException {
        knownSourceTopicPartitions = findSourceTopicPartitions();
        knownTargetTopicPartitions = findTargetTopicPartitions();
    }

    private void refreshKnownTargetTopics()
            throws InterruptedException, ExecutionException {
        knownTargetTopicPartitions = findTargetTopicPartitions();
    }

    private Set<String> topicsBeingReplicated() {
        Set<String> knownTargetTopics = toTopics(knownTargetTopicPartitions);
        return knownSourceTopicPartitions.stream()
            .map(TopicPartition::topic)
            .distinct()
            .filter(x -> knownTargetTopics.contains(formatRemoteTopic(x)))
            .collect(Collectors.toSet());
    }

    private Set<String> toTopics(Collection<TopicPartition> tps) {
        return tps.stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toSet());
    }

    // Visible for testing
    void syncTopicAcls()
            throws InterruptedException, ExecutionException {
        Optional<Collection<AclBinding>> rawBindings = listTopicAclBindings();
        if (!rawBindings.isPresent())
            return;
        List<AclBinding> filteredBindings = rawBindings.get().stream()
            .filter(x -> x.pattern().resourceType() == ResourceType.TOPIC)
            .filter(x -> x.pattern().patternType() == PatternType.LITERAL)
            .filter(this::shouldReplicateAcl)
            .filter(x -> shouldReplicateTopic(x.pattern().name()))
            .map(this::targetAclBinding)
            .collect(Collectors.toList());
        updateTopicAcls(filteredBindings);
    }

    // visible for testing
    void syncTopicConfigs()
            throws InterruptedException, ExecutionException {
        Map<String, Config> sourceConfigs = describeTopicConfigs(topicsBeingReplicated());
        Map<String, Config> targetConfigs = sourceConfigs.entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> targetConfig(x.getValue(), true)));
        incrementalAlterConfigs(targetConfigs);
    }

    private void createOffsetSyncsTopic() {
        if (config.emitOffsetSyncsEnabled()) {
            try (Admin offsetSyncsAdminClient = config.forwardingAdmin(config.offsetSyncsTopicAdminConfig())) {
                MirrorUtils.createSinglePartitionCompactedTopic(
                        config.offsetSyncsTopic(),
                        config.offsetSyncsTopicReplicationFactor(),
                        offsetSyncsAdminClient
                );
            }
        }
    }

    void computeAndCreateTopicPartitions() throws ExecutionException, InterruptedException {
        // get source and target topics with respective partition counts
        Map<String, Long> sourceTopicToPartitionCounts = knownSourceTopicPartitions.stream()
                .collect(Collectors.groupingBy(TopicPartition::topic, Collectors.counting())).entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Map<String, Long> targetTopicToPartitionCounts = knownTargetTopicPartitions.stream()
                .collect(Collectors.groupingBy(TopicPartition::topic, Collectors.counting())).entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        Set<String> knownSourceTopics = sourceTopicToPartitionCounts.keySet();
        Set<String> knownTargetTopics = targetTopicToPartitionCounts.keySet();
        Map<String, String> sourceToRemoteTopics = knownSourceTopics.stream()
                .collect(Collectors.toMap(Function.identity(), this::formatRemoteTopic));

        // compute existing and new source topics
        Map<Boolean, Set<String>> partitionedSourceTopics = knownSourceTopics.stream()
                .collect(Collectors.partitioningBy(sourceTopic -> knownTargetTopics.contains(sourceToRemoteTopics.get(sourceTopic)),
                        Collectors.toSet()));
        Set<String> existingSourceTopics = partitionedSourceTopics.get(true);
        Set<String> newSourceTopics = partitionedSourceTopics.get(false);

        // create new topics
        if (!newSourceTopics.isEmpty())
            createNewTopics(newSourceTopics, sourceTopicToPartitionCounts);

        // compute topics with new partitions
        Map<String, Long> sourceTopicsWithNewPartitions = existingSourceTopics.stream()
                .filter(sourceTopic -> {
                    String targetTopic = sourceToRemoteTopics.get(sourceTopic);
                    return sourceTopicToPartitionCounts.get(sourceTopic) > targetTopicToPartitionCounts.get(targetTopic);
                })
                .collect(Collectors.toMap(Function.identity(), sourceTopicToPartitionCounts::get));

        // create new partitions
        if (!sourceTopicsWithNewPartitions.isEmpty()) {
            Map<String, NewPartitions> newTargetPartitions = sourceTopicsWithNewPartitions.entrySet().stream()
                    .collect(Collectors.toMap(sourceTopicAndPartitionCount -> sourceToRemoteTopics.get(sourceTopicAndPartitionCount.getKey()),
                        sourceTopicAndPartitionCount -> NewPartitions.increaseTo(sourceTopicAndPartitionCount.getValue().intValue())));
            createNewPartitions(newTargetPartitions);
        }
    }

    // visible for testing
    void createNewTopics(Set<String> newSourceTopics, Map<String, Long> sourceTopicToPartitionCounts)
            throws ExecutionException, InterruptedException {
        Map<String, Config> sourceTopicToConfig = describeTopicConfigs(newSourceTopics);
        Map<String, NewTopic> newTopics = newSourceTopics.stream()
                .map(sourceTopic -> {
                    String remoteTopic = formatRemoteTopic(sourceTopic);
                    int partitionCount = sourceTopicToPartitionCounts.get(sourceTopic).intValue();
                    Map<String, String> configs = configToMap(targetConfig(sourceTopicToConfig.get(sourceTopic), false));
                    return new NewTopic(remoteTopic, partitionCount, (short) replicationFactor)
                            .configs(configs);
                })
                .collect(Collectors.toMap(NewTopic::name, Function.identity()));
        createNewTopics(newTopics);
    }

    // visible for testing
    void createNewTopics(Map<String, NewTopic> newTopics) throws ExecutionException, InterruptedException {
        adminCall(
                () -> {
                    targetAdminClient.createTopics(newTopics.values(), new CreateTopicsOptions()).values()
                            .forEach((k, v) -> v.whenComplete((x, e) -> {
                                if (e != null) {
                                    log.warn("Could not create topic {}.", k, e);
                                } else {
                                    log.info("Created remote topic {} with {} partitions.", k, newTopics.get(k).numPartitions());
                                }
                            }));
                    return null;
                },
                () -> String.format("create topics %s on %s cluster", newTopics, config.targetClusterAlias())
        );
    }

    void createNewPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException {
        adminCall(
                () -> {
                    targetAdminClient.createPartitions(newPartitions).values().forEach((k, v) -> v.whenComplete((x, e) -> {
                        if (e instanceof InvalidPartitionsException) {
                            // swallow, this is normal
                        } else if (e != null) {
                            log.warn("Could not create topic-partitions for {}.", k, e);
                        } else {
                            log.info("Increased size of {} to {} partitions.", k, newPartitions.get(k).totalCount());
                        }
                    }));
                    return null;
                },
                () -> String.format("create partitions %s on %s cluster", newPartitions, config.targetClusterAlias())
        );
    }

    private Set<String> listTopics(Admin adminClient)
            throws InterruptedException, ExecutionException {
        return adminCall(
                () -> adminClient.listTopics().names().get(),
                () -> "list topics on " + actualClusterAlias(adminClient) + " cluster"
        );
    }

    private Optional<Collection<AclBinding>> listTopicAclBindings()
            throws InterruptedException, ExecutionException {
        return adminCall(
                () -> {
                    Collection<AclBinding> bindings;
                    try {
                        bindings = sourceAdminClient.describeAcls(ANY_TOPIC_ACL).values().get();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof SecurityDisabledException) {
                            if (noAclAuthorizer.compareAndSet(false, true)) {
                                log.info(
                                        "No ACL authorizer is configured on the source Kafka cluster, so no topic ACL syncing will take place. "
                                                + "Consider disabling topic ACL syncing by setting " + SYNC_TOPIC_ACLS_ENABLED + " to 'false'."
                                );
                            } else {
                                log.debug("Source-side ACL authorizer still not found; skipping topic ACL sync");
                            }
                            return Optional.empty();
                        } else {
                            throw e;
                        }
                    }
                    return Optional.of(bindings);
                },
                () -> "describe ACLs on " + config.sourceClusterAlias() + " cluster"
        );
    }

    private Collection<TopicDescription> describeTopics(Admin adminClient, Collection<String> topics)
            throws InterruptedException, ExecutionException {
        return adminCall(
                () -> adminClient.describeTopics(topics).allTopicNames().get().values(),
                () -> String.format("describe topics %s on %s cluster", topics, actualClusterAlias(adminClient))
        );
    }

    static Map<String, String> configToMap(Config config) {
        return config.entries().stream()
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    }


    // visible for testing
    void incrementalAlterConfigs(Map<String, Config> topicConfigs) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = new HashMap<>();
        for (Map.Entry<String, Config> topicConfig : topicConfigs.entrySet()) {
            Collection<AlterConfigOp> ops = new ArrayList<>();
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicConfig.getKey());
            for (ConfigEntry config : topicConfig.getValue().entries()) {
                if (config.isDefault() && !shouldReplicateSourceDefault(config.name())) {
                    ops.add(new AlterConfigOp(config, AlterConfigOp.OpType.DELETE));
                } else {
                    ops.add(new AlterConfigOp(config, AlterConfigOp.OpType.SET));
                }
            }
            configOps.put(configResource, ops);
        }
        log.trace("Syncing configs for {} topics.", configOps.size());
        adminCall(() -> {
            targetAdminClient.incrementalAlterConfigs(configOps).values()
                .forEach((k, v) -> v.whenComplete((x, e) -> {
                    if (e instanceof UnsupportedVersionException) {
                        log.error("Failed to sync configs for topic {} on cluster {} with " +
                                "IncrementalAlterConfigs API", k.name(), sourceAndTarget.target(), e);
                        context.raiseError(new ConnectException("the target cluster '"
                                + sourceAndTarget.target() + "' is not compatible with " +
                                "IncrementalAlterConfigs " +
                                "API", e));
                    } else {
                        log.warn("Could not alter configuration of topic {}.", k.name(), e);
                    }
                }));
            return null;
        },
            () -> String.format("incremental alter topic configs %s on %s cluster", topicConfigs,
                    config.targetClusterAlias()));
    }

    private void updateTopicAcls(List<AclBinding> bindings) throws ExecutionException, InterruptedException {
        log.trace("Syncing {} topic ACL bindings.", bindings.size());
        adminCall(
                () -> {
                    targetAdminClient.createAcls(bindings).values().forEach((k, v) -> v.whenComplete((x, e) -> {
                        if (e != null) {
                            log.warn("Could not sync ACL of topic {}.", k.pattern().name(), e);
                        }
                    }));
                    return null;
                },
                () -> String.format("create ACLs %s on %s cluster", bindings, config.targetClusterAlias())
        );
    }

    private static Stream<TopicPartition> expandTopicDescription(TopicDescription description) {
        String topic = description.name();
        return description.partitions().stream()
            .map(x -> new TopicPartition(topic, x.partition()));
    }

    Map<String, Config> describeTopicConfigs(Set<String> topics)
            throws InterruptedException, ExecutionException {
        Set<ConfigResource> resources = topics.stream()
            .map(x -> new ConfigResource(ConfigResource.Type.TOPIC, x))
            .collect(Collectors.toSet());
        return adminCall(
                () -> sourceAdminClient.describeConfigs(resources).all().get().entrySet().stream()
                        .collect(Collectors.toMap(x -> x.getKey().name(), Entry::getValue)),
                () -> String.format("describe configs for topics %s on %s cluster", topics, config.sourceClusterAlias())
        );
    }

    Config targetConfig(Config sourceConfig, boolean incremental) {
        // If using incrementalAlterConfigs API, sync the default property with either SET or DELETE action determined by ConfigPropertyFilter::shouldReplicateSourceDefault later.
        // If not using incrementalAlterConfigs API, sync the default property only if ConfigPropertyFilter::shouldReplicateSourceDefault returns true.
        // If ConfigPropertyFilter::shouldReplicateConfigProperty returns false, do not sync the property at all.
        List<ConfigEntry> entries = sourceConfig.entries().stream()
            .filter(x -> incremental || (x.isDefault() && shouldReplicateSourceDefault(x.name())) || !x.isDefault())
            .filter(x -> !x.isReadOnly() && !x.isSensitive())
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
        return (topicFilter.shouldReplicateTopic(topic)
                || (heartbeatsReplicationEnabled && replicationPolicy.isHeartbeatsTopic(topic)))
            && !replicationPolicy.isInternalTopic(topic) && !isCycle(topic);
    }

    boolean shouldReplicateAcl(AclBinding aclBinding) {
        return !(aclBinding.entry().permissionType() == AclPermissionType.ALLOW
            && aclBinding.entry().operation() == AclOperation.WRITE);
    }

    boolean shouldReplicateTopicConfigurationProperty(String property) {
        return configPropertyFilter.shouldReplicateConfigProperty(property);
    }

    boolean shouldReplicateSourceDefault(String property) {
        return configPropertyFilter.shouldReplicateSourceDefault(property);
    }

    // Recurse upstream to detect cycles, i.e. whether this topic is already on the target cluster
    boolean isCycle(String topic) {
        String source = replicationPolicy.topicSource(topic);
        if (source == null) {
            return false;
        } else if (source.equals(sourceAndTarget.target())) {
            return true;
        } else {
            String upstreamTopic = replicationPolicy.upstreamTopic(topic);
            if (upstreamTopic == null || upstreamTopic.equals(topic)) {
                // Extra check for IdentityReplicationPolicy and similar impls that don't prevent cycles.
                return false;
            }
            return isCycle(upstreamTopic);
        }
    }

    String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceAndTarget.source(), topic);
    }

    private String actualClusterAlias(Admin adminClient) {
        return adminClient.equals(sourceAdminClient) ? config.sourceClusterAlias() : config.targetClusterAlias();
    }
}

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
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.MirrorUtils.adminCall;

/** Emits checkpoints for upstream consumer groups. */
public class MirrorCheckpointTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorCheckpointTask.class);

    private Admin sourceAdminClient;
    private Admin targetAdminClient;
    private String sourceClusterAlias;
    private String targetClusterAlias;
    private String checkpointsTopic;
    private Duration interval;
    private Duration pollTimeout;
    private TopicFilter topicFilter;
    private Set<String> consumerGroups;
    private ReplicationPolicy replicationPolicy;
    private OffsetSyncStore offsetSyncStore;
    private boolean stopping;
    private MirrorCheckpointMetrics metrics;
    private Scheduler scheduler;
    private Map<String, Map<TopicPartition, OffsetAndMetadata>> idleConsumerGroupsOffset;
    private CheckpointStore checkpointStore;

    public MirrorCheckpointTask() {}

    // for testing
    MirrorCheckpointTask(String sourceClusterAlias, String targetClusterAlias,
            ReplicationPolicy replicationPolicy, OffsetSyncStore offsetSyncStore, Set<String> consumerGroups,
            Map<String, Map<TopicPartition, OffsetAndMetadata>> idleConsumerGroupsOffset,
            CheckpointStore checkpointStore) {
        this.sourceClusterAlias = sourceClusterAlias;
        this.targetClusterAlias = targetClusterAlias;
        this.replicationPolicy = replicationPolicy;
        this.offsetSyncStore = offsetSyncStore;
        this.consumerGroups = consumerGroups;
        this.idleConsumerGroupsOffset = idleConsumerGroupsOffset;
        this.checkpointStore = checkpointStore;
        this.topicFilter = topic -> true;
        this.interval = Duration.ofNanos(1);
        this.pollTimeout = Duration.ofNanos(1);
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorCheckpointTaskConfig config = new MirrorCheckpointTaskConfig(props);
        stopping = false;
        sourceClusterAlias = config.sourceClusterAlias();
        targetClusterAlias = config.targetClusterAlias();
        consumerGroups = config.taskConsumerGroups();
        checkpointsTopic = config.checkpointsTopic();
        topicFilter = config.topicFilter();
        replicationPolicy = config.replicationPolicy();
        interval = config.emitCheckpointsInterval();
        pollTimeout = config.consumerPollTimeout();
        offsetSyncStore = new OffsetSyncStore(config);
        sourceAdminClient = config.forwardingAdmin(config.sourceAdminConfig("checkpoint-source-admin"));
        targetAdminClient = config.forwardingAdmin(config.targetAdminConfig("checkpoint-target-admin"));
        metrics = config.metrics();
        idleConsumerGroupsOffset = new HashMap<>();
        checkpointStore = new CheckpointStore(config, consumerGroups);
        scheduler = new Scheduler(getClass(), config.entityLabel(), config.adminTimeout());
        scheduler.executeAsync(() -> {
            // loading the stores are potentially long running operations, so they run asynchronously
            // to avoid blocking task::start (until a task has completed starting it cannot be stopped)
            boolean checkpointsReadOk = checkpointStore.start();
            offsetSyncStore.start(!checkpointsReadOk);
            scheduler.scheduleRepeating(this::refreshIdleConsumerGroupOffset, config.syncGroupOffsetsInterval(),
                    "refreshing idle consumers group offsets at target cluster");
            scheduler.scheduleRepeatingDelayed(this::syncGroupOffset, config.syncGroupOffsetsInterval(),
                    "sync idle consumer group offset from source to target");
        }, "starting checkpoint and offset sync stores");
        log.info("{} checkpointing {} consumer groups {}->{}: {}.", Thread.currentThread().getName(),
                consumerGroups.size(), sourceClusterAlias, config.targetClusterAlias(), consumerGroups);
    }

    @Override
    public void commit() {
        // nop
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        Utils.closeQuietly(topicFilter, "topic filter");
        Utils.closeQuietly(checkpointStore, "checkpoints store");
        Utils.closeQuietly(offsetSyncStore, "offset sync store");
        Utils.closeQuietly(sourceAdminClient, "source admin client");
        Utils.closeQuietly(targetAdminClient, "target admin client");
        Utils.closeQuietly(metrics, "metrics");
        Utils.closeQuietly(scheduler, "scheduler");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }

    @Override
    public String version() {
        return new MirrorCheckpointConnector().version();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            long deadline = System.currentTimeMillis() + interval.toMillis();
            while (!stopping && System.currentTimeMillis() < deadline) {
                Thread.sleep(pollTimeout.toMillis());
            }
            if (stopping || !checkpointStore.isInitialized()) {
                // we are stopping, or not fully initialized, return early.
                return null;
            }
            List<SourceRecord> records = new ArrayList<>();
            for (String group : consumerGroups) {
                records.addAll(sourceRecordsForGroup(group));
            }
            if (records.isEmpty()) {
                // WorkerSourceTask expects non-zero batches or null
                return null;
            } else {
                return records;
            }
        } catch (Throwable e) {
            log.warn("Failure polling consumer state for checkpoints.", e);
            return null;
        }
    }

    // visible for testing
    List<SourceRecord> sourceRecordsForGroup(String group) throws InterruptedException {
        try {
            long timestamp = System.currentTimeMillis();
            Map<TopicPartition, OffsetAndMetadata> upstreamGroupOffsets = listConsumerGroupOffsets(group);
            Map<TopicPartition, Checkpoint> newCheckpoints = checkpointsForGroup(upstreamGroupOffsets, group);
            checkpointStore.update(group, newCheckpoints);
            return newCheckpoints.values().stream()
                .map(x -> checkpointRecord(x, timestamp))
                .collect(Collectors.toList());
        } catch (ExecutionException e) {
            log.error("Error querying offsets for consumer group {} on cluster {}.",  group, sourceClusterAlias, e);
            return Collections.emptyList();
        }
    }

    // for testing
    Map<TopicPartition, Checkpoint> checkpointsForGroup(Map<TopicPartition, OffsetAndMetadata> upstreamGroupOffsets, String group) {
        return upstreamGroupOffsets.entrySet().stream()
            .filter(x -> shouldCheckpointTopic(x.getKey().topic())) // Only perform relevant checkpoints filtered by "topic filter"
            .map(x -> checkpoint(group, x.getKey(), x.getValue()))
            .flatMap(o -> o.stream()) // do not emit checkpoints for partitions that don't have offset-syncs
            .filter(x -> x.downstreamOffset() >= 0)  // ignore offsets we cannot translate accurately
            .filter(this::checkpointIsMoreRecent) // do not emit checkpoints for partitions that have a later checkpoint
            .collect(Collectors.toMap(Checkpoint::topicPartition, Function.identity()));
    }

    private boolean checkpointIsMoreRecent(Checkpoint checkpoint) {
        Map<TopicPartition, Checkpoint> checkpoints = checkpointStore.get(checkpoint.consumerGroupId());
        if (checkpoints == null) {
            log.trace("Emitting {} (first for this group)", checkpoint);
            return true;
        }
        Checkpoint lastCheckpoint = checkpoints.get(checkpoint.topicPartition());
        if (lastCheckpoint == null) {
            log.trace("Emitting {} (first for this partition)", checkpoint);
            return true;
        }
        // Emit sync after a rewind of the upstream consumer group takes place (checkpoints can be non-monotonic)
        if (checkpoint.upstreamOffset() < lastCheckpoint.upstreamOffset()) {
            log.trace("Emitting {} (upstream offset rewind)", checkpoint);
            return true;
        }
        // Or if the downstream offset is newer (force checkpoints to be monotonic)
        if (checkpoint.downstreamOffset() > lastCheckpoint.downstreamOffset()) {
            log.trace("Emitting {} (downstream offset advanced)", checkpoint);
            return true;
        }
        if (checkpoint.downstreamOffset() != lastCheckpoint.downstreamOffset()) {
            log.trace("Skipping {} (preventing downstream rewind)", checkpoint);
        } else {
            log.trace("Skipping {} (repeated checkpoint)", checkpoint);
        }
        return false;
    }

    private Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String group)
            throws InterruptedException, ExecutionException {
        if (stopping) {
            // short circuit if stopping
            return Collections.emptyMap();
        }
        return adminCall(
                () -> sourceAdminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(),
                () -> String.format("list offsets for consumer group %s on %s cluster", group, sourceClusterAlias)
        );
    }

    Optional<Checkpoint> checkpoint(String group, TopicPartition topicPartition,
                                    OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null) {
            long upstreamOffset = offsetAndMetadata.offset();
            OptionalLong downstreamOffset =
                offsetSyncStore.translateDownstream(group, topicPartition, upstreamOffset);
            if (downstreamOffset.isPresent()) {
                return Optional.of(new Checkpoint(group, renameTopicPartition(topicPartition),
                    upstreamOffset, downstreamOffset.getAsLong(), offsetAndMetadata.metadata()));
            }
        }
        return Optional.empty();
    }

    SourceRecord checkpointRecord(Checkpoint checkpoint, long timestamp) {
        return new SourceRecord(
            checkpoint.connectPartition(), MirrorUtils.wrapOffset(0),
            checkpointsTopic, 0,
            Schema.BYTES_SCHEMA, checkpoint.recordKey(),
            Schema.BYTES_SCHEMA, checkpoint.recordValue(),
            timestamp);
    }

    TopicPartition renameTopicPartition(TopicPartition upstreamTopicPartition) {
        if (targetClusterAlias.equals(replicationPolicy.topicSource(upstreamTopicPartition.topic()))) {
            // this topic came from the target cluster, so we rename like us-west.topic1 -> topic1
            return new TopicPartition(replicationPolicy.originalTopic(upstreamTopicPartition.topic()),
                upstreamTopicPartition.partition());
        } else {
            // rename like topic1 -> us-west.topic1
            return new TopicPartition(replicationPolicy.formatRemoteTopic(sourceClusterAlias,
                upstreamTopicPartition.topic()), upstreamTopicPartition.partition());
        }
    }

    boolean shouldCheckpointTopic(String topic) {
        return topicFilter.shouldReplicateTopic(topic);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        metrics.checkpointLatency(MirrorUtils.unwrapPartition(record.sourcePartition()),
            Checkpoint.unwrapGroup(record.sourcePartition()),
            System.currentTimeMillis() - record.timestamp());
    }

    private void refreshIdleConsumerGroupOffset() throws ExecutionException, InterruptedException {
        Map<String, KafkaFuture<ConsumerGroupDescription>> consumerGroupsDesc = adminCall(
                () -> targetAdminClient.describeConsumerGroups(consumerGroups).describedGroups(),
                () -> String.format("describe consumer groups %s on %s cluster", consumerGroups, targetClusterAlias)
        );

        for (String group : consumerGroups) {
            try {
                ConsumerGroupDescription consumerGroupDesc = consumerGroupsDesc.get(group).get();
                GroupState consumerGroupState = consumerGroupDesc.groupState();
                // sync offset to the target cluster only if the state of current consumer group is:
                // (1) idle: because the consumer at target is not actively consuming the mirrored topic
                // (2) dead: the new consumer that is recently created at source and never existed at target
                //           This case will be reported as a GroupIdNotFoundException
                if (consumerGroupState == GroupState.EMPTY) {
                    idleConsumerGroupsOffset.put(
                            group,
                            adminCall(
                                    () -> targetAdminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(),
                                    () -> String.format("list offsets for consumer group %s on %s cluster", group, targetClusterAlias)
                            )
                    );
                }
                // new consumer upstream has state "DEAD" and will be identified during the offset sync-up
            } catch (InterruptedException ie) {
                log.error("Error querying for consumer group {} on cluster {}.", group, targetClusterAlias, ie);
            } catch (ExecutionException ee) {
                // check for non-existent new consumer upstream which will be identified during the offset sync-up
                if (!(ee.getCause() instanceof GroupIdNotFoundException)) {
                    log.error("Error querying for consumer group {} on cluster {}.", group, targetClusterAlias, ee);
                }
            }
        }
    }

    Map<String, Map<TopicPartition, OffsetAndMetadata>> syncGroupOffset() throws ExecutionException, InterruptedException {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetToSyncAll = new HashMap<>();

        // first, sync offsets for the idle consumers at target
        for (Entry<String, Map<TopicPartition, OffsetAndMetadata>> group : checkpointStore.computeConvertedUpstreamOffset().entrySet()) {
            String consumerGroupId = group.getKey();
            // for each idle consumer at target, read the checkpoints (converted upstream offset)
            // from the pre-populated map
            Map<TopicPartition, OffsetAndMetadata> convertedUpstreamOffset = group.getValue();

            Map<TopicPartition, OffsetAndMetadata> offsetToSync = new HashMap<>();
            Map<TopicPartition, OffsetAndMetadata> targetConsumerOffset = idleConsumerGroupsOffset.get(consumerGroupId);
            if (targetConsumerOffset == null) {
                // this is a new consumer, just sync the offset to target
                syncGroupOffset(consumerGroupId, convertedUpstreamOffset);
                offsetToSyncAll.put(consumerGroupId, convertedUpstreamOffset);
                continue;
            }

            for (Entry<TopicPartition, OffsetAndMetadata> convertedEntry : convertedUpstreamOffset.entrySet()) {

                TopicPartition topicPartition = convertedEntry.getKey();
                OffsetAndMetadata convertedOffset = convertedUpstreamOffset.get(topicPartition);
                if (!targetConsumerOffset.containsKey(topicPartition)) {
                    // if is a new topicPartition from upstream, just sync the offset to target
                    offsetToSync.put(topicPartition, convertedOffset);
                    continue;
                }

                // if translated offset from upstream is smaller than the current consumer offset
                // in the target, skip updating the offset for that partition
                OffsetAndMetadata targetOffsetAndMetadata = targetConsumerOffset.get(topicPartition);
                if (targetOffsetAndMetadata != null) {
                    long latestDownstreamOffset = targetOffsetAndMetadata.offset();
                    if (latestDownstreamOffset >= convertedOffset.offset()) {
                        log.trace("latestDownstreamOffset {} is larger than or equal to convertedUpstreamOffset {} for "
                                + "TopicPartition {}", latestDownstreamOffset, convertedOffset.offset(), topicPartition);
                        continue;
                    }
                } else {
                    // It is possible that when resetting offsets are performed in the java kafka client, the reset to -1 will be intercepted.
                    // However, there are some other types of clients such as sarama, which can magically reset the group offset to -1, which will cause
                    // `targetOffsetAndMetadata` here is null. For this case, just sync the offset to target.
                    log.warn("Group {} offset for partition {} may has been reset to a negative offset, just sync the offset to target.",
                            consumerGroupId, topicPartition);
                }
                offsetToSync.put(topicPartition, convertedOffset);
            }

            if (offsetToSync.size() == 0) {
                log.trace("skip syncing the offset for consumer group: {}", consumerGroupId);
                continue;
            }
            syncGroupOffset(consumerGroupId, offsetToSync);

            offsetToSyncAll.put(consumerGroupId, offsetToSync);
        }
        idleConsumerGroupsOffset.clear();
        return offsetToSyncAll;
    }

    void syncGroupOffset(String consumerGroupId, Map<TopicPartition, OffsetAndMetadata> offsetToSync) throws ExecutionException, InterruptedException {
        if (targetAdminClient != null) {
            adminCall(
                    () -> targetAdminClient.alterConsumerGroupOffsets(consumerGroupId, offsetToSync).all()
                            .whenComplete((v, throwable) -> {
                                if (throwable != null) {
                                    if (throwable.getCause() instanceof UnknownMemberIdException) {
                                        log.warn("Unable to sync offsets for consumer group {}. This is likely caused " +
                                                "by consumers currently using this group in the target cluster.", consumerGroupId);
                                    } else {
                                        log.error("Unable to sync offsets for consumer group {}.", consumerGroupId, throwable);
                                    }
                                } else {
                                    log.trace("Sync-ed {} offsets for consumer group {}.", offsetToSync.size(), consumerGroupId);
                                }
                            }),
                    () -> String.format("alter offsets for consumer group %s on %s cluster", consumerGroupId, targetClusterAlias)
            );
        }
    }
}

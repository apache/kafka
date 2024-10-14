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

package kafka.server;

import kafka.cluster.Partition;
import kafka.log.UnifiedLog;
import kafka.log.remote.RemoteLogManager;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.CheckpointFile;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason.LeaderOffsetIncremented;

/**
 *  This class defines the APIs and implementation needed to handle any state transitions related to tiering
 *
 *  When started, the tier state machine will fetch the local log start offset of the
 *  leader and then build the follower's remote log aux state until the leader's
 *  local log start offset.
 */
public class TierStateMachine {
    private static final Logger log = LoggerFactory.getLogger(TierStateMachine.class);

    private final LeaderEndPoint leader;
    private final ReplicaManager replicaMgr;
    private final boolean useFutureLog;
    public TierStateMachine(LeaderEndPoint leader,
                            ReplicaManager replicaMgr,
                            boolean useFutureLog) {
        this.leader = leader;
        this.replicaMgr = replicaMgr;
        this.useFutureLog = useFutureLog;
    }

    /**
     * Start the tier state machine for the provided topic partition.
     *
     * @param topicPartition the topic partition
     * @param currentFetchState the current PartitionFetchState which will
     *                          be used to derive the return value
     * @param fetchPartitionData the data from the fetch response that returned the offset moved to tiered storage error
     *
     * @return the new PartitionFetchState after the successful start of the
     *         tier state machine
     */
    PartitionFetchState start(TopicPartition topicPartition,
                              PartitionFetchState currentFetchState,
                              PartitionData fetchPartitionData) throws Exception {
        OffsetAndEpoch epochAndLeaderLocalStartOffset = leader.fetchEarliestLocalOffset(topicPartition, currentFetchState.currentLeaderEpoch());
        int epoch = epochAndLeaderLocalStartOffset.leaderEpoch();
        long leaderLocalStartOffset = epochAndLeaderLocalStartOffset.offset();

        long offsetToFetch;
        replicaMgr.brokerTopicStats().topicStats(topicPartition.topic()).buildRemoteLogAuxStateRequestRate().mark();
        replicaMgr.brokerTopicStats().allTopicsStats().buildRemoteLogAuxStateRequestRate().mark();

        UnifiedLog unifiedLog;
        if (useFutureLog) {
            unifiedLog = replicaMgr.futureLogOrException(topicPartition);
        } else {
            unifiedLog = replicaMgr.localLogOrException(topicPartition);
        }

        try {
            offsetToFetch = buildRemoteLogAuxState(topicPartition, currentFetchState.currentLeaderEpoch(), leaderLocalStartOffset, epoch, fetchPartitionData.logStartOffset(), unifiedLog);
        } catch (RemoteStorageException e) {
            replicaMgr.brokerTopicStats().topicStats(topicPartition.topic()).failedBuildRemoteLogAuxStateRate().mark();
            replicaMgr.brokerTopicStats().allTopicsStats().failedBuildRemoteLogAuxStateRate().mark();
            throw e;
        }

        OffsetAndEpoch fetchLatestOffsetResult = leader.fetchLatestOffset(topicPartition, currentFetchState.currentLeaderEpoch());
        long leaderEndOffset = fetchLatestOffsetResult.offset();

        long initialLag = leaderEndOffset - offsetToFetch;

        return PartitionFetchState.apply(currentFetchState.topicId(), offsetToFetch, Option.apply(initialLag), currentFetchState.currentLeaderEpoch(),
                Fetching$.MODULE$, unifiedLog.latestEpoch());

    }

    private OffsetForLeaderEpochResponseData.EpochEndOffset fetchEarlierEpochEndOffset(Integer epoch,
                                                                                       TopicPartition partition,
                                                                                       Integer currentLeaderEpoch) {
        int previousEpoch = epoch - 1;

        // Find the end-offset for the epoch earlier to the given epoch from the leader
        Map<TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition> partitionsWithEpochs = new HashMap<>();
        partitionsWithEpochs.put(partition, new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(partition.partition()).setCurrentLeaderEpoch(currentLeaderEpoch).setLeaderEpoch(previousEpoch));
        Option<OffsetForLeaderEpochResponseData.EpochEndOffset> maybeEpochEndOffset = leader.fetchEpochEndOffsets(CollectionConverters.asScala(partitionsWithEpochs)).get(partition);
        if (maybeEpochEndOffset.isEmpty()) {
            throw new KafkaException("No response received for partition: " + partition);
        }

        OffsetForLeaderEpochResponseData.EpochEndOffset epochEndOffset = maybeEpochEndOffset.get();
        if (epochEndOffset.errorCode() != Errors.NONE.code()) {
            throw Errors.forCode(epochEndOffset.errorCode()).exception();
        }

        return epochEndOffset;
    }

    private List<EpochEntry> readLeaderEpochCheckpoint(RemoteLogManager rlm,
                                                       RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws IOException, RemoteStorageException {
        InputStream inputStream = rlm.storageManager().fetchIndex(remoteLogSegmentMetadata, RemoteStorageManager.IndexType.LEADER_EPOCH);
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            CheckpointFile.CheckpointReadBuffer<EpochEntry> readBuffer = new CheckpointFile.CheckpointReadBuffer<>("", bufferedReader, 0, LeaderEpochCheckpointFile.FORMATTER);
            return readBuffer.read();
        }
    }

    private void buildProducerSnapshotFile(UnifiedLog unifiedLog,
                                           long nextOffset,
                                           RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                           RemoteLogManager rlm) throws IOException, RemoteStorageException {
        // Restore producer snapshot
        File snapshotFile = LogFileUtils.producerSnapshotFile(unifiedLog.dir(), nextOffset);
        Path tmpSnapshotFile = Paths.get(snapshotFile.getAbsolutePath() + ".tmp");
        // Copy it to snapshot file in atomic manner.
        Files.copy(rlm.storageManager().fetchIndex(remoteLogSegmentMetadata, RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT),
                tmpSnapshotFile, StandardCopyOption.REPLACE_EXISTING);
        Utils.atomicMoveWithFallback(tmpSnapshotFile, snapshotFile.toPath(), false);

        // Reload producer snapshots.
        unifiedLog.producerStateManager().truncateFullyAndReloadSnapshots();
        unifiedLog.loadProducerState(nextOffset);
    }

    /**
     * It tries to build the required state for this partition from leader and remote storage so that it can start
     * fetching records from the leader. The return value is the next offset to fetch from the leader, which is the
     * next offset following the end offset of the remote log portion.
     */
    private Long buildRemoteLogAuxState(TopicPartition topicPartition,
                                        Integer currentLeaderEpoch,
                                        Long leaderLocalLogStartOffset,
                                        Integer epochForLeaderLocalLogStartOffset,
                                        Long leaderLogStartOffset,
                                        UnifiedLog unifiedLog) throws IOException, RemoteStorageException {

        if (!unifiedLog.remoteStorageSystemEnable() || !unifiedLog.config().remoteStorageEnable()) {
            // If the tiered storage is not enabled throw an exception back so that it will retry until the tiered storage
            // is set as expected.
            throw new RemoteStorageException("Couldn't build the state from remote store for partition " + topicPartition + ", as remote log storage is not yet enabled");
        }

        if (replicaMgr.remoteLogManager().isEmpty())
            throw new IllegalStateException("RemoteLogManager is not yet instantiated");

        RemoteLogManager rlm = replicaMgr.remoteLogManager().get();

        // Find the respective leader epoch for (leaderLocalLogStartOffset - 1). We need to build the leader epoch cache
        // until that offset
        long previousOffsetToLeaderLocalLogStartOffset = leaderLocalLogStartOffset - 1;
        int targetEpoch;
        // If the existing epoch is 0, no need to fetch from earlier epoch as the desired offset(leaderLogStartOffset - 1)
        // will have the same epoch.
        if (epochForLeaderLocalLogStartOffset == 0) {
            targetEpoch = epochForLeaderLocalLogStartOffset;
        } else {
            // Fetch the earlier epoch/end-offset(exclusive) from the leader.
            OffsetForLeaderEpochResponseData.EpochEndOffset earlierEpochEndOffset = fetchEarlierEpochEndOffset(epochForLeaderLocalLogStartOffset, topicPartition, currentLeaderEpoch);
            // Check if the target offset lies within the range of earlier epoch. Here, epoch's end-offset is exclusive.
            if (earlierEpochEndOffset.endOffset() > previousOffsetToLeaderLocalLogStartOffset) {
                // Always use the leader epoch from returned earlierEpochEndOffset.
                // This gives the respective leader epoch, that will handle any gaps in epochs.
                // For ex, leader epoch cache contains:
                // leader-epoch   start-offset
                //  0               20
                //  1               85
                //  <2> - gap no messages were appended in this leader epoch.
                //  3               90
                //  4               98
                // There is a gap in leader epoch. For leaderLocalLogStartOffset as 90, leader-epoch is 3.
                // fetchEarlierEpochEndOffset(2) will return leader-epoch as 1, end-offset as 90.
                // So, for offset 89, we should return leader epoch as 1 like below.
                targetEpoch = earlierEpochEndOffset.leaderEpoch();
            } else {
                targetEpoch = epochForLeaderLocalLogStartOffset;
            }
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = rlm.fetchRemoteLogSegmentMetadata(topicPartition, targetEpoch, previousOffsetToLeaderLocalLogStartOffset)
                .orElseThrow(() -> new RemoteStorageException("Couldn't build the state from remote store for partition: " + topicPartition +
                        ", currentLeaderEpoch: " + currentLeaderEpoch +
                        ", leaderLocalLogStartOffset: " + leaderLocalLogStartOffset +
                        ", leaderLogStartOffset: " + leaderLogStartOffset +
                        ", epoch: " + targetEpoch +
                        "as the previous remote log segment metadata was not found"));


        // Build leader epoch cache, producer snapshots until remoteLogSegmentMetadata.endOffset() and start
        // segments from (remoteLogSegmentMetadata.endOffset() + 1)
        // Assign nextOffset with the offset from which next fetch should happen.
        long nextOffset = remoteLogSegmentMetadata.endOffset() + 1;

        // Truncate the existing local log before restoring the leader epoch cache and producer snapshots.
        Partition partition = replicaMgr.getPartitionOrException(topicPartition);
        partition.truncateFullyAndStartAt(nextOffset, useFutureLog, Option.apply(leaderLogStartOffset));
        // Increment start offsets
        unifiedLog.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented);
        unifiedLog.maybeIncrementLocalLogStartOffset(nextOffset, LeaderOffsetIncremented);

        // Build leader epoch cache.
        List<EpochEntry> epochs = readLeaderEpochCheckpoint(rlm, remoteLogSegmentMetadata);
        if (unifiedLog.leaderEpochCache().isDefined()) {
            unifiedLog.leaderEpochCache().get().assign(epochs);
        }

        log.info("Updated the epoch cache from remote tier till offset: {} with size: {} for {}", leaderLocalLogStartOffset, epochs.size(), partition);

        buildProducerSnapshotFile(unifiedLog, nextOffset, remoteLogSegmentMetadata, rlm);

        log.debug("Built the leader epoch cache and producer snapshots from remote tier for {}, " +
                        "with active producers size: {}, leaderLogStartOffset: {}, and logEndOffset: {}",
                partition, unifiedLog.producerStateManager().activeProducers().size(), leaderLogStartOffset, nextOffset);

        return nextOffset;
    }
}

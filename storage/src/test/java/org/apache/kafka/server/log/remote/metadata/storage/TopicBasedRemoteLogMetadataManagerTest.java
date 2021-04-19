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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP;

public class TopicBasedRemoteLogMetadataManagerTest {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerTest.class);

    private static final int SEG_SIZE = 1024 * 1024;

    private final Time time = new MockTime(1);
    private final TopicBasedRemoteLogMetadataManagerHarness remoteLogMetadataManagerHarness = new TopicBasedRemoteLogMetadataManagerHarness() {
        @Override
        protected Map<String, Object> overrideRemoteLogMetadataManagerProps() {
            return Collections.singletonMap(REMOTE_LOG_METADATA_CONSUME_WAIT_MS_PROP, 5000L);
        }
    };

    @BeforeEach
    public void setup() {
        // Start the cluster and initialize TopicBasedRemoteLogMetadataManager.
        remoteLogMetadataManagerHarness.initialize(Collections.emptySet());
    }

    @AfterEach
    public void teardown() throws IOException {
        remoteLogMetadataManagerHarness.close();
    }

    public TopicBasedRemoteLogMetadataManager topicBasedRlmm() {
        return remoteLogMetadataManagerHarness.topicBasedRlmm();
    }

    @Test
    public void testNewPartitionUpdates() throws Exception {
        final TopicIdPartition newLeaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("new-leader", 0));
        final TopicIdPartition newFollowerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("new-follower", 0));

        // Add segments for these partitions but they are not available as they have not yet been subscribed.
        RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(newLeaderTopicIdPartition, Uuid.randomUuid()),
                                                                                0, 100, -1L, 0,
                                                                                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        topicBasedRlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata);

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(newFollowerTopicIdPartition, Uuid.randomUuid()),
                                                                                0, 100, -1L, 0,
                                                                                time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        topicBasedRlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata);

        // `listRemoteLogSegments` will receive an exception as these topic partitions are not yet registered.
        Assertions.assertThrows(RemoteStorageException.class, () -> topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition));
        Assertions.assertThrows(RemoteStorageException.class, () -> topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition));

        topicBasedRlmm().onPartitionLeadershipChanges(Collections.singleton(newLeaderTopicIdPartition),
                                                      Collections.singleton(newFollowerTopicIdPartition));

        waitUntilConsumerCatchesup(newLeaderTopicIdPartition, newFollowerTopicIdPartition, 30000L);

        Assertions.assertTrue(topicBasedRlmm().listRemoteLogSegments(newLeaderTopicIdPartition).hasNext());
        Assertions.assertTrue(topicBasedRlmm().listRemoteLogSegments(newFollowerTopicIdPartition).hasNext());
    }

    private void waitUntilConsumerCatchesup(TopicIdPartition newLeaderTopicIdPartition,
                                            TopicIdPartition newFollowerTopicIdPartition,
                                            long timeoutMs) throws TimeoutException {
        int leaderMetadataPartition = topicBasedRlmm().metadataPartition(newLeaderTopicIdPartition);
        int followerMetadataPartition = topicBasedRlmm().metadataPartition(newFollowerTopicIdPartition);

        log.debug("Metadata partition for newLeaderTopicIdPartition: [{}], is: [{}]", newLeaderTopicIdPartition, leaderMetadataPartition);
        log.debug("Metadata partition for newFollowerTopicIdPartition: [{}], is: [{}]", newFollowerTopicIdPartition, followerMetadataPartition);

        long sleepMs = 100L;
        long time = System.currentTimeMillis();

        while (true) {
            if (System.currentTimeMillis() - time > timeoutMs) {
                throw new TimeoutException("Timed out after " + timeoutMs + "ms ");
            }

            if (leaderMetadataPartition == followerMetadataPartition) {
                if (topicBasedRlmm().receivedOffsetForPartition(leaderMetadataPartition).orElse(-1L) > 0) {
                    break;
                }
            } else {
                if (topicBasedRlmm().receivedOffsetForPartition(leaderMetadataPartition).orElse(-1L) > -1 ||
                        topicBasedRlmm().receivedOffsetForPartition(followerMetadataPartition).orElse(-1L) > -1) {
                    break;
                }
            }

            log.debug("Sleeping for: " + sleepMs);
            Utils.sleep(sleepMs);
        }
    }

}

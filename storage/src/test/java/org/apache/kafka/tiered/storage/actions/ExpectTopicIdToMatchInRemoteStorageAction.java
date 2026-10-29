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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageSnapshot;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.describeTopic;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ExpectTopicIdToMatchInRemoteStorageAction implements TieredStorageTestAction {

    private final String topic;

    public ExpectTopicIdToMatchInRemoteStorageAction(String topic) {
        this.topic = topic;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        Uuid topicId = describeTopic(context, topic).topicId();
        context.remoteStorageManagers().forEach(rsm -> {
            LocalTieredStorageSnapshot snapshot = LocalTieredStorageSnapshot.takeSnapshot(rsm);
            List<TopicPartition> partitions = snapshot.getTopicPartitions()
                    .stream()
                    .filter(tp -> tp.topic().equals(topic))
                    .toList();
            partitions.forEach(partition ->
                snapshot.getFilesets(partition)
                        .forEach(fileset -> assertEquals(topicId, fileset.getRemoteLogSegmentId().id()))
            );
        });
    }

    @Override
    public void describe(PrintStream output) {
        output.println("expect-topic-id-to-match-in-remote-storage: " + topic);
    }
}

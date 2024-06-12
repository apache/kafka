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

import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageSnapshot;
import org.apache.kafka.test.TestUtils;

import java.io.PrintStream;

public final class ExpectEmptyRemoteStorageAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;

    public ExpectEmptyRemoteStorageAction(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            LocalTieredStorageSnapshot snapshot = context.takeTieredStorageSnapshot();
            // We don't differentiate the case between segment deletion and topic deletion so the underlying
            // remote-storage-manager (RSM) doesn't know when to remove any topic-level marker files/folders.
            // In case of LocalTieredStorage (RSM), there will be empty partition directories.
            // With KAFKA-15166, the RSM will be able to delete the topic-level marker folders, then the
            // `LocalTieredStorageSnapshot` should not contain the partition directories.
            return snapshot.getFilesets(topicPartition).isEmpty();
        }, 2000L, "Remote storage is not empty for " + topicPartition);
    }

    @Override
    public void describe(PrintStream output) {
        output.println("expect-empty-remote-storage topic-partition: " + topicPartition);
    }
}

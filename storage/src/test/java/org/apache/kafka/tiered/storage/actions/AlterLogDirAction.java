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
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;

import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public final class AlterLogDirAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;
    private final int brokerId;

    public AlterLogDirAction(TopicPartition topicPartition,
                             int brokerId) {
        this.topicPartition = topicPartition;
        this.brokerId = brokerId;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        Optional<BrokerLocalStorage> localStorage = context.localStorages().stream().filter(storage -> storage.getBrokerId().intValue() == brokerId).findFirst();
        if (!localStorage.isPresent()) {
            throw new IllegalArgumentException("cannot find local storage for this topic partition:" + topicPartition + " in this broker id:" + brokerId);
        }

        Optional<File> sourceDir = localStorage.get().getBrokerStorageDirectory().stream().filter(dir -> localStorage.get().isTopicPartitionFileExistInDir(topicPartition, dir)).findFirst();
        Optional<File> targetDir = localStorage.get().getBrokerStorageDirectory().stream().filter(dir -> !localStorage.get().isTopicPartitionFileExistInDir(topicPartition, dir)).findFirst();
        if (!sourceDir.isPresent()) {
            throw new IllegalArgumentException("No log dir with topic partition:" + topicPartition + " in this broker id:" + brokerId);
        }

        if (!targetDir.isPresent()) {
            throw new IllegalArgumentException("No log dir without topic partition:" + topicPartition + " in this broker id:" + brokerId);
        }

        // build alterReplicaLogDirs request content to move from sourceDir to targetDir
        Map<TopicPartitionReplica, String> logDirs = Collections.singletonMap(new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), brokerId), targetDir.get().getAbsolutePath());

        context.admin().alterReplicaLogDirs(logDirs);

        // wait until the topic partition folder disappearing from source dir and appearing in the target dir
        TestUtils.waitForCondition(() -> localStorage.get().isTopicPartitionFileExistInDir(topicPartition, targetDir.get()) &&
                    !localStorage.get().isTopicPartitionFileExistInDir(topicPartition, sourceDir.get()),
                "Failed to alter dir:" + logDirs);
    }

    @Override
    public void describe(PrintStream output) {
        output.print("alter di for topic partition:" + topicPartition + " in this broker id:" + brokerId);
    }
}

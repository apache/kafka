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

import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AlterLogDirAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;
    private final int brokerId;

    public AlterLogDirAction(TopicPartition topicPartition,
                             int brokerId) {
        this.topicPartition = topicPartition;
        this.brokerId = brokerId;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        Optional<BrokerLocalStorage> localStorage = context.localStorages().stream().filter(storage -> storage.getBrokerId() == brokerId).findFirst();
        if (localStorage.isEmpty()) {
            throw new IllegalArgumentException("cannot find local storage for this topic partition:" + topicPartition + " in this broker id:" + brokerId);
        }

        Optional<File> sourceDir = localStorage.get().getBrokerStorageDirectories().stream().filter(dir -> localStorage.get().dirContainsTopicPartition(topicPartition, dir)).findFirst();
        if (sourceDir.isEmpty()) {
            throw new IllegalArgumentException("No log dir with topic partition:" + topicPartition + " in this broker id:" + brokerId);
        }
        Optional<File> targetDir = localStorage.get().getBrokerStorageDirectories().stream().filter(dir -> !localStorage.get().dirContainsTopicPartition(topicPartition, dir)).findFirst();
        if (targetDir.isEmpty()) {
            throw new IllegalArgumentException("No log dir without topic partition:" + topicPartition + " in this broker id:" + brokerId);
        }

        // build alterReplicaLogDirs request content to move from sourceDir to targetDir
        TopicPartitionReplica topicPartitionReplica = new TopicPartitionReplica(topicPartition.topic(), topicPartition.partition(), brokerId);
        Map<TopicPartitionReplica, String> logDirs = Collections.singletonMap(topicPartitionReplica, targetDir.get().getAbsolutePath());

        AlterReplicaLogDirsResult results = context.admin().alterReplicaLogDirs(logDirs);
        results.values().get(topicPartitionReplica).get(30, TimeUnit.SECONDS);

        // wait until the topic partition folder disappears from source dir and appears in the target dir
        TestUtils.waitForCondition(() -> localStorage.get().dirContainsTopicPartition(topicPartition, targetDir.get()) &&
                    !localStorage.get().dirContainsTopicPartition(topicPartition, sourceDir.get()),
                "Failed to alter dir:" + logDirs);
    }

    @Override
    public void describe(PrintStream output) {
        output.print("alter dir for topic partition:" + topicPartition + " in this broker id:" + brokerId);
    }
}

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

package org.apache.kafka.common.test;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AdminUtils {

    /**
     * Wait for a leader to be elected or changed using the provided admin client.
     *
     * @param admin           the admin client to use for describing the topic.
     * @param topic           the topic to check for leadership.
     * @param partitionNumber the partition number to check for leadership.
     * @param timeoutMs       the maximum time to wait for a leader to be elected or changed in milliseconds.
     * @return the id of the elected leader.
     * @throws ExecutionException   if an unexpected exception occurs while describing the topic.
     * @throws InterruptedException if the thread is interrupted while waiting for a leader to be elected or changed.
     * @throws AssertionError       if a leader is not elected within the specified timeout.
     */
    public static int waitUntilLeaderIsElectedOrChanged(Admin admin,
                                                        String topic,
                                                        int partitionNumber,
                                                        long timeoutMs) throws ExecutionException, InterruptedException, AssertionError {
        return waitUntilLeaderIsElectedOrChanged(admin, topic, partitionNumber, timeoutMs, System::currentTimeMillis);
    }


    // overload with time supplier for testing
    static int waitUntilLeaderIsElectedOrChanged(Admin admin,
                                                 String topic,
                                                 int partitionNumber,
                                                 long timeoutMs,
                                                 Supplier<Long> timeProvider) throws ExecutionException, InterruptedException, AssertionError {
        long startTime = timeProvider.get();
        TopicPartition topicPartition = new TopicPartition(topic, partitionNumber);

        while (timeProvider.get() < startTime + timeoutMs) {
            try {
                TopicDescription topicDescription = admin.describeTopics(List.of(topic))
                        .allTopicNames().get().get(topic);

                Optional<Integer> leader = topicDescription.partitions().stream()
                        .filter(partitionInfo -> partitionInfo.partition() == partitionNumber)
                        .findFirst()
                        .flatMap(partitionInfo -> Optional.ofNullable(partitionInfo.leader()))
                        .map(node -> {
                            int leaderId = node.id();
                            return leaderId == Node.noNode().id() ? null : leaderId;
                        });

                if (leader.isPresent()) {
                    return leader.get();
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnknownTopicOrPartitionException ||
                        cause instanceof LeaderNotAvailableException) {
                    continue;
                } else {
                    throw e;
                }
            }

            TimeUnit.MILLISECONDS.sleep(Math.min(100L, timeoutMs));
        }

        throw new AssertionError("Timing out after " + timeoutMs +
                " ms since a leader was not elected for partition " + topicPartition);
    }
}

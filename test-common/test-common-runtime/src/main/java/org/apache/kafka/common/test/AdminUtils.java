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
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public final class AdminUtils {

    private AdminUtils() {}

    /**
     * Fetch the partition leader or wait until one is elected using the provided admin client.
     */
    public static int fetchOrWaitForLeader(Admin admin,
                                           String topic,
                                           int partitionNumber,
                                           long timeoutMs) throws InterruptedException {

        var condition = new Supplier<Boolean>() {
            int leader = Node.noNode().id();
            @Override
            public Boolean get() {
                checkLeader();
                return this.leader != Node.noNode().id();
            }

            public void checkLeader() {
                fetchPartitionLeader(admin, topic, partitionNumber).ifPresent(integer -> this.leader = integer);
            }
        };

        TestUtils.waitForCondition(condition, timeoutMs, "Timing out after %d ms since a leader was not elected for partition %s-%d".formatted(timeoutMs, topic, partitionNumber));

        return condition.leader;
    }

    /**
     * Fetch the partition leader or wait until the expected leader is observed using the provided admin client.
     */
    public static int fetchOrWaitForExpectedLeader(Admin admin,
                                                   String topic,
                                                   int partitionNumber,
                                                   int expectedLeaderId,
                                                   long timeoutMs) throws InterruptedException {
        var condition = new Supplier<Boolean>() {
            int leader = Node.noNode().id();

            @Override
            public Boolean get() {
                checkLeader();
                return this.leader == expectedLeaderId;
            }

            public void checkLeader() {
                fetchPartitionLeader(admin, topic, partitionNumber).ifPresent(integer -> this.leader = integer);
            }
        };

        TestUtils.waitForCondition(condition, timeoutMs,
            () -> "Timing out after %d ms waiting for partition %s-%d leader to become %d, last observed=%d"
                .formatted(timeoutMs, topic, partitionNumber, expectedLeaderId, condition.leader));

        return condition.leader;
    }

    private static Optional<Integer> fetchPartitionLeader(Admin admin, String topic, int partitionNumber) {
        try {
            TopicDescription topicDescription = admin.describeTopics(List.of(topic))
                .allTopicNames().get().get(topic);

            return topicDescription.partitions().stream()
                .filter(partitionInfo -> partitionInfo.partition() == partitionNumber)
                .findFirst()
                .flatMap(partitionInfo -> Optional.ofNullable(partitionInfo.leader()))
                .map(node -> {
                    int leaderId = node.id();
                    return leaderId == Node.noNode().id() ? null : leaderId;
                });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            boolean isTransient = cause instanceof UnknownTopicOrPartitionException
                || cause instanceof LeaderNotAvailableException;
            if (!isTransient) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return Optional.empty();
    }
}

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

package org.apache.kafka.server.util;

import org.apache.kafka.common.errors.OperationNotAttemptedException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.partition.AlterPartitionItem;
import org.apache.kafka.server.partition.AlterPartitionManager;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.fail;

public class MockAlterPartitionManager implements AlterPartitionManager {
    // Visible for testing
    public final Deque<AlterPartitionItem> isrUpdates = new ArrayDeque<>();
    private final AtomicBoolean inFlight = new AtomicBoolean(false);

    @Override
    public void start() {

    }

    @Override
    public void shutdown() throws InterruptedException {

    }

    @Override
    public CompletableFuture<LeaderAndIsr> submit(TopicIdPartition topicIdPartition, LeaderAndIsr leaderAndIsr) {
        var future = new CompletableFuture<LeaderAndIsr>();
        if (inFlight.compareAndSet(false, true)) {
            isrUpdates.add(new AlterPartitionItem(topicIdPartition, leaderAndIsr, future));
        } else {
            future.completeExceptionally(new OperationNotAttemptedException(
                    String.format("Failed to enqueue AlterIsr request for %s since there is already an inflight request",
                            topicIdPartition)
            ));
        }
        return future;
    }

    public void completeIsrUpdate(int newPartitionEpoch) {
        if (inFlight.compareAndSet(true, false)) {
            var item = isrUpdates.poll();
            item.future().complete(item.leaderAndIsr().withPartitionEpoch(newPartitionEpoch));
        } else {
            fail("Expected an in-flight ISR update, but there was none");
        }
    }

    public void failIsrUpdate(Errors error) {
        if (inFlight.compareAndSet(true, false)) {
            var item = isrUpdates.poll();
            item.future().completeExceptionally(error.exception());
        } else {
            fail("Expected an in-flight ISR update, but there was none");
        }
    }
}

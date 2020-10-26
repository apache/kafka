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

package org.apache.kafka.clients.admin.internals;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

/**
 * Context class to encapsulate parameters of a call to find and use a consumer group coordinator.
 * Some of the parameters are provided at construction and are immutable whereas others are provided
 * as "Call" are completed and values are available, like node id of the coordinator.
 *
 * @param <T> The type of return value of the KafkaFuture
 * @param <O> The type of configuration option. Different for different consumer group commands.
 */
public final class ConsumerGroupOperationContext<T, O extends AbstractOptions<O>> {
    final private String groupId;
    final private O options;
    final private long deadline;
    final private KafkaFutureImpl<T> future;
    private Optional<Node> node;

    public ConsumerGroupOperationContext(String groupId,
                                         O options,
                                         long deadline,
                                         KafkaFutureImpl<T> future) {
        this.groupId = groupId;
        this.options = options;
        this.deadline = deadline;
        this.future = future;
        this.node = Optional.empty();
    }

    public String groupId() {
        return groupId;
    }

    public O options() {
        return options;
    }

    public long deadline() {
        return deadline;
    }

    public KafkaFutureImpl<T> future() {
        return future;
    }

    public Optional<Node> node() {
        return node;
    }

    public void setNode(Node node) {
        this.node = Optional.ofNullable(node);
    }

    public static boolean hasCoordinatorMoved(AbstractResponse response) {
        return hasCoordinatorMoved(response.errorCounts());
    }

    public static boolean hasCoordinatorMoved(Map<Errors, Integer> errorCounts) {
        return errorCounts.containsKey(Errors.NOT_COORDINATOR);
    }

    public static boolean shouldRefreshCoordinator(Map<Errors, Integer> errorCounts) {
        return errorCounts.containsKey(Errors.COORDINATOR_LOAD_IN_PROGRESS) ||
                errorCounts.containsKey(Errors.COORDINATOR_NOT_AVAILABLE);
    }
}

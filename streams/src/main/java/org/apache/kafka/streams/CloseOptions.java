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
package org.apache.kafka.streams;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public class CloseOptions {
    /**
     * Enum to specify the group membership operation upon closing the Kafka Streams application.
     *
     * <ul>
     *   <li><b>{@code LEAVE_GROUP}</b>: means the consumer leave the group.</li>
     *   <li><b>{@code REMAIN_IN_GROUP}</b>: means the consumer will remain in the group.</li>
     * </ul>
     */
    public enum GroupMembershipOperation {
        LEAVE_GROUP,
        REMAIN_IN_GROUP
    }

    /**
     * Specifies the group membership operation upon shutdown.
     * By default, {@code GroupMembershipOperation.REMAIN_IN_GROUP} will be applied, which follows the KafkaStreams default behavior.
     */
    protected GroupMembershipOperation operation = GroupMembershipOperation.REMAIN_IN_GROUP;

    /**
     * Specifies the maximum amount of time to wait for the close process to complete.
     * This allows users to define a custom timeout for gracefully stopping the KafkaStreams.
     */
    protected Optional<Duration> timeout = Optional.of(Duration.ofMillis(Long.MAX_VALUE));

    private CloseOptions() {
    }

    protected CloseOptions(final CloseOptions closeOptions) {
        this.operation = closeOptions.operation;
        this.timeout = closeOptions.timeout;
    }

    /**
     * Static method to create a {@code CloseOptions} with a custom timeout.
     *
     * @param timeout the maximum time to wait for the KafkaStreams to close.
     * @return a new {@code CloseOptions} instance with the specified timeout.
     */
    public static CloseOptions timeout(final Duration timeout) {
        return new CloseOptions().withTimeout(timeout);
    }

    /**
     * Static method to create a {@code CloseOptions} with a specified group membership operation.
     *
     * @param operation the group membership operation to apply. Must be one of {@code LEAVE_GROUP}, {@code REMAIN_IN_GROUP}.
     * @return a new {@code CloseOptions} instance with the specified group membership operation.
     */
    public static CloseOptions groupMembershipOperation(final GroupMembershipOperation operation) {
        return new CloseOptions().withGroupMembershipOperation(operation);
    }

    /**
     * Fluent method to set the timeout for the close process.
     *
     * @param timeout the maximum time to wait for the KafkaStreams to close. If {@code null}, the default timeout will be used.
     * @return this {@code CloseOptions} instance.
     */
    public CloseOptions withTimeout(final Duration timeout) {
        this.timeout = Optional.ofNullable(timeout);
        return this;
    }

    /**
     * Fluent method to set the group membership operation upon shutdown.
     *
     * @param operation the group membership operation to apply. Must be one of {@code LEAVE_GROUP}, {@code REMAIN_IN_GROUP}.
     * @return this {@code CloseOptions} instance.
     */
    public CloseOptions withGroupMembershipOperation(final GroupMembershipOperation operation) {
        this.operation = Objects.requireNonNull(operation, "operation should not be null");
        return this;
    }
}

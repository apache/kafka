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

package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.annotation.InterfaceAudience;

/**
 * This exception is raised when an offset commit with {@link KafkaConsumer#commitSync()} fails
 * with an unrecoverable error. This exception is generated on the client side, typically when
 * a group rebalance completes before the commit could be successfully applied. In this case,
 * the commit cannot generally be retried because some of the partitions may have already been
 * assigned to another member in the group.
 */
@InterfaceAudience.Public
public class CommitFailedException extends KafkaException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new CommitFailedException with the specified detail message.
     *
     * @param message The error message
     */
    public CommitFailedException(final String message) {
        super(message);
    }

    /**
     * Constructs a new CommitFailedException with a default message explaining the cause of the commit failure.
     */
    public CommitFailedException() {
        super("Commit cannot be completed since the group has already " +
                "rebalanced and assigned the partitions to another member. This means that the time " +
                "between subsequent calls to poll() was longer than the configured max.poll.interval.ms, " +
                "which typically implies that the poll loop is spending too much time message processing. " +
                "You can address this either by increasing max.poll.interval.ms or by reducing the maximum " +
                "size of batches returned in poll() with max.poll.records.");
    }
}

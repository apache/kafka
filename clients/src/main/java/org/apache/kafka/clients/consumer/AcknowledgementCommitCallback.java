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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Map;
import java.util.Set;

/**
 * A callback interface that the user can implement to trigger custom actions when an acknowledgement completes.
 * The callback may be executed in any thread calling {@link ShareConsumer#poll(java.time.Duration)}.
 */
@InterfaceStability.Evolving
public interface AcknowledgementCommitCallback {

    /**
     * A callback method the user can implement to provide asynchronous handling of acknowledgement completion.
     * This method will be called when the acknowledgement request sent to the server has been completed.
     *
     * @param offsets A map of the offsets that this callback applies to.
     *
     * @param exception The exception thrown during processing of the request, or null if the acknowledgement completed successfully.
     * <p><ul>
     * <li> {@link AuthorizationException} if not authorized to the topic or group
     * <li> {@link InvalidRecordStateException} if the record state is invalid
     * <li> {@link NotLeaderOrFollowerException} if the leader had changed by the time the acknowledgements were sent
     * <li> {@link DisconnectException} if the broker disconnected before the request could be completed
     * <li> {@link WakeupException} if {@link KafkaShareConsumer#wakeup()} is called before or while this function is called
     * <li> {@link InterruptException} if the calling thread is interrupted before or while this function is called
     * <li> {@link KafkaException} for any other unrecoverable errors
     * </ul>
     * <p>Note that even if the exception is a retriable exception, the acknowledgement could not be completed and the
     * records need to be fetched again. The callback is called after any retries have been performed.
     */
    void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception);
}

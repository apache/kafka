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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Map;
import java.util.Set;

/**
 * A callback interface that the user can implement to trigger custom actions when an acknowledge request completes.
 * The callback may be executed in any thread calling {@link ShareConsumer#poll(java.time.Duration)}.
 */
@InterfaceStability.Evolving
public interface AcknowledgementCommitCallback {

    /**
     * A callback method the user can implement to provide asynchronous handling of commit request completion.
     * This method will be called when the commit request sent to the server has been acknowledged.
     *
     * @param offsets A map of the offsets that this callback applies to.
     *
     * @param exception The exception thrown during processing of the request, or null if the acknowledgement completed successfully.
     *
     * @throws InvalidRecordStateException The record state is invalid. The acknowledgement of delivery
     *             could not be completed.
     * @throws WakeupException if {@link KafkaShareConsumer#wakeup()} is called before or while this
     *             function is called
     * @throws InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws AuthorizationException if not authorized to the topic of group
     * @throws KafkaException for any other unrecoverable errors
     */
    void onComplete(Map<TopicIdPartition, Set<OffsetAndMetadata>> offsets, Exception exception);
}

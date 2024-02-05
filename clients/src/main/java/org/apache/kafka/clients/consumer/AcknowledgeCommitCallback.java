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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Map;
import java.util.Optional;

/**
 * A callback interface that the user can implement to trigger custom actions when an acknowledge request completes.
 * The callback may be executed in any thread calling {@link ShareConsumer#poll(java.time.Duration)}.
 */
public interface AcknowledgeCommitCallback {

    /**
     * A callback method the user can implement to provide asynchronous handling of commit request completion.
     * This method will be called when the commit request sent to the server has been acknowledged.
     *
     * @param results A map of the results for each topic-partition for which delivery was acknowledged.
     *                If the acknowledgement failed for a topic-partition, an exception is present.
     *
     * @throws WakeupException if {@link KafkaShareConsumer#wakeup()} is called before or while this
     *             function is called
     * @throws InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the committed offset is invalid).
     */
    void onComplete(Map<TopicIdPartition, Optional<KafkaException>> results);
}

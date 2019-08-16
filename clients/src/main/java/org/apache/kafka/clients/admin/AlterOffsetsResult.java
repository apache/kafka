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
package org.apache.kafka.clients.admin;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

/**
 * The result of the {@link AdminClient#alterConsumerGroupOffsets(String, Map)} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class AlterOffsetsResult {

    private final KafkaFutureImpl<Map<TopicPartition, KafkaFutureImpl<Void>>> future;

    public AlterOffsetsResult(KafkaFutureImpl<Map<TopicPartition, KafkaFutureImpl<Void>>> future) {
        this.future = future;
    }

    public KafkaFutureImpl<Map<TopicPartition, KafkaFutureImpl<Void>>> values() {
        return future;
    }

    /**
     * Return a future which succeeds if all the alter offsets succeed.
     */
    public KafkaFuture<Void> all() {
        try {
            return KafkaFuture.allOf(values().get().values().toArray(new KafkaFuture[0]));
        } catch (InterruptedException | ExecutionException e) {
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            future.completeExceptionally(e);
            return future;
        }
    }
}

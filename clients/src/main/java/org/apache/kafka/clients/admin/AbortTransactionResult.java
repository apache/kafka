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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Map;

/**
 * The result of {@link Admin#abortTransaction(AbortTransactionSpec, AbortTransactionOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class AbortTransactionResult {
    private final Map<TopicPartition, KafkaFutureImpl<Void>> futures;

    AbortTransactionResult(Map<TopicPartition, KafkaFutureImpl<Void>> futures) {
        this.futures = futures;
    }

    /**
     * Get a future which completes when the transaction specified by {@link AbortTransactionSpec}
     * in the respective call to {@link Admin#abortTransaction(AbortTransactionSpec, AbortTransactionOptions)}
     * returns successfully or fails due to an error or timeout.
     *
     * @return the future
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }

}

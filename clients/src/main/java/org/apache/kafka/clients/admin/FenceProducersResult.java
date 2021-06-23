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
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import java.util.Collection;
import java.util.Map;

/**
 * The result of the {@link Admin#fenceProducers(Collection)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class FenceProducersResult {

    private final Map<String, KafkaFutureImpl<ProducerIdAndEpoch>> futures;

    FenceProducersResult(Map<String, KafkaFutureImpl<ProducerIdAndEpoch>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from transactional ID to futures which can be used to check the status of
     * individual fencings.
     */
    public Map<String, KafkaFuture<Void>> fencedProducers() {
        return null;
    }

    /**
     * Returns a future that provides the producer ID generated while initializing the given transaction when the request completes.
     */
    public KafkaFuture<Long> producerId(String transactionalId) {
        return futures.get(transactionalId).thenApply(p -> p.producerId);
    }

    /**
     * Returns a future that provides the epoch ID generated while initializing the given transaction when the request completes.
     */
    public KafkaFuture<Short> epochId(String transactionalId) {
        return futures.get(transactionalId).thenApply(p -> p.epoch);
    }

    /**
     * Return a future which succeeds only if all the producer fencings succeed.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}

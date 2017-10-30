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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The result of {@link AdminClient#electPreferredLeaders(Collection, ElectPreferredLeadersOptions)}
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ElectPreferredLeadersResult {

    private final KafkaFuture<? extends Map<TopicPartition, ? extends KafkaFuture<Void>>> futures;

    ElectPreferredLeadersResult(KafkaFuture<? extends Map<TopicPartition, ? extends KafkaFuture<Void>>> futures) {
        this.futures = futures;
    }

    /**
     * Get the result of the election for the given {@code partition}.
     * If there was not an election triggered for the given {@code partition}, the
     * returned future will complete with an error.
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        class Function<T extends Map<TopicPartition, ? extends KafkaFuture<Void>>>
                extends KafkaFuture.Function<T, KafkaFuture<Void>> {
            @Override
            public KafkaFuture<Void> apply(T map) {
                KafkaFuture<Void> result = map.get(partition);
                if (result == null) {
                    KafkaFutureImpl future = new KafkaFutureImpl<>();
                    future.completeExceptionally(new IllegalArgumentException(
                            "Preferred leader election for partition \"" + partition + "\" was not attempted"));
                    result = future;
                }
                return result;
            }
        }
        return futures.<Void>thenCompose(new Function());
    }

    /**
     * <p>Get a future for the topic partitions for which a leader election
     * was attempted. A partition will be present in this result if
     * an election was attempted even if the election was not successful.</p>
     *
     * <p>This method is provided to discover the partitions attempted when
     * {@link AdminClient#electPreferredLeaders(Collection)} is called
     * with a null {@code partitions} argument.</p>
     */
    public KafkaFuture<Set<TopicPartition>> partitions() {
        class Function<T extends Map<TopicPartition, ? extends KafkaFuture<Void>>> extends KafkaFuture.Function<T, Set<TopicPartition>> {
            @Override
            public Set<TopicPartition> apply(T map) {
                return map.keySet();
            }
        }
        return futures.thenApply(new Function());
    }

    /**
     * Return a future which succeeds if all the topic elections succeed.
     */
    public KafkaFuture<Void> all() {
        class Function<T extends Map<TopicPartition, ? extends KafkaFuture<Void>>> extends KafkaFuture.Function<T, KafkaFuture<Void>> {

            @Override
            public KafkaFuture<Void> apply(T map) {
                return KafkaFuture.allOf(map.values().toArray(new KafkaFuture[0]));
            }
        }
        return futures.thenCompose(new Function());
    }
}

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
import java.util.concurrent.ExecutionException;

/**
 * The result of {@link AdminClient#electPreferredLeaders(Collection, ElectPreferredLeadersOptions)}
 *
 * <p>The {@code KafkaFuture}s available from instances of this class may be completed
 * exceptionally due to:</p>
 * <ul>
 *   <li>{@link org.apache.kafka.common.protocol.Errors#CLUSTER_AUTHORIZATION_FAILED CLUSTER_AUTHORIZATION_FAILED} if the authenticated user didn't have {@code Alter} access to the cluster.</li>
 *   <li>{@link org.apache.kafka.common.protocol.Errors#UNKNOWN_TOPIC_OR_PARTITION UNKNOWN_TOPIC_OR_PARTITION} if the topic or partition did not exist within the cluster.</li>
 *   <li>{@link org.apache.kafka.common.protocol.Errors#INVALID_TOPIC_EXCEPTION INVALID_TOPIC_EXCEPTION} if the topic was already queued for deletion.</li>
 *   <li>{@link org.apache.kafka.common.protocol.Errors#NOT_CONTROLLER NOT_CONTROLLER} if the request was sent to a broker that was not the controller for the cluster.</li>
 *   <li>{@link org.apache.kafka.common.protocol.Errors#REQUEST_TIMED_OUT REQUEST_TIMED_OUT} if the request timed out before the election was complete.</li>
 *   <li>{@link org.apache.kafka.common.protocol.Errors#UNKNOWN_SERVER_ERROR UNKNOWN_SERVER_ERROR} if the preferred leader was not alive or not in the ISR.</li>
 * </ul>
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ElectPreferredLeadersResult {

    private final KafkaFuture<? extends Map<TopicPartition, ? extends KafkaFuture<Void>>> futures;

    ElectPreferredLeadersResult(KafkaFuture<? extends Map<TopicPartition, ? extends KafkaFuture<Void>>> futures) {
        this.futures = futures;
    }

    /** Return a new future that has completed exceptionally */
    private static <T> KafkaFuture<T> exceptionalFuture(Throwable e) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(e);
        return future;
    }

    /**
     * Get the result of the election for the given {@code partition}.
     * If there was not an election triggered for the given {@code partition}, the
     * returned future will complete with an error.
     */
    public KafkaFuture<Void> partitionResult(TopicPartition partition) {
        final Map<TopicPartition, ? extends KafkaFuture<Void>> map;
        try {
            map = futures.get();
        } catch (InterruptedException | ExecutionException e) {
            return exceptionalFuture(e);
        }
        KafkaFuture<Void> result = map.get(partition);
        if (result == null) {
            KafkaFutureImpl future = new KafkaFutureImpl<>();
            future.completeExceptionally(new IllegalArgumentException(
                    "Preferred leader election for partition \"" + partition + "\" was not attempted"));
            result = future;
        }
        return result;
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
    public KafkaFuture<Collection<TopicPartition>> partitions() {
        final Map<TopicPartition, ? extends KafkaFuture<Void>> map;
        try {
            map = futures.get();
        } catch (InterruptedException e) {
            return exceptionalFuture(e);
        } catch (ExecutionException e) {
            return exceptionalFuture(e.getCause());
        }
        KafkaFutureImpl<Collection<TopicPartition>> result = new KafkaFutureImpl<>();
        result.complete(map.keySet());
        return result;
    }

    /**
     * Return a future which succeeds if all the topic elections succeed.
     */
    public KafkaFuture<Void> all() {
        final Map<TopicPartition, ? extends KafkaFuture<Void>> map;
        try {
            map = futures.get();
        } catch (InterruptedException | ExecutionException e) {
            return exceptionalFuture(e);
        }
        return KafkaFuture.allOf(map.values().toArray(new KafkaFuture[0]));
    }
}

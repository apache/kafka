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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceAudience;

import java.time.Duration;
import java.util.Collection;

/**
 * A callback interface that the user can implement to trigger custom actions when the set of partitions assigned to the
 * consumer changes.
 * <p>
 * This is applicable when the consumer is having Kafka auto-manage group membership. If the consumer directly assigns partitions,
 * those partitions will never be reassigned and this callback is not applicable.
 * <p>
 * When Kafka is managing the group membership, a partition re-assignment will be triggered any time the members of the group change or the subscription
 * of the members changes. This can occur when processes die, new process instances are added or old instances come back to life after failure.
 * Partition re-assignments can also be triggered by changes affecting the subscribed topics (e.g. when the number of partitions is
 * administratively adjusted).
 *
 * <h3>Consumer-Aware Callbacks</h3>
 *
 * Each callback method receives a {@link RebalanceConsumer}, a restricted view of the {@link Consumer} that exposes
 * only operations safe to invoke during a rebalance (e.g. offset commits, seeks, position queries). The
 * {@code RebalanceConsumer} is valid only for the duration of the callback; storing a reference and using it later
 * will throw {@link IllegalStateException}.
 * <p>
 * Prefer using the provided {@code RebalanceConsumer} instead of capturing the full {@link Consumer} reference
 * externally. This avoids accidental use of operations like {@code poll()}, {@code close()}, or {@code subscribe()}
 * that could corrupt consumer state mid-rebalance.
 *
 * <h3>Common Use Cases</h3>
 *
 * There are many uses for this functionality. One common use is saving offsets in a custom store. By saving offsets in
 * the {@link #onPartitionsRevoked(Collection, RebalanceConsumer)} call we can ensure that any time partition assignment changes
 * the offset gets saved.
 * <p>
 * Another use is flushing out any kind of cache of intermediate results the consumer may be keeping. For example,
 * consider a case where the consumer is subscribed to a topic containing user page views, and the goal is to count the
 * number of page views per user for each five minute window. Let's say the topic is partitioned by the user id so that
 * all events for a particular user go to a single consumer instance. The consumer can keep in memory a running
 * tally of actions per user and only flush these out to a remote data store when its cache gets too big. However if a
 * partition is reassigned it may want to automatically trigger a flush of this cache, before the new owner takes over
 * consumption.
 * <p>
 * This callback will only execute in the user thread as part of the {@link KafkaConsumer#poll(java.time.Duration) poll(Duration)} call
 * whenever partition assignment changes.
 * <p>
 * Under normal conditions, if a partition is reassigned from one consumer to another, then the old consumer will
 * always invoke {@link #onPartitionsRevoked(Collection, RebalanceConsumer) onPartitionsRevoked} for that partition
 * prior to the new consumer invoking {@link #onPartitionsAssigned(Collection, RebalanceConsumer) onPartitionsAssigned}
 * for the same partition. So if offsets or other state is saved durably in the
 * {@link #onPartitionsRevoked(Collection, RebalanceConsumer) onPartitionsRevoked} callback by one consumer member,
 * it will always be accessible by the time the other consumer member taking over that partition triggers its
 * {@link #onPartitionsAssigned(Collection, RebalanceConsumer) onPartitionsAssigned} callback to load the state.
 * <p>
 * You can think of revocation as a graceful way to give up ownership of a partition. In some cases, the consumer may not have an opportunity to do so.
 * For example, if the session times out, then the partitions may be reassigned before we have a chance to revoke them gracefully.
 * For this case, we have a third callback {@link #onPartitionsLost(Collection, RebalanceConsumer)}. The difference between this function and
 * {@link #onPartitionsRevoked(Collection, RebalanceConsumer)} is that upon invocation of {@link #onPartitionsLost(Collection, RebalanceConsumer)}, the partitions
 * may already be owned by some other members in the group and therefore users would not be able to commit its consumed offsets for example.
 * Users could implement these two functions differently (by default,
 * {@link #onPartitionsLost(Collection, RebalanceConsumer)} will be calling {@link #onPartitionsRevoked(Collection, RebalanceConsumer)} directly); for example, in the
 * {@link #onPartitionsLost(Collection, RebalanceConsumer)} we should not need to store the offsets since we know these partitions are no longer owned by the consumer
 * at that time.
 * <p>
 * During a rebalance event, the {@link #onPartitionsAssigned(Collection, RebalanceConsumer) onPartitionsAssigned} function will always be triggered exactly once when
 * the rebalance completes. That is, even if there are no newly assigned partitions for a consumer member, its {@link #onPartitionsAssigned(Collection, RebalanceConsumer) onPartitionsAssigned}
 * will still be triggered with an empty collection of partitions. As a result this function can be used also to notify when a rebalance event has happened.
 * With eager rebalancing, {@link #onPartitionsRevoked(Collection, RebalanceConsumer)} will always be called at the start of a rebalance. On the other hand, {@link #onPartitionsLost(Collection, RebalanceConsumer)}
 * will only be called when there were non-empty partitions that were lost.
 * With cooperative rebalancing, {@link #onPartitionsRevoked(Collection, RebalanceConsumer)} and {@link #onPartitionsLost(Collection, RebalanceConsumer)}
 * will only be triggered when there are non-empty partitions revoked or lost from this consumer member during a rebalance event.
 * <p>
 * It is possible
 * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
 * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
 * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
 * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
 * Also if the callback function implementation itself throws an exception, this exception will be propagated to the current
 * invocation of {@link KafkaConsumer#poll(java.time.Duration)} as well.
 * <p>
 * Note that callbacks only serve as notification of an assignment change.
 * They cannot be used to express acceptance of the change.
 * Hence throwing an exception from a callback does not affect the assignment in any way,
 * as it will be propagated all the way up to the {@link KafkaConsumer#poll(java.time.Duration)} call.
 * If user captures the exception in the caller, the callback is still assumed successful and no further retries will be attempted.
 *
 * <h3>Relation to {@link ConsumerRebalanceListener}</h3>
 *
 * {@code RebalanceListener} is the preferred entry point for new code. The legacy {@link ConsumerRebalanceListener}
 * interface extends this one and adds one-argument variants of each callback for source compatibility with
 * pre-existing implementations. New code should implement {@code RebalanceListener} directly and register it via
 * {@code Consumer.setRebalanceListener(RebalanceListener)}.
 * <p>
 *
 * Here is pseudo-code for a callback implementation for saving offsets:
 * <pre>
 * {@code
 *   consumer.setRebalanceListener(new RebalanceListener() {
 *       @Override
 *       public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer rebalanceConsumer) {
 *           // save the offsets in an external store using some custom code not described here
 *           for(TopicPartition partition: partitions)
 *              saveOffsetInExternalStore(rebalanceConsumer.position(partition));
 *       }
 *
 *       @Override
 *       public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer rebalanceConsumer) {
 *           // do not need to save the offsets since these partitions are probably owned by other consumers already
 *       }
 *
 *       @Override
 *       public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rebalanceConsumer) {
 *           // read the offsets from an external store using some custom code not described here
 *           for(TopicPartition partition: partitions)
 *              rebalanceConsumer.seek(partition, readOffsetFromExternalStore(partition));
 *       }
 *   });
 *   consumer.subscribe(List.of("topic-1", "topic-2"));
 * }
 * </pre>
 *
 * @see RebalanceConsumer
 * @see ConsumerRebalanceListener
 */
@InterfaceAudience.Public
public interface RebalanceListener {

    /**
     * A callback method the user can implement to provide handling of offset commits to a customized store.
     * This method will be called during a rebalance operation when the consumer has to give up some partitions.
     * The consumer may need to give up some partitions (thus this callback executed) under the following scenarios:
     * <ul>
     *     <li>If the consumer assignment changes</li>
     *     <li>If the consumer is being closed ({@link KafkaConsumer#close(CloseOptions option)})</li>
     *     <li>If the consumer is unsubscribing ({@link KafkaConsumer#unsubscribe()})</li>
     * </ul>
     * It is recommended that offsets should be committed in this callback to either Kafka or a
     * custom offset store to prevent duplicate data.
     * <p>
     * This callback is always called before re-assigning the partitions.
     * If the consumer is using the {@link GroupProtocol#CLASSIC} rebalance protocol:
     * <ul>
     *     <li>
     *         In eager rebalancing, onPartitionsRevoked will be called with the full set of assigned partitions as a parameter (all partitions are revoked).
     *         It will be called even if there are no partitions to revoke.
     *     </li>
     *     <li>
     *         In cooperative rebalancing, onPartitionsRevoked will be called with the set of partitions to revoke,
     *         if and only if the set is non-empty.
     *     </li>
     * </ul>
     * If the consumer is using the {@link GroupProtocol#CONSUMER} rebalance protocol, this callback will be called
     * with the set of partitions to revoke if and only if the set is non-empty
     * (same behavior as the {@link GroupProtocol#CLASSIC} rebalance protocol with Cooperative mode).
     * <p>
     * For examples on usage of this API, see Usage Examples section of {@link KafkaConsumer KafkaConsumer}.
     * <p>
     * It is common for the revocation callback to use the consumer instance in order to commit offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     * <p>
     * The {@link RebalanceConsumer} parameter exposes only operations safe during a rebalance (e.g. offset commits,
     * seeks, position queries). It is valid only for the duration of this callback; storing a reference and using it
     * later will throw {@link IllegalStateException}.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked. This will
     *                   include the full assignment under the Classic/Eager protocol, given that it revokes all partitions.
     *                   It will only include the subset to revoke under the Classic/Cooperative and Consumer protocols.
     * @param consumer   A restricted view of the {@link Consumer} that is only valid for the duration of this callback.
     *                   See {@link RebalanceConsumer} for the set of permitted operations.
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link RebalanceConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link RebalanceConsumer}
     */
    void onPartitionsRevoked(Collection<TopicPartition> partitions,
                             RebalanceConsumer consumer);

    /**
     * A callback method the user can implement to provide handling of customized offsets on completion of a successful
     * partition re-assignment. This method will be called after the partition re-assignment completes (even if no new
     * partitions were assigned to the consumer), and before the consumer starts fetching data,
     * and only as the result of a {@link KafkaConsumer#poll(Duration) poll(Duration)} call.
     * <p>
     * It is guaranteed that under normal conditions all the processes in a consumer group will execute their
     * {@link #onPartitionsRevoked(Collection, RebalanceConsumer)} callback before any instance executes this
     * onPartitionsAssigned callback. During exceptional scenarios, partitions may be migrated
     * without the old owner being notified (i.e. their {@link #onPartitionsRevoked(Collection, RebalanceConsumer)}
     * callback not triggered), and later when the old owner consumer realized this event, the
     * {@link #onPartitionsLost(Collection, RebalanceConsumer)} callback will be triggered by the consumer then.
     * <p>
     * It is common for the assignment callback to use the consumer instance in order to query offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     * <p>
     * The {@link RebalanceConsumer} parameter exposes only operations safe during a rebalance (e.g. seeking to
     * externally stored offsets, pausing partitions until caches warm up). It is valid only for the duration of this
     * callback; storing a reference and using it later will throw {@link IllegalStateException}.
     *
     * @param partitions Partitions that have been added to the assignment as a result of the rebalance.
     *                   Note that partitions that were already owned by this consumer and remain assigned are not
     *                   included in this list under the Classic/Cooperative or Consumer protocols. The full assignment
     *                   will be received under the Classic/Eager protocol.
     * @param consumer   A restricted view of the {@link Consumer} that is only valid for the duration of this callback.
     *                   See {@link RebalanceConsumer} for the set of permitted operations.
     * @throws org.apache.kafka.common.errors.WakeupException    If raised from a nested call to {@link RebalanceConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link RebalanceConsumer}
     */
    void onPartitionsAssigned(Collection<TopicPartition> partitions,
                              RebalanceConsumer consumer);

    /**
     * A callback method you can implement to provide handling of cleaning up resources for partitions that have already
     * been reassigned to other consumers. This method will not be called during normal execution as the owned partitions would
     * first be revoked by calling {@link #onPartitionsRevoked(Collection, RebalanceConsumer)}, before being reassigned
     * to other consumers during a rebalance event. However, during exceptional scenarios when the consumer realized that it
     * does not own this partition any longer, i.e. not revoked via a normal rebalance event, then this method would be invoked.
     * <p>
     * For example, this function is called if a consumer's session timeout has expired, or if a fatal error has been
     * received indicating the consumer is no longer part of the group.
     * <p>
     * By default it will just trigger {@link #onPartitionsRevoked(Collection, RebalanceConsumer)}; for users who want
     * to distinguish the handling logic of revoked partitions v.s. lost partitions, they can override the default
     * implementation.
     * <p>
     * It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     * <p>
     * The {@link RebalanceConsumer} parameter is valid only for the duration of this callback; storing a reference
     * and using it later will throw {@link IllegalStateException}.
     *
     * <h4>Important difference from {@code onPartitionsRevoked}</h4>
     *
     * Unlike {@link #onPartitionsRevoked(Collection, RebalanceConsumer)}, where the affected
     * partitions are still part of the consumer's assignment until the callback completes, the
     * partitions passed to this method have <em>already been removed</em> from the assignment
     * before this callback fires. This means:
     *
     * <ul>
     *   <li>{@link RebalanceConsumer#commitSync() commitSync()}/{@link RebalanceConsumer#commitAsync() commitAsync()}
     *       will not commit the current assignment, since the consumer no longer includes the lost partitions.</li>
     *   <li>{@link RebalanceConsumer#commitSync(java.util.Map) commitSync(offsets)}
     *       /{@link RebalanceConsumer#commitAsync(java.util.Map, OffsetCommitCallback) commitAsync(offsets)}
     *       with the lost partitions explicitly will be rejected by the broker since the consumer
     *       no longer owns them.</li>
     *   <li>{@link RebalanceConsumer#position(org.apache.kafka.common.TopicPartition) position()} and
     *       {@link RebalanceConsumer#seek(org.apache.kafka.common.TopicPartition, long) seek()} will
     *       fail for the lost partitions since they are not in the assignment.</li>
     * </ul>
     * <p>
     * The {@link RebalanceConsumer} does not add guardrails against these operations on lost
     * partitions. Users should be aware that offset management for lost partitions is not
     * possible in this callback.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now have been reassigned
     *                   to other consumers. With both the Classic and Consumer protocols, this will always include
     *                   all partitions that were previously assigned to the consumer.
     * @param consumer   A restricted view of the {@link Consumer} that is only valid for the duration of this callback.
     *                   See {@link RebalanceConsumer} for the set of permitted operations.
     * @throws org.apache.kafka.common.errors.WakeupException    If raised from a nested call to {@link RebalanceConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link RebalanceConsumer}
     */
    default void onPartitionsLost(Collection<TopicPartition> partitions,
                                  RebalanceConsumer consumer) {
        onPartitionsRevoked(partitions, consumer);
    }
}

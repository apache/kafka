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

import java.time.Duration;
import java.util.Collection;

import org.apache.kafka.common.TopicPartition;

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
 * <p>
 * There are many uses for this functionality. One common use is saving offsets in a custom store. By saving offsets in
 * the {@link #onPartitionsRevoked(Collection)} call we can ensure that any time partition assignment changes
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
 * This callback will only execute in the user thread as part of the {@link Consumer#poll(java.time.Duration) poll(long)} call
 * whenever partition assignment changes.
 * <p>
 * Under normal conditions, if a partition is reassigned from one consumer to another, then the old consumer will
 * always invoke {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} for that partition prior to the new consumer
 * invoking {@link #onPartitionsAssigned(Collection) onPartitionsAssigned} for the same partition. So if offsets or other state is saved in the
 * {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} call by one consumer member, it will be always accessible by the time the
 * other consumer member taking over that partition and triggering its {@link #onPartitionsAssigned(Collection) onPartitionsAssigned} callback to load the state.
 * <p>
 * You can think of revocation as a graceful way to give up ownership of a partition. In some cases, the consumer may not have an opportunity to do so.
 * For example, if the session times out, then the partitions may be reassigned before we have a chance to revoke them gracefully.
 * For this case, we have a third callback {@link #onPartitionsLost(Collection)}. The difference between this function and
 * {@link #onPartitionsRevoked(Collection)} is that upon invocation of {@link #onPartitionsLost(Collection)}, the partitions
 * may already be owned by some other members in the group and therefore users would not be able to commit its consumed offsets for example.
 * Users could implement these two functions differently (by default,
 * {@link #onPartitionsLost(Collection)} will be calling {@link #onPartitionsRevoked(Collection)} directly); for example, in the
 * {@link #onPartitionsLost(Collection)} we should not need to store the offsets since we know these partitions are no longer owned by the consumer
 * at that time.
 * <p>
 * During a rebalance event, the {@link #onPartitionsAssigned(Collection) onPartitionsAssigned} function will always be triggered exactly once when
 * the rebalance completes. That is, even if there is no newly assigned partitions for a consumer member, its {@link #onPartitionsAssigned(Collection) onPartitionsAssigned}
 * will still be triggered with an empty collection of partitions. As a result this function can be used also to notify when a rebalance event has happened.
 * With eager rebalancing, {@link #onPartitionsRevoked(Collection)} will always be called at the start of a rebalance. On the other hand, {@link #onPartitionsLost(Collection)}
 * will only be called when there were non-empty partitions that were lost.
 * With cooperative rebalancing, {@link #onPartitionsRevoked(Collection)} and {@link #onPartitionsLost(Collection)}
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
 * <p>
 *
 * Here is pseudo-code for a callback implementation for saving offsets:
 * <pre>
 * {@code
 *   public class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
 *       private Consumer<?,?> consumer;
 *
 *       public SaveOffsetsOnRebalance(Consumer<?,?> consumer) {
 *           this.consumer = consumer;
 *       }
 *
 *       public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *           // save the offsets in an external store using some custom code not described here
 *           for(TopicPartition partition: partitions)
 *              saveOffsetInExternalStore(consumer.position(partition));
 *       }
 *
 *       public void onPartitionsLost(Collection<TopicPartition> partitions) {
 *           // do not need to save the offsets since these partitions are probably owned by other consumers already
 *       }
 *
 *       public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
 *           // read the offsets from an external store using some custom code not described here
 *           for(TopicPartition partition: partitions)
 *              consumer.seek(partition, readOffsetFromExternalStore(partition));
 *       }
 *   }
 * }
 * </pre>
 */
public interface ConsumerRebalanceListener {

    /**
     * A callback method the user can implement to provide handling of offset commits to a customized store.
     * This method will be called during a rebalance operation when the consumer has to give up some partitions.
     * It can also be called when consumer is being closed ({@link KafkaConsumer#close(Duration)})
     * or is unsubscribing ({@link KafkaConsumer#unsubscribe()}).
     * It is recommended that offsets should be committed in this callback to either Kafka or a
     * custom offset store to prevent duplicate data.
     * <p>
     * In eager rebalancing, it will always be called at the start of a rebalance and after the consumer stops fetching data.
     * In cooperative rebalancing, it will be called at the end of a rebalance on the set of partitions being revoked iff the set is non-empty.
     * For examples on usage of this API, see Usage Examples section of {@link KafkaConsumer KafkaConsumer}.
     * <p>
     * It is common for the revocation callback to use the consumer instance in order to commit offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked (may not
     *                   include all currently assigned partitions, i.e. there may still be some partitions left)
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link KafkaConsumer}
     */
    void onPartitionsRevoked(Collection<TopicPartition> partitions);

    /**
     * A callback method the user can implement to provide handling of customized offsets on completion of a successful
     * partition re-assignment. This method will be called after the partition re-assignment completes and before the
     * consumer starts fetching data, and only as the result of a {@link Consumer#poll(java.time.Duration) poll(long)} call.
     * <p>
     * It is guaranteed that under normal conditions all the processes in a consumer group will execute their
     * {@link #onPartitionsRevoked(Collection)} callback before any instance executes its
     * {@link #onPartitionsAssigned(Collection)} callback. During exceptional scenarios, partitions may be migrated
     * without the old owner being notified (i.e. their {@link #onPartitionsRevoked(Collection)} callback not triggered),
     * and later when the old owner consumer realized this event, the {@link #onPartitionsLost(Collection)} (Collection)} callback
     * will be triggered by the consumer then.
     * <p>
     * It is common for the assignment callback to use the consumer instance in order to query offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * @param partitions The list of partitions that are now assigned to the consumer (previously owned partitions will
     *                   NOT be included, i.e. this list will only include newly added partitions)
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link KafkaConsumer}
     */
    void onPartitionsAssigned(Collection<TopicPartition> partitions);

    /**
     * A callback method you can implement to provide handling of cleaning up resources for partitions that have already
     * been reassigned to other consumers. This method will not be called during normal execution as the owned partitions would
     * first be revoked by calling the {@link ConsumerRebalanceListener#onPartitionsRevoked}, before being reassigned
     * to other consumers during a rebalance event. However, during exceptional scenarios when the consumer realized that it
     * does not own this partition any longer, i.e. not revoked via a normal rebalance event, then this method would be invoked.
     * <p>
     * For example, this function is called if a consumer's session timeout has expired, or if a fatal error has been
     * received indicating the consumer is no longer part of the group.
     * <p>
     * By default it will just trigger {@link ConsumerRebalanceListener#onPartitionsRevoked}; for users who want to distinguish
     * the handling logic of revoked partitions v.s. lost partitions, they can override the default implementation.
     * <p>
     * It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one of these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now have been reassigned
     *                   to other consumers. With the current protocol this will always include all of the consumer's
     *                   previously assigned partitions, but this may change in future protocols (ie there would still
     *                   be some partitions left)
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link KafkaConsumer}
     */
    default void onPartitionsLost(Collection<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
    }
}

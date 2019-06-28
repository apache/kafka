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
 * of the members changes. This can occur when processes die, new process instances are added or old instances come back to life after failure causing
 * a rebalance event.
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
 * It is guaranteed that for any partition, if a partition is reassigned from one consumer to another, then the old consumer will
 * always invoke {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} for that partition prior to the new consumer
 * invoking {@link #onPartitionsAssigned(Collection) onPartitionsAssigned} for the same partition. So if offsets or other state is saved in the
 * {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} call by one consumer member, it will be always accessible by the time the
 * other consumer member taking over that partition and triggering its {@link #onPartitionsAssigned(Collection) onPartitionsAssigned} callback to load the state.
 * <p>
 * In addition, a third {@link #onPartitionsLost(Collection)} callback will be invoked when some partitions previously owned by the consumer
 * are lost due to unexpected events, for example, the consumer finding some of its assigned partitions no longer
 * exist from its topic metadata due to administrative operations like delete topics, or the consumer realizing it is
 * no longer part of the consumer group due to soft failures and therefore all of its previously owned partitions have reassigned to other consumers already.
 * Note this function is very different
 * to the {@link #onPartitionsRevoked(Collection)} function: the latter is invoked when a partition is revoked
 * from the consumer who still owns it right before the invocation, while the former is invoked when a partition is already lost
 * and not owned by the consumer anymore during the invocation. Users then could implement these two functions differently (by default,
 * {@link #onPartitionsLost(Collection)} will be calling {@link #onPartitionsRevoked(Collection)} directly); for example, in the
 * {@link #onPartitionsLost(Collection)} we should not need to store the offsets since we know these partitions are no longer owned by the consumer
 * at that time.
 * <p>
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
     * A callback method the user can implement to provide handling of offset commits to a customized store on the start
     * of a rebalance operation. This method will be called before a rebalance operation starts and after the consumer
     * stops fetching data. It is recommended that offsets should be committed in this callback to either Kafka or a
     * custom offset store to prevent duplicate data.
     * <p>
     * For examples on usage of this API, see Usage Examples section of {@link KafkaConsumer KafkaConsumer}
     * <p>
     * <b>NOTE:</b> This method is only called before rebalances. It is not called prior to {@link KafkaConsumer#close()}.
     * <p>
     * It is common for the revocation callback to use the consumer instance in order to commit offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link KafkaConsumer}
     */
    void onPartitionsRevoked(Collection<TopicPartition> partitions);

    /**
     * A callback method the user can implement to provide handling of customized offsets on completion of a successful
     * partition re-assignment. This method will be called after the partition re-assignment completes and before the
     * consumer starts fetching data, and only as the result of a {@link Consumer#poll(java.time.Duration) poll(long)} call.
     * <p>
     * It is guaranteed that all the processes in a consumer group will execute their
     * {@link #onPartitionsRevoked(Collection)} callback before any instance executes its
     * {@link #onPartitionsAssigned(Collection)} callback.
     * <p>
     * It is common for the assignment callback to use the consumer instance in order to query offsets. It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * @param partitions The list of partitions that are now assigned to the consumer (may include partitions previously
     *            assigned to the consumer)
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link KafkaConsumer}
     */
    void onPartitionsAssigned(Collection<TopicPartition> partitions);

    /**
     * A callback method you can implement to provide handling of cleaning up resources for partitions that have already
     * been re-assigned to other consumers. This method will not be called during normal execution as the owned partitions would
     * first be revoked by calling the {@link ConsumerRebalanceListener#onPartitionsRevoked}, before being re-assigned
     * to other consumers during a rebalance event. However, during exceptional scenarios when the consumer realized that it
     * does not own this partition any longer, i.e. not revoked via a normal rebalance event, then this method would be invoked.
     *
     * For example, if a consumer is kicked out of the group in the previous generation and hence were not aware when the new
     * group is formed and partitions assigned to other members; later when it finally be notified about the illegal generation
     * error and are going to reset its generation and re-join, this function will be called.
     *
     * By default it will just trigger {@link ConsumerRebalanceListener#onPartitionsRevoked}; for advanced users who want to distinguish
     * the handling logic of revoked partitions v.s. lost partitions, they can override the default implementation.
     *
     * It is possible
     * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
     * to be raised from one these nested invocations. In this case, the exception will be propagated to the current
     * invocation of {@link KafkaConsumer#poll(java.time.Duration)} in which this callback is being executed. This means it is not
     * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now have been re-assigned
     *                  to other consumers
     * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link KafkaConsumer}
     * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link KafkaConsumer}
     */
    default void onPartitionsLost(Collection<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
 * of the members changes. This can occur when processes die, new process instances are added or old instances come back to life after failure.
 * Rebalances can also be triggered by changes affecting the subscribed topics (e.g. when the number of partitions is
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
 * This callback will execute in the user thread as part of the {@link Consumer#poll(long) poll(long)} call whenever partition assignment changes.
 * <p>
 * It is guaranteed that all consumer processes will invoke {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} prior to
 * any process invoking {@link #onPartitionsAssigned(Collection) onPartitionsAssigned}. So if offsets or other state is saved in the
 * {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} call it is guaranteed to be saved by the time the process taking over that
 * partition has their {@link #onPartitionsAssigned(Collection) onPartitionsAssigned} callback called to load the state.
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
     *
     * @param partitions The list of partitions that were assigned to the consumer on the last rebalance
     */
    public void onPartitionsRevoked(Collection<TopicPartition> partitions);

    /**
     * A callback method the user can implement to provide handling of customized offsets on completion of a successful
     * partition re-assignment. This method will be called after an offset re-assignment completes and before the
     * consumer starts fetching data.
     * <p>
     * It is guaranteed that all the processes in a consumer group will execute their
     * {@link #onPartitionsRevoked(Collection)} callback before any instance executes its
     * {@link #onPartitionsAssigned(Collection)} callback.
     *
     * @param partitions The list of partitions that are now assigned to the consumer (may include partitions previously
     *            assigned to the consumer)
     */
    public void onPartitionsAssigned(Collection<TopicPartition> partitions);
}

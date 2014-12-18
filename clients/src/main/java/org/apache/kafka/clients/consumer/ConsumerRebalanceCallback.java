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
 * A callback interface that the user can implement to manage customized offsets on the start and end of 
 * every rebalance operation. This callback will execute in the user thread as part of the 
 * {@link Consumer#poll(long) poll(long)} API on every rebalance attempt.
 * Default implementation of the callback will {@link Consumer#seek(java.util.Map) seek(offsets)} to the last committed offsets in the
 * {@link #onPartitionsAssigned(Consumer, Collection) onPartitionsAssigned()} callback. And will commit offsets synchronously
 * for the specified list of partitions to Kafka in the {@link #onPartitionsRevoked(Consumer, Collection) onPartitionsRevoked()}
 * callback.
 */
public interface ConsumerRebalanceCallback {

    /**
     * A callback method the user can implement to provide handling of customized offsets on completion of a successful 
     * rebalance operation. This method will be called after a rebalance operation completes and before the consumer 
     * starts fetching data.
     * <p> 
     * For examples on usage of this API, see Usage Examples section of {@link KafkaConsumer KafkaConsumer}  
     * @param partitions The list of partitions that are assigned to the consumer after rebalance
     */
    public void onPartitionsAssigned(Consumer<?,?> consumer, Collection<TopicPartition> partitions);
    
    /**
     * A callback method the user can implement to provide handling of offset commits to a customized store on the 
     * start of a rebalance operation. This method will be called before a rebalance operation starts and after the 
     * consumer stops fetching data. It is recommended that offsets should be committed in this callback to 
     * either Kafka or a custom offset store to prevent duplicate data 
     * <p> 
     * For examples on usage of this API, see Usage Examples section of {@link KafkaConsumer KafkaConsumer}  
     * @param partitions The list of partitions that were assigned to the consumer on the last rebalance
     */
    public void onPartitionsRevoked(Consumer<?,?> consumer, Collection<TopicPartition> partitions);
}

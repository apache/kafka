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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * TODO: Clean this after the consumer implementation is complete. Until then, it is useful to write some sample test code using the new APIs
 *
 */
public class ConsumerExampleTest {
    /**
     * This example demonstrates how to use the consumer to leverage Kafka's group management functionality for automatic consumer load 
     * balancing and failure detection. This example assumes that the offsets are stored in Kafka and are automatically committed periodically, 
     * as controlled by the auto.commit.interval.ms config 
     */
//    @Test
//    public void testConsumerGroupManagementWithAutoOffsetCommits() {
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("group.id", "test");
//        props.put("session.timeout.ms", "1000");
//        props.put("auto.commit.enable", "true");
//        props.put("auto.commit.interval.ms", "10000");
//        KafkaConsumer consumer = new KafkaConsumer(props);
//        // subscribe to some topics
//        consumer.subscribe("foo", "bar");
//        boolean isRunning = true;
//        while(isRunning) {
//            Map<String, ConsumerRecords> records = consumer.poll(100);
//            process(records);
//        }
//        consumer.close();
//    }

    /**
     * This example demonstrates how to use the consumer to leverage Kafka's group management functionality for automatic consumer load 
     * balancing and failure detection. This example assumes that the offsets are stored in Kafka and are manually committed using the 
     * commit() API. This example also demonstrates rewinding the consumer's offsets if processing of consumed messages fails.
     */
//     @Test
//     public void testConsumerGroupManagementWithManualOffsetCommit() {
//         Properties props = new Properties();
//         props.put("metadata.broker.list", "localhost:9092");
//         props.put("group.id", "test");
//         props.put("session.timeout.ms", "1000");
//         props.put("auto.commit.enable", "false");
//         KafkaConsumer consumer = new KafkaConsumer(props);
//         // subscribe to some topics
//         consumer.subscribe("foo", "bar");
//         int commitInterval = 100;
//         int numRecords = 0;
//         boolean isRunning = true;
//         Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();
//         while(isRunning) {
//             Map<String, ConsumerRecords> records = consumer.poll(100);
//             try {
//                 Map<TopicPartition, Long> lastConsumedOffsets = process(records);
//                 consumedOffsets.putAll(lastConsumedOffsets);
//                 numRecords += records.size();
//                 // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
//                 if(numRecords % commitInterval == 0) 
//                     consumer.commit(true);
//             } catch(Exception e) {
//                 // rewind consumer's offsets for failed partitions
//                 List<TopicPartition> failedPartitions = getFailedPartitions();
//                 Map<TopicPartition, Long> offsetsToRewindTo = new HashMap<TopicPartition, Long>();
//                 for(TopicPartition failedPartition : failedPartitions) {
//                     // rewind to the last consumed offset for the failed partition. Since process() failed for this partition, the consumed offset
//                     // should still be pointing to the last successfully processed offset and hence is the right offset to rewind consumption to.
//                     offsetsToRewindTo.put(failedPartition, consumedOffsets.get(failedPartition));
//                 }
//                 // seek to new offsets only for partitions that failed the last process()
//                 consumer.seek(offsetsToRewindTo);
//             }
//         }         
//         consumer.close();
//     }

     private List<TopicPartition> getFailedPartitions() { return null; }

    /**
     * This example demonstrates the consumer can be used to leverage Kafka's group management functionality along with custom offset storage. 
     * In this example, the assumption made is that the user chooses to store the consumer offsets outside Kafka. This requires the user to 
     * plugin logic for retrieving the offsets from a custom store and provide the offsets to the consumer in the ConsumerRebalanceCallback
     * callback. The onPartitionsAssigned callback is invoked after the consumer is assigned a new set of partitions on rebalance <i>and</i>
     * before the consumption restarts post rebalance. This is the right place to supply offsets from a custom store to the consumer.
     */
//    @Test
//    public void testConsumerRebalanceWithCustomOffsetStore() {
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("group.id", "test");
//        props.put("session.timeout.ms", "1000");
//        props.put("auto.commit.enable", "true");
//        props.put("auto.commit.interval.ms", "10000");
//        KafkaConsumer consumer = new KafkaConsumer(props,
//                                                   new ConsumerRebalanceCallback() {
//                                                        public void onPartitionsAssigned(Consumer consumer, Collection<TopicPartition> partitions) {
//                                                           Map<TopicPartition, Long> lastCommittedOffsets = getLastCommittedOffsetsFromCustomStore(partitions);
//                                                           consumer.seek(lastCommittedOffsets);
//                                                        }
//                                                        public void onPartitionsRevoked(Consumer consumer, Collection<TopicPartition> partitions) {
//                                                            Map<TopicPartition, Long> offsets = getLastConsumedOffsets(partitions); // implemented by the user
//                                                           commitOffsetsToCustomStore(offsets);                                            // implemented by the user
//                                                        }
//                                                        private Map<TopicPartition, Long> getLastCommittedOffsetsFromCustomStore(Collection<TopicPartition> partitions) {
//                                                            return null;
//                                                        }
//                                                        private Map<TopicPartition, Long> getLastConsumedOffsets(Collection<TopicPartition> partitions) { return null; }
//                                                        private void commitOffsetsToCustomStore(Map<TopicPartition, Long> offsets) {}
//                                                    });
//        // subscribe to topics
//        consumer.subscribe("foo", "bar");
//        int commitInterval = 100;
//        int numRecords = 0;
//        boolean isRunning = true;
//        while(isRunning) {
//            Map<String, ConsumerRecords> records = consumer.poll(100);
//            Map<TopicPartition, Long> consumedOffsets = process(records);
//            numRecords += records.size();
//            // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
//            if(numRecords % commitInterval == 0) 
//                commitOffsetsToCustomStore(consumedOffsets);
//        }
//        consumer.close();
//    }

    /**
     * This example demonstrates how the consumer can be used to leverage Kafka's group management functionality along with Kafka based offset storage. 
     * In this example, the assumption made is that the user chooses to use Kafka based offset management. 
     */
//    @Test
//    public void testConsumerRewindWithGroupManagementAndKafkaOffsetStorage() {
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("group.id", "test");
//        props.put("session.timeout.ms", "1000");
//        props.put("auto.commit.enable", "false");        
//        KafkaConsumer consumer = new KafkaConsumer(props,
//                                                   new ConsumerRebalanceCallback() {
//                                                        boolean rewindOffsets = true;
//                                                        public void onPartitionsAssigned(Consumer consumer, Collection<TopicPartition> partitions) {
//                                                            if(rewindOffsets) {
//                                                            Map<TopicPartition, Long> latestCommittedOffsets = consumer.committed(null);
//                                                            Map<TopicPartition, Long> newOffsets = rewindOffsets(latestCommittedOffsets, 100);
//                                                            consumer.seek(newOffsets);
//                                                            }
//                                                        }
//                                                        public void onPartitionsRevoked(Consumer consumer, Collection<TopicPartition> partitions) {
//                                                            consumer.commit(true);
//                                                        }
//                                                        // this API rewinds every partition back by numberOfMessagesToRewindBackTo messages 
//                                                        private Map<TopicPartition, Long> rewindOffsets(Map<TopicPartition, Long> currentOffsets,
//                                                                                                        long numberOfMessagesToRewindBackTo) {
//                                                            Map<TopicPartition, Long> newOffsets = new HashMap<TopicPartition, Long>();
//                                                            for(Map.Entry<TopicPartition, Long> offset : currentOffsets.entrySet()) {
//                                                                newOffsets.put(offset.getKey(), offset.getValue() - numberOfMessagesToRewindBackTo);
//                                                            }
//                                                            return newOffsets;
//                                                        }
//                                                    });
//        // subscribe to topics
//        consumer.subscribe("foo", "bar");
//        int commitInterval = 100;
//        int numRecords = 0;
//        boolean isRunning = true;
//        while(isRunning) {
//            Map<String, ConsumerRecords> records = consumer.poll(100);
//            Map<TopicPartition, Long> consumedOffsets = process(records);
//            numRecords += records.size();
//            // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
//            if(numRecords % commitInterval == 0) 
//                commitOffsetsToCustomStore(consumedOffsets);
//        }
//        consumer.close();
//    }

    /**
     * This example demonstrates how the consumer can be used to subscribe to specific partitions of certain topics and consume upto the latest
     * available message for each of those partitions before shutting down. When used to subscribe to specific partitions, the user foregoes 
     * the group management functionality and instead relies on manually configuring the consumer instances to subscribe to a set of partitions.
     * This example assumes that the user chooses to use Kafka based offset storage. The user still has to specify a group.id to use Kafka 
     * based offset management. However, session.timeout.ms is not required since the Kafka consumer only does failure detection with group 
     * management.
     */
//    @Test
//    public void testConsumerWithKafkaBasedOffsetManagement() {
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("group.id", "test");
//        props.put("auto.commit.enable", "true");
//        props.put("auto.commit.interval.ms", "10000");
//        KafkaConsumer consumer = new KafkaConsumer(props);
//        // subscribe to some partitions of topic foo
//        TopicPartition partition0 = new TopicPartition("foo", 0);
//        TopicPartition partition1 = new TopicPartition("foo", 1);
//        TopicPartition[] partitions = new TopicPartition[2];
//        partitions[0] = partition0;
//        partitions[1] = partition1;
//        consumer.subscribe(partitions);
//        // find the last committed offsets for partitions 0,1 of topic foo
//        Map<TopicPartition, Long> lastCommittedOffsets = consumer.committed(null);
//        // seek to the last committed offsets to avoid duplicates
//        consumer.seek(lastCommittedOffsets);        
//        // find the offsets of the latest available messages to know where to stop consumption
//        Map<TopicPartition, Long> latestAvailableOffsets = consumer.offsetsBeforeTime(-2, null);
//        boolean isRunning = true;
//        while(isRunning) {
//            Map<String, ConsumerRecords> records = consumer.poll(100);
//            Map<TopicPartition, Long> consumedOffsets = process(records);
//            for(TopicPartition partition : partitions) {
//                if(consumedOffsets.get(partition) >= latestAvailableOffsets.get(partition))
//                    isRunning = false;
//                else
//                    isRunning = true;
//            }
//        }
//        consumer.close();
//    }

    /**
     * This example demonstrates how the consumer can be used to subscribe to specific partitions of certain topics and consume upto the latest
     * available message for each of those partitions before shutting down. When used to subscribe to specific partitions, the user foregoes 
     * the group management functionality and instead relies on manually configuring the consumer instances to subscribe to a set of partitions.
     * This example assumes that the user chooses to use custom offset storage.
     */
    @Test
    public void testConsumerWithCustomOffsetManagement() {
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
//        KafkaConsumer consumer = new KafkaConsumer(props);
//        // subscribe to some partitions of topic foo
//        TopicPartition partition0 = new TopicPartition("foo", 0);
//        TopicPartition partition1 = new TopicPartition("foo", 1);
//        TopicPartition[] partitions = new TopicPartition[2];
//        partitions[0] = partition0;
//        partitions[1] = partition1;
//        consumer.subscribe(partitions);
//        Map<TopicPartition, Long> lastCommittedOffsets = getLastCommittedOffsetsFromCustomStore();
//        // seek to the last committed offsets to avoid duplicates
//        consumer.seek(lastCommittedOffsets);        
//        // find the offsets of the latest available messages to know where to stop consumption
//        Map<TopicPartition, Long> latestAvailableOffsets = consumer.offsetsBeforeTime(-2, null);
//        boolean isRunning = true;
//        while(isRunning) {
//            Map<String, ConsumerRecords> records = consumer.poll(100);
//            Map<TopicPartition, Long> consumedOffsets = process(records);
//            // commit offsets for partitions 0,1 for topic foo to custom store
//            commitOffsetsToCustomStore(consumedOffsets);
//            for(TopicPartition partition : partitions) {
//                if(consumedOffsets.get(partition) >= latestAvailableOffsets.get(partition))
//                    isRunning = false;
//                else
//                    isRunning = true;
//            }            
//        }         
//        consumer.close();
    }

    private Map<TopicPartition, Long> getLastCommittedOffsetsFromCustomStore() { return null; }
    private void commitOffsetsToCustomStore(Map<TopicPartition, Long> consumedOffsets) {}
    private Map<TopicPartition, Long> process(Map<String, ConsumerRecords> records) {
        Map<TopicPartition, Long> processedOffsets = new HashMap<TopicPartition, Long>();
        for(Entry<String, ConsumerRecords> recordMetadata : records.entrySet()) {
            List<ConsumerRecord> recordsPerTopic = recordMetadata.getValue().records();
            for(int i = 0;i < recordsPerTopic.size();i++) {
                ConsumerRecord record = recordsPerTopic.get(i);
                // process record
                try {
                    processedOffsets.put(record.topicAndPartition(), record.offset());
                } catch (Exception e) {
                    e.printStackTrace();
                }                
            }
        }
        return processedOffsets; 
    }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.MetricName;

/**
 * A mock of the {@link Consumer} interface you can use for testing code that uses Kafka.
 * This class is <i> not threadsafe </i>
 * <p>
 * The consumer runs in the user thread and multiplexes I/O over TCP connections to each of the brokers it
 * needs to communicate with. Failure to close the consumer after use will leak these resources.
 */
public class MockConsumer implements Consumer<byte[], byte[]> {

    private final Set<TopicPartition> subscribedPartitions;
    private final Set<String> subscribedTopics;
    private final Map<TopicPartition, Long> committedOffsets; 
    private final Map<TopicPartition, Long> consumedOffsets;
    
    public MockConsumer() {
        subscribedPartitions = new HashSet<TopicPartition>();
        subscribedTopics = new HashSet<String>();
        committedOffsets = new HashMap<TopicPartition, Long>();
        consumedOffsets = new HashMap<TopicPartition, Long>();
    }
    
    @Override
    public void subscribe(String... topics) {
        if(subscribedPartitions.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions is mutually exclusive");
        for(String topic : topics) {
            subscribedTopics.add(topic);
        }
    }

    @Override
    public void subscribe(TopicPartition... partitions) {
        if(subscribedTopics.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions is mutually exclusive");
        for(TopicPartition partition : partitions) {
            subscribedPartitions.add(partition);
            consumedOffsets.put(partition, 0L);
        }
    }

    public void unsubscribe(String... topics) {
        // throw an exception if the topic was never subscribed to
        for(String topic:topics) {
            if(!subscribedTopics.contains(topic))
                throw new IllegalStateException("Topic " + topic + " was never subscribed to. subscribe(" + topic + ") should be called prior" +
                        " to unsubscribe(" + topic + ")");
            subscribedTopics.remove(topic);
        }
    }

    public void unsubscribe(TopicPartition... partitions) {
        // throw an exception if the partition was never subscribed to
        for(TopicPartition partition:partitions) {
            if(!subscribedPartitions.contains(partition))
                throw new IllegalStateException("Partition " + partition + " was never subscribed to. subscribe(new TopicPartition(" + 
                                                 partition.topic() + "," + partition.partition() + ") should be called prior" +
                                                " to unsubscribe(new TopicPartition(" + partition.topic() + "," + partition.partition() + ")");
            subscribedPartitions.remove(partition);                    
            committedOffsets.remove(partition);
            consumedOffsets.remove(partition);
        }
    }

    @Override
    public Map<String, ConsumerRecords<byte[], byte[]>> poll(long timeout) {
        // hand out one dummy record, 1 per topic
        Map<String, List<ConsumerRecord>> records = new HashMap<String, List<ConsumerRecord>>();
        Map<String, ConsumerRecords<byte[], byte[]>> recordMetadata = new HashMap<String, ConsumerRecords<byte[], byte[]>>();
        for(TopicPartition partition : subscribedPartitions) {
            // get the last consumed offset
            long messageSequence = consumedOffsets.get(partition);
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream outputStream;
            try {
                outputStream = new ObjectOutputStream(byteStream);
                outputStream.writeLong(messageSequence++);
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            List<ConsumerRecord> recordsForTopic = records.get(partition.topic());
            if(recordsForTopic == null) {
                recordsForTopic = new ArrayList<ConsumerRecord>();
                records.put(partition.topic(), recordsForTopic);
            }
            recordsForTopic.add(new ConsumerRecord(partition.topic(), partition.partition(), null, byteStream.toByteArray(), messageSequence));
            consumedOffsets.put(partition, messageSequence);
        }
        for(Entry<String, List<ConsumerRecord>> recordsPerTopic : records.entrySet()) {
            Map<Integer, List<ConsumerRecord>> recordsPerPartition = new HashMap<Integer, List<ConsumerRecord>>();
            for(ConsumerRecord record : recordsPerTopic.getValue()) {
                List<ConsumerRecord> recordsForThisPartition = recordsPerPartition.get(record.partition());
                if(recordsForThisPartition == null) {
                    recordsForThisPartition = new ArrayList<ConsumerRecord>();
                    recordsPerPartition.put(record.partition(), recordsForThisPartition);
                }
                recordsForThisPartition.add(record);
            }
            recordMetadata.put(recordsPerTopic.getKey(), new ConsumerRecords(recordsPerTopic.getKey(), recordsPerPartition));
        }
        return recordMetadata;
    }

    @Override
    public OffsetMetadata commit(Map<TopicPartition, Long> offsets, boolean sync) {
        if(!sync)
            return null;
        for(Entry<TopicPartition, Long> partitionOffset : offsets.entrySet()) {
            committedOffsets.put(partitionOffset.getKey(), partitionOffset.getValue());            
        }        
        return new OffsetMetadata(committedOffsets, null);
    }

    @Override
    public OffsetMetadata commit(boolean sync) {
        if(!sync)
            return null;
        return commit(consumedOffsets, sync);
    }

    @Override
    public void seek(Map<TopicPartition, Long> offsets) {
        // change the fetch offsets
        for(Entry<TopicPartition, Long> partitionOffset : offsets.entrySet()) {
            consumedOffsets.put(partitionOffset.getKey(), partitionOffset.getValue());            
        }
    }

    @Override
    public Map<TopicPartition, Long> committed(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
        for(TopicPartition partition : partitions) {
            offsets.put(new TopicPartition(partition.topic(), partition.partition()), committedOffsets.get(partition));
        }
        return offsets;
    }

    @Override
    public Map<TopicPartition, Long> position(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> positions = new HashMap<TopicPartition, Long>();
        for(TopicPartition partition : partitions) {
            positions.put(partition, consumedOffsets.get(partition));
        }
        return positions;
    }

    @Override
    public Map<TopicPartition, Long> offsetsBeforeTime(long timestamp,
            Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public void close() {
       // unsubscribe from all partitions
        TopicPartition[] allPartitions = new TopicPartition[subscribedPartitions.size()];
        unsubscribe(subscribedPartitions.toArray(allPartitions));
    }
}

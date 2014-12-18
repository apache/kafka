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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A container that holds the list {@link ConsumerRecord} per partition for a particular topic. There is one for every topic returned by a 
 * {@link Consumer#poll(long)} operation. 
 */
public class ConsumerRecords<K,V> {

    private final String topic;
    private final Map<Integer, List<ConsumerRecord<K,V>>> recordsPerPartition;
    
    public ConsumerRecords(String topic, Map<Integer, List<ConsumerRecord<K,V>>> records) {
        this.topic = topic;
        this.recordsPerPartition = records;
    }
    
    /**
     * @param partitions The input list of partitions for a particular topic. If no partitions are 
     * specified, returns records for all partitions
     * @return The list of {@link ConsumerRecord}s associated with the given partitions.
     */
    public List<ConsumerRecord<K,V>> records(int... partitions) {
        List<ConsumerRecord<K,V>> recordsToReturn = new ArrayList<ConsumerRecord<K,V>>();
        if(partitions.length == 0) {
            // return records for all partitions
            for(Entry<Integer, List<ConsumerRecord<K,V>>> record : recordsPerPartition.entrySet()) {
                recordsToReturn.addAll(record.getValue());
            }
        } else {
           for(int partition : partitions) {
               List<ConsumerRecord<K,V>> recordsForThisPartition = recordsPerPartition.get(partition);
               recordsToReturn.addAll(recordsForThisPartition);
           }
        }
        return recordsToReturn;
    }

    /**
     * @return The topic of all records associated with this instance
     */
    public String topic() {
        return this.topic;
    }
}

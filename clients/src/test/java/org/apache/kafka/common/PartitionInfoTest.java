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
package org.apache.kafka.common;

import org.junit.Assert;
import org.junit.Test;

public class PartitionInfoTest {
    
    @Test
    public void testToString() {
        String topic = "sample";
        int partition = 0;
        Node leader = new Node(0, "localhost", 9092);
        Node r1 = new Node(1, "localhost", 9093);
        Node r2 = new Node(2, "localhost", 9094); 
        Node[] replicas = new Node[] {leader, r1, r2};
        Node[] inSyncReplicas = new Node[] {leader, r1, r2};
        PartitionInfo partitionInfo = new PartitionInfo(topic, partition, leader, replicas, inSyncReplicas);
        
        String expected = String.format("Partition(topic = %s, partition = %d, leader = %s, replicas = %s, isr = %s)",
                topic, partition, leader.idString(), "[0,1,2]", "[0,1,2]");
        Assert.assertEquals(expected, partitionInfo.toString());
    }

}
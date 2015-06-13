/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

/**
 * A partitioning strategy for load balancing based on "M. A. U. Nasir, G. De Francisci Morales, D. Garcia-Soriano, N.
 * Kourtellis, M. Serafini, 'The Power of Both Choices: Practical Load Balancing for Distributed Stream Processing
 * Engines', in ICDE ’15: 31st IEEE International Conference on Data Engineering, pp. 137−148, Seoul, 2015".
 * 
 */
public class PKGPartitioner extends DefaultPartitioner {
  private long[] targetTaskStats;

  @Override
  protected int partitionWithKey(byte[] keyBytes, int numPartitions, String topic, Cluster cluster) {
    // initialization
    if (targetTaskStats == null)
      targetTaskStats = new long[numPartitions];
    assert targetTaskStats.length == numPartitions;
    // choice
    int firstChoice  = toPositive(Utils.murmur2(keyBytes, 13)) % numPartitions;
    int secondChoice = toPositive(Utils.murmur2(keyBytes, 17)) % numPartitions;
    int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
    targetTaskStats[selected]++;
    return selected;
  }
}

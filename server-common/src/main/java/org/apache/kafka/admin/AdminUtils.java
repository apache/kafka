/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class AdminUtils {
    private static final Logger log = LoggerFactory.getLogger(AdminUtils.class);

    static final Random rand = new Random();

    static final String AdminClientId = "__admin_client";

    public static Map<Integer, List<Integer>> assignReplicasToBrokers(Iterable<BrokerMetadata> brokerMetadatas,
                                                                      int nPartitions,
                                                                      int replicationFactor) {
        return assignReplicasToBrokers(brokerMetadatas, nPartitions, replicationFactor, -1, -1);
    }

    /**
     * There are 3 goals of replica assignment:
     *
     * <ol>
     * <li> Spread the replicas evenly among brokers.</li>
     * <li> For partitions assigned to a particular broker, their other replicas are spread over the other brokers.</li>
     * <li> If all brokers have rack information, assign the replicas for each partition to different racks if possible</li>
     * </ol>
     *
     * To achieve this goal for replica assignment without considering racks, we:
     * <ol>
     * <li> Assign the first replica of each partition by round-robin, starting from a random position in the broker list.</li>
     * <li> Assign the remaining replicas of each partition with an increasing shift.</li>
     * </ol>
     *
     * Here is an example of assigning
     * <table cellpadding="2" cellspacing="2">
     * <tr><th>broker-0</th><th>broker-1</th><th>broker-2</th><th>broker-3</th><th>broker-4</th><th>&nbsp;</th></tr>
     * <tr><td>p0      </td><td>p1      </td><td>p2      </td><td>p3      </td><td>p4      </td><td>(1st replica)</td></tr>
     * <tr><td>p5      </td><td>p6      </td><td>p7      </td><td>p8      </td><td>p9      </td><td>(1st replica)</td></tr>
     * <tr><td>p4      </td><td>p0      </td><td>p1      </td><td>p2      </td><td>p3      </td><td>(2nd replica)</td></tr>
     * <tr><td>p8      </td><td>p9      </td><td>p5      </td><td>p6      </td><td>p7      </td><td>(2nd replica)</td></tr>
     * <tr><td>p3      </td><td>p4      </td><td>p0      </td><td>p1      </td><td>p2      </td><td>(3nd replica)</td></tr>
     * <tr><td>p7      </td><td>p8      </td><td>p9      </td><td>p5      </td><td>p6      </td><td>(3nd replica)</td></tr>
     * </table>
     *
     * <p>
     * To create rack aware assignment, this API will first create a rack alternated broker list. For example,
     * from this brokerID -> rack mapping:</p>
     * 0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1"
     * <br><br>
     * <p>
     * The rack alternated list will be:
     * </p>
     * 0, 3, 1, 5, 4, 2
     * <br><br>
     * <p>
     * Then an easy round-robin assignment can be applied. Assume 6 partitions with replication factor of 3, the assignment
     * will be:
     * </p>
     * 0 -> 0,3,1 <br>
     * 1 -> 3,1,5 <br>
     * 2 -> 1,5,4 <br>
     * 3 -> 5,4,2 <br>
     * 4 -> 4,2,0 <br>
     * 5 -> 2,0,3 <br>
     * <br>
     * <p>
     * Once it has completed the first round-robin, if there are more partitions to assign, the algorithm will start
     * shifting the followers. This is to ensure we will not always get the same set of sequences.
     * In this case, if there is another partition to assign (partition #6), the assignment will be:
     * </p>
     * 6 -> 0,4,2 (instead of repeating 0,3,1 as partition 0)
     * <br><br>
     * <p>
     * The rack aware assignment always chooses the 1st replica of the partition using round robin on the rack alternated
     * broker list. For rest of the replicas, it will be biased towards brokers on racks that do not have
     * any replica assignment, until every rack has a replica. Then the assignment will go back to round-robin on
     * the broker list.
     * </p>
     * <br>
     * <p>
     * As the result, if the number of replicas is equal to or greater than the number of racks, it will ensure that
     * each rack will get at least one replica. Otherwise, each rack will get at most one replica. In a perfect
     * situation where the number of replicas is the same as the number of racks and each rack has the same number of
     * brokers, it guarantees that the replica distribution is even across brokers and racks.
     * </p>
     * @return a Map from partition id to replica ids
     * @throws AdminOperationException If rack information is supplied but it is incomplete, or if it is not possible to
     *                                 assign each replica to a unique rack.
     *
     */
    public static Map<Integer, List<Integer>> assignReplicasToBrokers(Iterable<BrokerMetadata> brokerMetadatas,
                                                                      int nPartitions,
                                                                      int replicationFactor,
                                                                      int fixedStartIndex,
                                                                      int startPartitionId) {
        return null;
    }

    private static Map<Integer, List<Integer>> assignReplicasToBrokersRackUnaware(int nPartitions,
                                                                                  int replicationFactor,
                                                                                  Iterable<Integer> brokerList,
                                                                                  int fixedStartIndex,
                                                                                  int startPartitionId) {
        return null;
    }

    private static Map<Integer, List<Integer>> assignReplicasToBrokersRackAware(int nPartitions,
                                                                                int replicationFactor,
                                                                                Iterable<BrokerMetadata> brokerMetadatas,
                                                                                int fixedStartIndex,
                                                                                int startPartitionId) {
        return null;
    }

    /**
     * Given broker and rack information, returns a list of brokers alternated by the rack. Assume
     * this is the rack and its brokers:
     *
     * rack1: 0, 1, 2
     * rack2: 3, 4, 5
     * rack3: 6, 7, 8
     *
     * This API would return the list of 0, 3, 6, 1, 4, 7, 2, 5, 8
     *
     * This is essential to make sure that the assignReplicasToBrokers API can use such list and
     * assign replicas to brokers in a simple round-robin fashion, while ensuring an even
     * distribution of leader and replica counts on each broker and that replicas are
     * distributed to all racks.
     */
    static List<Integer> getRackAlternatedBrokerList(Map<Integer, String> brokerRackMap) {
        return null;
    }

    static Map<String, List<Integer>> getInverseMap(Map<Integer, String> brokerRackMap) {
        return null;
    }

    static int replicaIndex(int firstReplicaIndex, int secondReplicaShift, int replicaIndex, int nBrokers) {
        long shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
        return (int) ((firstReplicaIndex + shift) % nBrokers);
    }
}

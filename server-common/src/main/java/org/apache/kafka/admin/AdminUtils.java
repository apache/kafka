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
package org.apache.kafka.admin;

import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.server.common.AdminOperationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class AdminUtils {
    static final Random RAND = new Random();

    public static final String ADMIN_CLIENT_ID = "__admin_client";

    public static Map<Integer, List<Integer>> assignReplicasToBrokers(Collection<BrokerMetadata> brokerMetadatas,
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
     * @throws AdminOperationException If rack information is supplied, but it is incomplete, or if it is not possible to
     *                                 assign each replica to a unique rack.
     *
     */
    public static Map<Integer, List<Integer>> assignReplicasToBrokers(Collection<BrokerMetadata> brokerMetadatas,
                                                                      int nPartitions,
                                                                      int replicationFactor,
                                                                      int fixedStartIndex,
                                                                      int startPartitionId) {
        if (nPartitions <= 0)
            throw new InvalidPartitionsException("Number of partitions must be larger than 0.");
        if (replicationFactor <= 0)
            throw new InvalidReplicationFactorException("Replication factor must be larger than 0.");
        if (replicationFactor > brokerMetadatas.size())
            throw new InvalidReplicationFactorException("Replication factor: " + replicationFactor + " larger than available brokers: " + brokerMetadatas.size() + ".");
        if (brokerMetadatas.stream().noneMatch(b -> b.rack.isPresent()))
            return assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.stream().map(b -> b.id).toList(), fixedStartIndex,
                startPartitionId);
        else {
            return assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,
                startPartitionId);
        }
    }

    private static Map<Integer, List<Integer>> assignReplicasToBrokersRackUnaware(int nPartitions,
                                                                                  int replicationFactor,
                                                                                  List<Integer> brokerList,
                                                                                  int fixedStartIndex,
                                                                                  int startPartitionId) {
        Map<Integer, List<Integer>> ret = new HashMap<>();
        int startIndex = fixedStartIndex >= 0 ? fixedStartIndex : RAND.nextInt(brokerList.size());
        int currentPartitionId = Math.max(0, startPartitionId);
        int nextReplicaShift = fixedStartIndex >= 0 ? fixedStartIndex : RAND.nextInt(brokerList.size());
        for (int i = 0; i < nPartitions; i++) {
            if (currentPartitionId > 0 && (currentPartitionId % brokerList.size() == 0))
                nextReplicaShift += 1;
            int firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size();
            List<Integer> replicaBuffer = new ArrayList<>();
            replicaBuffer.add(brokerList.get(firstReplicaIndex));
            for (int j = 0; j < replicationFactor - 1; j++)
                replicaBuffer.add(brokerList.get(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size())));
            ret.put(currentPartitionId, replicaBuffer);
            currentPartitionId += 1;
        }
        return ret;
    }

    private static Map<Integer, List<Integer>> assignReplicasToBrokersRackAware(int nPartitions,
                                                                                int replicationFactor,
                                                                                Collection<BrokerMetadata> brokerMetadatas,
                                                                                int fixedStartIndex,
                                                                                int startPartitionId) {
        Map<Integer, String> brokerRackMap = new HashMap<>();
        brokerMetadatas.forEach(m -> brokerRackMap.put(m.id, m.rack.orElseThrow(() -> new AdminOperationException("Not all brokers have rack information for replica rack aware assignment."))));
        int numRacks = new HashSet<>(brokerRackMap.values()).size();
        List<Integer> arrangedBrokerList = getRackAlternatedBrokerList(brokerRackMap);
        int numBrokers = arrangedBrokerList.size();
        Map<Integer, List<Integer>> ret = new HashMap<>();
        int startIndex = fixedStartIndex >= 0 ? fixedStartIndex : RAND.nextInt(arrangedBrokerList.size());
        int currentPartitionId = Math.max(0, startPartitionId);
        int nextReplicaShift = fixedStartIndex >= 0 ? fixedStartIndex : RAND.nextInt(arrangedBrokerList.size());
        for (int i = 0; i < nPartitions; i++) {
            if (currentPartitionId > 0 && (currentPartitionId % arrangedBrokerList.size() == 0))
                nextReplicaShift += 1;
            int firstReplicaIndex = (currentPartitionId + startIndex) % arrangedBrokerList.size();
            int leader = arrangedBrokerList.get(firstReplicaIndex);
            List<Integer> replicaBuffer = new ArrayList<>();
            replicaBuffer.add(leader);
            Set<String> racksWithReplicas = new HashSet<>();
            racksWithReplicas.add(brokerRackMap.get(leader));
            Set<Integer> brokersWithReplicas = new HashSet<>();
            brokersWithReplicas.add(leader);
            int k = 0;
            for (int j = 0; j < replicationFactor - 1; j++) {
                boolean done = false;
                while (!done) {
                    Integer broker = arrangedBrokerList.get(replicaIndex(firstReplicaIndex, nextReplicaShift * numRacks, k, arrangedBrokerList.size()));
                    String rack = brokerRackMap.get(broker);
                    // Skip this broker if
                    // 1. there is already a broker in the same rack that has assigned a replica AND there is one or more racks
                    //    that do not have any replica, or
                    // 2. the broker has already assigned a replica AND there is one or more brokers that do not have replica assigned
                    if ((!racksWithReplicas.contains(rack) || racksWithReplicas.size() == numRacks)
                        && (!brokersWithReplicas.contains(broker) || brokersWithReplicas.size() == numBrokers)) {
                        replicaBuffer.add(broker);
                        racksWithReplicas.add(rack);
                        brokersWithReplicas.add(broker);
                        done = true;
                    }
                    k += 1;
                }
            }
            ret.put(currentPartitionId, replicaBuffer);
            currentPartitionId += 1;
        }
        return ret;
    }

    /**
     * Given broker and rack information, returns a list of brokers alternated by the rack. Assume
     * this is the rack and its brokers:
     * <pre>
     * rack1: 0, 1, 2
     * rack2: 3, 4, 5
     * rack3: 6, 7, 8
     * </pre>
     * This API would return the list of 0, 3, 6, 1, 4, 7, 2, 5, 8
     * <br>
     * This is essential to make sure that the assignReplicasToBrokers API can use such list and
     * assign replicas to brokers in a simple round-robin fashion, while ensuring an even
     * distribution of leader and replica counts on each broker and that replicas are
     * distributed to all racks.
     */
    public static List<Integer> getRackAlternatedBrokerList(Map<Integer, String> brokerRackMap) {
        Map<String, Iterator<Integer>> brokersIteratorByRack = new HashMap<>();
        getInverseMap(brokerRackMap).forEach((rack, brokers) -> brokersIteratorByRack.put(rack, brokers.iterator()));
        String[] racks = brokersIteratorByRack.keySet().toArray(new String[0]);
        Arrays.sort(racks);
        List<Integer> result = new ArrayList<>();
        int rackIndex = 0;
        while (result.size() < brokerRackMap.size()) {
            Iterator<Integer> rackIterator = brokersIteratorByRack.get(racks[rackIndex]);
            if (rackIterator.hasNext())
                result.add(rackIterator.next());
            rackIndex = (rackIndex + 1) % racks.length;
        }
        return result;
    }

    static Map<String, List<Integer>> getInverseMap(Map<Integer, String> brokerRackMap) {
        Map<String, List<Integer>> results = new HashMap<>();
        brokerRackMap.forEach((id, rack) -> results.computeIfAbsent(rack, key -> new ArrayList<>()).add(id));
        results.forEach((rack, rackAndIdList) -> rackAndIdList.sort(Integer::compareTo));
        return results;
    }

    static int replicaIndex(int firstReplicaIndex, int secondReplicaShift, int replicaIndex, int nBrokers) {
        long shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
        return (int) ((firstReplicaIndex + shift) % nBrokers);
    }
}

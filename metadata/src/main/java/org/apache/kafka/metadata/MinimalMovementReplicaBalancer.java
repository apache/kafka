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

package org.apache.kafka.metadata;

import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.DuplicateBrokerRegistrationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.metadata.placement.UsableBroker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p><b>Minimal Movement Replica Balancer</b></p>
 * Optimizes topic replica distribution across brokers with minimal movement while satisfying rack-aware constraints
 * and load balancing objectives.
 *
 * <p><h3>@Goals:</h3><br>
 * Achieve balanced replica distribution across target brokers with the following prioritized goals:</p>
 *
 * <ol>
 *   <li><b>Minimal Movement:</b> Minimize replica relocation operations during rebalancing.</li>
 *   <li><b>Replicas Balancing:</b> Ensures that replicas are distributed as evenly as possible among nodes.</li>
 *   <li><b>Optional Rack Awareness:</b> Supports configurable rack-aware allocation when enabled.</li>
 *   <li><b>Leader Balancing:</b> Ensures that the number of leaders is distributed as evenly as possible among nodes.</li>
 *   <li><b>ISR Order Optimization:</b> Implements adjacency relationship balancing, prevents failover traffic concentration during broker outages.</li>
 *   <li><b>Leader Stability:</b> Keep the original partition leader unchanged as much as possible to minimize leader transitions. This objective has a lower priority than the first five.</li>
 * </ol>
 *
 * <ul>
 *   <li><b>Rack-Level Replica Distribution:</b>
 *     <ol type="A">
 *       <li>When rack count = replication factor:
 *         <ul><li>All racks receive exactly partitionCount replicas</li></ul>
 *       </li>
 *       <li>When rack count > replication factor:
 *         <ol type="a">
 *           <li>If weighted allocation (rackBrokers/totalBrokers × totalReplicas) ≥ partitionCount: racks receive exactly partitionCount replicas
 *           </li>
 *           <li>If weighted allocation < partitionCount: Distribute remaining replicas using weighted remainder allocation
 *           </li>
 *         </ol>
 *       </li>
 *     </ol>
 *   </li>
 *
 *   <li><b>Node-Level Replica Distribution:</b>
 *     <ul>
 * <p>If the number of replicas assigned to a rack is not a multiple of the number of nodes in that rack, some nodes will host one additional replica compared to others.</p>
 *       <li><b>Rack Count = Replication Factor:</b>
 *         <ul>
 *           <li>If all racks contain an equal number of nodes, each node will have the same number of replicas.</li>
 *           <li>If rack sizes vary, nodes in racks with a higher node count will host fewer replicas on average.</li>
 *         </ul>
 *       </li>
 *       <li><b>Rack Count > Replication Factor:</b>
 *         <ul>
 *           <li>If no rack has an excessively high node weight, replicas can be evenly distributed across nodes.</li>
 *           <li>However, in racks with disproportionately high node weights, the nodes will receive fewer replicas.</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </li>
 * </ul>
 *
 *   <li><b>Anti-Affinity Support:</b>
 *     <ul>
 *       <li>When enabled, prevents replicas from colocating on same rack.</li>
 *       <li>Nodes without rack configuration are excluded from anti-affinity checks.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class MinimalMovementReplicaBalancer {

    /**
     * <p>Mapping of partition IDs to the list of broker IDs hosting replicas for that partition.</p>
     * Key: Partition ID, Value: List of replica broker IDs.
     */
    private final Map<Integer, List<Integer>> assignment;

    /**
     * Flag indicating whether rack-aware replica placement is enabled.
     */
    private final boolean enableRackAwareness;

    /**
     * Set of broker IDs that are eligible for new replica assignments.
     */
    private final Set<Integer> targetBrokerIds;

    /**
     * Total number of partitions in the topic.
     */
    private final int partitionCount;

    /**
     * Replication factor specifying the number of replicas per partition.
     */
    private final int replicationFactor;

    private final List<UsableBroker> usableBrokers;

    /**
     * Total number of replicas across all partitions (calculated as partitionCount * replicationFactor).
     */
    private int totalReplicaCount;

    /**
     * Set of all broker IDs involved in scaling operations (both source and target brokers).
     */
    private Set<Integer> involvedBrokerIds;

    /**
     * <p>Mapping of broker IDs to their corresponding rack Names.</p>
     * Key: Broker ID, Value: Rack Name.
     */
    private Map<Integer, String> brokerRackMapping;

    /**
     * <p>Current replica distribution across brokers.</p>
     * Key: Broker ID, Value: Number of replicas assigned.
     */
    private Map<Integer, Integer> brokerReplicaCount;

    /**
     * <p>Current replica distribution across racks.</p>
     * Key: Rack Name, Value: Number of replicas in rack.
     */
    private Map<String, Integer> rackReplicaCount;

    /**
     * <p>Mapping of racks to their member brokers.</p>
     * Key: Rack Name, Value: Set of broker IDs in rack.
     */
    private Map<String, Set<Integer>> rackBrokerMapping;

    /**
     * <p>Number of brokers per rack.</p>
     * Key: Rack Name, Value: Broker count in rack.
     */
    private Map<String, Integer> rackBrokerCount;

    /**
     * <p>Target replica distribution per rack for balanced allocation.</p>
     * Key: Rack Name, Value: Ideal replica count.
     */
    private Map<String, Integer> rackIdealReplicaCount;

    private final Map<Integer, Integer> brokerAverageReplicaCount = new HashMap<>();

    /**
     * <p>Target replica distribution per broker for balanced allocation.</p>
     * Key: Broker ID, Value: Ideal replica count
     */
    private final Map<Integer, Integer> brokerIdealReplicaCount = new HashMap<>();

    private final Map<Integer, String> brokerToRemainderRackMapping = new HashMap<>();

    /**
     * When the number of replicas in a rack is not an exact multiple of the number of nodes in the rack,
     * a remainder of replicas exists within the rack.
     */
    private final Map<String, Integer> rackToRemainderReplicaCount = new HashMap<>();

    private static final String COMMON_REMAINDER_RACK = UUID.randomUUID().toString();

    /**
     * <p>Partition assignments per broker, tracking partition locations.</p>
     * Key: Broker ID, Value: Map of Partition ID to replica broker IDs
     */
    private Map<Integer, Map<Integer, List<Integer>>> brokerPartitionAssignments;

    /**
     * <p>Partitions violating rack awareness constraints per broker.</p>
     * Key: Broker ID, Value: List of violating partition IDs
     */
    private Map<Integer, List<Integer>> rackAwarenessViolations;

    /**
     * <p>Target leader distribution per broker for balanced leadership.</p>
     * Key: Broker ID, Value: Ideal leader count.
     */
    private Map<Integer, Integer> brokerIdealLeaderCount;

    /**
     * <p>A map that tracks the remaining chances for each node to be assigned as a leader.</p>
     * Key: BrokerId, Value: Remaining opportunities to become a leader.
     */
    private Map<Integer, Integer> leaderRemainingQuotaMap;

    public static final String BROKER_ADJACENCY_SYMBOL = "->";

    public MinimalMovementReplicaBalancer(Map<Integer, List<Integer>> assignment, List<Integer> targetBrokerIds, List<UsableBroker> usableBrokers, boolean enableRackAwareness) {
        if (assignment == null || assignment.isEmpty()) {
            throw new InvalidReplicaAssignmentException("assignment is empty.");
        }
        if (targetBrokerIds == null) {
            throw new BrokerNotAvailableException("targetBrokerIds is null.");
        }
        // Nodes with unclear rack information in the proxy metadata are not considered as anti-affinity.
        if (usableBrokers == null) {
            this.usableBrokers = new ArrayList<>();
        } else {
            this.usableBrokers = new ArrayList<>(usableBrokers);
        }
        if (this.usableBrokers.stream().map(UsableBroker::id).collect(Collectors.toSet()).size() < this.usableBrokers.size()) {
            throw new DuplicateBrokerRegistrationException("Duplicate broker ID found.");
        }
        this.assignment = assignment.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new LinkedList<>(entry.getValue())
            ));
        this.partitionCount = assignment.size();
        this.replicationFactor = assignment.get(0).size();
        this.targetBrokerIds = new HashSet<>(targetBrokerIds);
        this.enableRackAwareness = enableRackAwareness;
        initializeClusterState();
        verifyingParameters();
    }

    private void verifyingParameters() {
        if (partitionCount <= 0)
            throw new InvalidPartitionsException("Number of partitions must be larger than 0.");
        if (replicationFactor <= 0)
            throw new InvalidReplicationFactorException("Replication factor must be larger than 0.");
        long rackCount = brokerRackMapping.entrySet().stream()
            .filter(entry -> targetBrokerIds.contains(entry.getKey()))
            .map(Map.Entry::getValue)
            .distinct()
            .count();
        if (replicationFactor > rackCount)
            throw new InvalidReplicationFactorException("Replication factor: " + replicationFactor + " larger than available brokers or rack num: " + rackCount + ".");
    }

    private void initializeClusterState() {
        totalReplicaCount = partitionCount * replicationFactor;
        involvedBrokerIds = new LinkedHashSet<>();
        assignment.values().forEach(involvedBrokerIds::addAll);
        involvedBrokerIds.addAll(targetBrokerIds);
        computeBrokerRackMap();
        buildRackBrokerMapping();
        computeRackBrokerCount();
        computeCurrentBrokerReplicaDistribution();
        computeCurrentRackReplicaDistribution();
        computeBrokerAverageReplicaDistribution();
        detectRackAwarenessViolations();
        computeBrokerIdealReplicaDistribution();
        computeRackIdealReplicaDistribution();
        prioritizeRebalanceTargets();
        identifyBrokerPartitionAssignments();
    }

    private void computeRackIdealReplicaDistribution() {
        rackIdealReplicaCount = brokerIdealReplicaCount.entrySet().stream()
            .collect(Collectors.groupingBy(
                entry -> brokerRackMapping.get(entry.getKey()),
                Collectors.summingInt(Map.Entry::getValue)
            ));
    }

    /**
     * <p>Computes the average replica distribution across brokers while considering rack-aware allocation constraints.</p>
     *
     * <p><b>Algorithm Steps:</b></p>
     * <ol>
     *   <li>When replication factor equals total rack count:
     *     <ul>
     *       <li>Each rack gets exactly one replica per partition (total replicas per rack = partition count)</li>
     *     </ul>
     *   </li>
     *   <li>When replication factor < total rack count:
     *     <ul>
     *       <li>Distribute replicas using weighted allocation based on broker count per rack:
     *         <ol type="a">
     *           <li>Calculate weighted load: (rack_broker_count / total_brokers) * total_replicas</li>
     *           <li>If weighted load >= partition count:
     *             <ul>
     *               <li>Allocate partition_count replicas to the rack</li>
     *               <li>Deduct allocated replicas from remaining quota</li>
     *             </ul>
     *           </li>
     *           <li>If weighted load < partition count:
     *             <ul>
     *               <li>Distribute remaining replicas among racks proportionally based on node count</li>
     *             </ul>
     *           </li>
     *         </ol>
     *       </li>
     *     </ul>
     *   </li>
     * </ol>
     */
    private void computeBrokerAverageReplicaDistribution() {
        int totalReplicas = this.totalReplicaCount;
        int totalBrokerNum = targetBrokerIds.size();
        for (Map.Entry<String, Integer> entry : rackBrokerCount.entrySet()) {
            String rack = entry.getKey();
            Set<Integer> targetBrokers = rackBrokerMapping.get(rack).stream().filter(targetBrokerIds::contains).collect(Collectors.toSet());
            int brokerSize = targetBrokers.size();
            if (replicationFactor == rackBrokerCount.size()) {
                // Case 1: Strict rack-based allocation (1 replica/rack/partition)
                computeExcessReplicaCount(targetBrokers, rack, partitionCount, brokerSize);
            } else {
                Integer rackBrokerNum = entry.getValue();
                int weightLoad = divideAndCeil(rackBrokerNum * totalReplicas, totalBrokerNum);
                if (weightLoad >= partitionCount) {
                    // Case 2a: Allocate full partition count to this rack
                    totalReplicas -= partitionCount;
                    totalBrokerNum -= rackBrokerNum;
                    computeExcessReplicaCount(targetBrokers, rack, partitionCount, brokerSize);
                } else {
                    // Case 2b: Allocate remaining replicas proportionally based on the weighted distribution of brokers across racks.
                    computeExcessReplicaCount(targetBrokers, COMMON_REMAINDER_RACK, totalReplicas, totalBrokerNum);
                }
            }
        }
    }

    private void computeExcessReplicaCount(Set<Integer> brokers, String rack, int replicasCount, int brokerNum) {
        int aveReplicationCount = replicasCount / brokerNum;
        int remainder = replicasCount % brokerNum;
        for (Integer brokerId : brokers) {
            brokerAverageReplicaCount.put(brokerId, aveReplicationCount);
            brokerToRemainderRackMapping.put(brokerId, rack);
        }
        rackToRemainderReplicaCount.put(rack, remainder);
    }

    private void updateClusterStateAfterReplicaMove(Integer partition, Integer removeBrokerId, int replaceBrokerId) {
        brokerReplicaCount.put(removeBrokerId, brokerReplicaCount.get(removeBrokerId) - 1);
        brokerReplicaCount.put(replaceBrokerId, brokerReplicaCount.getOrDefault(replaceBrokerId, 0) + 1);

        String removeRackName = brokerRackMapping.get(removeBrokerId);
        String replaceRackName = brokerRackMapping.get(replaceBrokerId);
        rackReplicaCount.put(removeRackName, rackReplicaCount.get(removeRackName) - 1);
        rackReplicaCount.put(replaceRackName, rackReplicaCount.getOrDefault(replaceRackName, 0) + 1);

        brokerPartitionAssignments.get(removeBrokerId).remove(partition);

        Map<Integer, List<Integer>> brokerAssignments = brokerPartitionAssignments.computeIfAbsent(replaceBrokerId, k -> new HashMap<>());
        brokerAssignments.put(partition, assignment.get(partition));
        detectRackAwarenessViolations();
    }

    public Map<Integer, List<Integer>> assignReplicasToBrokers() {
        Iterator<Integer> iterator = involvedBrokerIds.iterator();
        while (iterator.hasNext()) {
            Integer brokerId = iterator.next();
            boolean violatesAntiAffinity = hasAntiAffinityViolation(brokerId);
            boolean exceedsDesiredReplicas = hasExcessReplicas(brokerId);
            while (exceedsDesiredReplicas || violatesAntiAffinity) {
                List<Integer> replicas;
                Integer partition;
                if (violatesAntiAffinity) {
                    partition = rackAwarenessViolations.get(brokerId).get(0);
                    replicas = assignment.get(partition);
                    hasReplicaMoved(brokerId, replicas, true, partition);
                } else {
                    Map<Integer, List<Integer>> brokerAssignment = brokerPartitionAssignments.get(brokerId);
                    for (Map.Entry<Integer, List<Integer>> entry : brokerAssignment.entrySet()) {
                        partition = entry.getKey();
                        replicas = entry.getValue();
                        boolean nextFlag = hasReplicaMoved(brokerId, replicas, false, partition);
                        if (nextFlag) {
                            break;
                        }
                    }
                }
                violatesAntiAffinity = hasAntiAffinityViolation(brokerId);
                exceedsDesiredReplicas = hasExcessReplicas(brokerId);
            }
            if (brokerIdealReplicaCount.get(brokerId).equals(brokerReplicaCount.get(brokerId))) {
                iterator.remove();
            }
        }
        adjustReplicaOrder();
        return assignment;
    }

    private void adjustReplicaOrder() {
        computeIdealLeaderDistribution();
        leaderRemainingQuotaMap = new HashMap<>(brokerReplicaCount);
        if (replicationFactor > 2) {
            balanceLeadersAcrossBrokers();
            adjustISRReplicaOrder();
        } else if (replicationFactor == 2) {
            adjustTwoReplicasOrder();
        }
    }

    /**
     * <p>Replica Order Optimization for Adjacency Balancing</p>
     * <p><b>Problem Scenario:</b></p>
     * Given an initial replica distribution with imbalanced adjacency relationships:
     * <pre>
     *  0: [1, 2]
     *  1: [1, 2]
     *  2: [2, 3]
     *  3: [2, 3]
     *  4: [3, 1]
     *  5: [3, 1]
     * </pre>
     * <p><b>Optimization Goal:</b></p>
     * Transform the distribution to balance adjacency relationships:
     * <pre>
     *  0: [1, 2]
     *  1: [2, 1]
     *  2: [2, 3]
     *  3: [3, 2]
     *  4: [3, 1]
     *  5: [1, 3]
     * </pre>
     */
    private void adjustTwoReplicasOrder() {
        Map<String, Integer> brokerAdjacencyCountMap = new HashMap<>();
        Map<Integer, Integer> brokerLeaderCount = new HashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            List<Integer> replicas = assignment.get(i);
            Integer preBroker = replicas.get(0);
            Integer nextBroker = replicas.get(1);

            Integer preBrokerLeaderCount = brokerLeaderCount.getOrDefault(preBroker, 0);
            Integer preBrokerIdealLeaderCount = brokerIdealLeaderCount.get(preBroker);
            int preDiffLeader = preBrokerIdealLeaderCount - preBrokerLeaderCount;

            Integer nextBrokerBrokerLeaderCount = brokerLeaderCount.getOrDefault(nextBroker, 0);
            Integer nextBrokerBrokerIdealLeaderCount = brokerIdealLeaderCount.get(nextBroker);
            int nextDiffLeader = nextBrokerBrokerIdealLeaderCount - nextBrokerBrokerLeaderCount;
            boolean needAdjust = false;
            if (nextDiffLeader > preDiffLeader) {
                needAdjust = true;
            } else if (nextDiffLeader == preDiffLeader) {
                String preNextAdjacency = preBroker + BROKER_ADJACENCY_SYMBOL + nextBroker;
                Integer brokerAdjacencyCount = brokerAdjacencyCountMap.getOrDefault(preNextAdjacency, 0);
                String nextPreAdjacency = nextBroker + BROKER_ADJACENCY_SYMBOL + preBroker;
                Integer nextPreAdjacencyCount = brokerAdjacencyCountMap.getOrDefault(nextPreAdjacency, 0);
                if (nextPreAdjacencyCount < brokerAdjacencyCount) {
                    needAdjust = true;
                }
            }
            if (needAdjust) {
                replicas.set(0, nextBroker);
                replicas.set(1, preBroker);
            }
            preBroker = replicas.get(0);
            nextBroker = replicas.get(1);
            String brokerAdjacency = preBroker + BROKER_ADJACENCY_SYMBOL + nextBroker;
            brokerAdjacencyCountMap.merge(brokerAdjacency, 1, Integer::sum);
            brokerLeaderCount.merge(preBroker, 1, Integer::sum);
        }
    }

    /**
     * Adjusts ISR Replica Order for Failover Load Balancing.
     * Optimizes replica order in partitions to prevent traffic concentration during broker failures by balancing adjacency relationships.
     */
    private void adjustISRReplicaOrder() {
        Map<String, Integer> brokerAdjacencyCountMap = new HashMap<>();
        for (int i = 0; i < replicationFactor - 1; i++) {
            for (int j = 0; j < partitionCount; j++) {
                List<Integer> replicas = assignment.get(j);
                Integer preBroker = replicas.get(i);
                int nextBroker = selectNextBroker(i, brokerAdjacencyCountMap, replicas);
                replicas.removeIf(replica -> replica.equals(nextBroker));
                replicas.add(i + 1, nextBroker);
                String brokerAdjacency = preBroker + BROKER_ADJACENCY_SYMBOL + nextBroker;
                brokerAdjacencyCountMap.merge(brokerAdjacency, 1, Integer::sum);
                if (i == replicationFactor - 3) {
                    // Fixing the penultimate replica inherently determines the last replica's position.
                    String lastBrokerAdjacency = nextBroker + BROKER_ADJACENCY_SYMBOL + replicas.get(i + 2);
                    brokerAdjacencyCountMap.merge(lastBrokerAdjacency, 1, Integer::sum);
                }
            }
        }
    }

    /**
     * Optimizes Adjacent Broker Selection
     * Implements weighted least-used adjacency selection to minimize broker pair concentration.
     * Selection prioritizes broker pairs with lowest historical adjacency count to prevent hot-spotting.
     *
     * @param preIndex                Current position in replica list being processed
     * @param brokerAdjacencyCountMap Historical adjacency frequency registry
     * @param replica                 Replica list for target partition
     */
    private int selectNextBroker(int preIndex, Map<String, Integer> brokerAdjacencyCountMap, List<Integer> replica) {
        Integer preBroker = replica.get(preIndex);
        int minAdjacencyCount = Integer.MAX_VALUE;
        int nextBroker = replica.get(preIndex + 1);
        for (int i = preIndex + 1; i < replicationFactor; i++) {
            Integer tmpNextBroker = replica.get(i);
            String brokerAdjacency = preBroker + BROKER_ADJACENCY_SYMBOL + tmpNextBroker;
            Integer brokerAdjacencyCount = brokerAdjacencyCountMap.getOrDefault(brokerAdjacency, 0);
            if (brokerAdjacencyCount < minAdjacencyCount) {
                minAdjacencyCount = brokerAdjacencyCount;
                nextBroker = tmpNextBroker;
            }
        }
        return nextBroker;
    }

    /**
     * Checks whether the specified node violates anti-affinity rules.
     */
    private boolean hasAntiAffinityViolation(int brokerId) {
        List<Integer> rackConflictPartitions = rackAwarenessViolations.get(brokerId);
        return rackConflictPartitions != null && !rackConflictPartitions.isEmpty();
    }


    /**
     * Determines whether the specified node has more replicas than the desired replica count.
     */
    private boolean hasExcessReplicas(int brokerId) {
        Integer currentReplicaCount = brokerReplicaCount.get(brokerId);
        Integer idealReplicaCount = brokerIdealReplicaCount.get(brokerId);
        int diffReplicaCount = currentReplicaCount - idealReplicaCount;
        return currentReplicaCount > 0 && diffReplicaCount > 0;
    }

    private boolean hasReplicaMoved(int brokerId, List<Integer> replicas, boolean hasRackConflict, int partition) {
        int indexOf = replicas.indexOf(brokerId);
        replicas.removeIf(replica -> replica.equals(brokerId));
        Set<String> usedRacks = replicas.stream()
            .filter(brokerRackMapping::containsKey)
            .map(brokerRackMapping::get)
            .collect(Collectors.toSet());
        Set<Integer> antiAffinityBrokerIds = findAntiAffinityBrokers(replicas, usedRacks);
        Integer replaceBrokerId = findReplacementBroker(brokerId, antiAffinityBrokerIds, hasRackConflict, replicas);
        if (null == replaceBrokerId) {
            replicas.add(indexOf, brokerId);
            return false;
        }
        replicas.add(indexOf, replaceBrokerId);
        updateClusterStateAfterReplicaMove(partition, brokerId, replaceBrokerId);
        return true;
    }

    /**
     * Balances leader distribution across brokers by reassigning leaders to replicas based on the following rules:
     * <ol>
     *   <li>Calculates the ideal leader count for each broker using {@link #computeIdealLeaderDistribution()}.</li>
     *   <li>Iterates through each partition and selects the best leader candidate from its replicas:
     *     <ol type="a">
     *       <li>For each replica broker, computes the difference between its ideal leader count and current leader count.</li>
     *       <li>Selects the broker with the largest positive difference as the new leader.</li>
     *     </ol>
     *   </li>
     *   <li>Updates the leader assignment by moving the selected leader to the first position in the replica list.</li>
     * </ol>
     */
    private void balanceLeadersAcrossBrokers() {
        Map<Integer, Integer> brokerLeaderCount = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            List<Integer> replicas = entry.getValue();
            int leaderId = replicas.get(0);
            Integer currentLeaderCount = brokerLeaderCount.getOrDefault(leaderId, 0);
            Integer idealLeaderCount = brokerIdealLeaderCount.get(leaderId);
            int leaderDeficit = idealLeaderCount - currentLeaderCount;
            for (int i = 1; i < replicas.size(); i++) {
                Integer tmpBrokerId = replicas.get(i);
                Integer tmpCurrentLeaderCount = brokerLeaderCount.getOrDefault(tmpBrokerId, 0);
                Integer tmpIdealLeaderCount = brokerIdealLeaderCount.get(tmpBrokerId);
                int tmpLeaderDeficit = tmpIdealLeaderCount - tmpCurrentLeaderCount;
                if (tmpLeaderDeficit > leaderDeficit) {
                    leaderId = tmpBrokerId;
                    leaderDeficit = tmpLeaderDeficit;
                } else if (tmpLeaderDeficit == leaderDeficit) {
                    Integer leaderRemainingQuota = leaderRemainingQuotaMap.get(leaderId);
                    Integer tmpBrokerRemainingQuota = leaderRemainingQuotaMap.get(tmpBrokerId);
                    if (tmpBrokerRemainingQuota < leaderRemainingQuota) {
                        leaderId = tmpBrokerId;
                    }
                }
            }
            replicas.remove(Integer.valueOf(leaderId));
            replicas.add(0, leaderId);
            brokerLeaderCount.put(leaderId, brokerLeaderCount.getOrDefault(leaderId, 0) + 1);
            replicas.forEach(brokerId -> leaderRemainingQuotaMap.merge(brokerId, -1, Integer::sum));
        }
    }

    /**
     * Computes the ideal leader distribution across brokers:
     * The strategy works as follows:
     * <ol>
     *   <li>For brokers where the assigned replica count is less than the average leader count:
     *       All replicas on this broker will be promoted to leaders.</li>
     *   <li>For brokers where the assigned replica count is equal to or greater than the average leader count:
     *       Assigns the baseline average leader count to prevent overloading.</li>
     * </ol>
     */
    private void computeIdealLeaderDistribution() {
        Map<Integer, Integer> brokerMaxLeaderMap = brokerPartitionAssignments.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().size()
            ))
            .entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (oldValue, newValue) -> oldValue,
                LinkedHashMap::new
            ));
        int partitionsNum = partitionCount;
        brokerIdealLeaderCount = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry : brokerMaxLeaderMap.entrySet()) {
            Integer brokerId = entry.getKey();
            Integer brokerMaxLeader = entry.getValue();
            int brokerAverageLeader = divideAndCeil(partitionsNum, targetBrokerIds.size());
            if (brokerMaxLeader < brokerAverageLeader) {
                brokerIdealLeaderCount.put(brokerId, brokerMaxLeader);
                partitionsNum = partitionsNum - brokerMaxLeader;
            } else {
                brokerIdealLeaderCount.put(brokerId, brokerAverageLeader);
            }
        }
    }


    /**
     * Detects partitions violating rack awareness constraints per broker.
     */
    private void detectRackAwarenessViolations() {
        Map<Integer, List<String>> partitionConflictRack = assignment.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream()
                    .map(brokerRackMapping::get)
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .entrySet().stream()
                    .filter(e -> e.getValue() >= 2)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList())
            ));

        Set<Integer> partitions = partitionConflictRack.keySet();
        rackAwarenessViolations = new HashMap<>();
        for (Integer partition : partitions) {
            List<Integer> brokers = assignment.get(partition);
            for (Set<Integer> rackBrokers : rackBrokerMapping.values()) {
                List<Integer> conflictBrokers = brokers.stream()
                    .filter(rackBrokers::contains)
                    .collect(Collectors.collectingAndThen(
                        Collectors.toList(), list -> list.size() > 1 ? list : Collections.emptyList()
                    ));
                if (conflictBrokers.isEmpty()) {
                    continue;
                }
                for (Integer conflictBrokerId : conflictBrokers) {
                    List<Integer> conflictPartition = rackAwarenessViolations.getOrDefault(conflictBrokerId, new ArrayList<>());
                    conflictPartition.add(partition);
                    rackAwarenessViolations.put(conflictBrokerId, conflictPartition);
                }
            }
        }
    }

    /**
     * Finds a replacement broker for replica migration following prioritized constraints:
     *
     * <ol>
     *   <li><b>Rack-Level Constraint Enforcement</b>:
     *     <ul>
     *       <li>When rack anti-affinity violations exist or rack load exceeds ideal capacity:
     *         <ol type="a">
     *           <li>Selects from candidate brokers in {@code antiAffinityBrokerIds}</li>
     *           <li>Prioritizes brokers with largest load deficit (current_load - ideal_load)</li>
     *         </ol>
     *       </li>
     *     </ul>
     *   </li>
     *
     *   <li><b>Node-Level Load Optimization</b>:
     *     <ul>
     *       <li>When rack constraints are satisfied:
     *         <ol type="a">
     *           <li>Prefers lowest-loaded broker within the same rack</li>
     *           <li>Excludes current broker and replica-hosting brokers to prevent node-level conflicts</li>
     *           <li>Falls back to cross-rack migration if no intra-rack candidates exist</li>
     *         </ol>
     *       </li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * @param brokerId              Source broker ID requiring replica migration
     * @param antiAffinityBrokerIds Candidate brokers satisfying anti-affinity constraints
     * @param rackConflict          Flag indicating rack constraint violations
     * @param replicas              Current replica distribution for the partition (avoids node-level conflicts)
     * @return Target broker ID for replica placement
     */
    private Integer findReplacementBroker(Integer brokerId, Set<Integer> antiAffinityBrokerIds, boolean rackConflict, List<Integer> replicas) {
        String rack = brokerRackMapping.get(brokerId);
        Integer rackLoad = rackReplicaCount.get(rack);
        Integer rackIdealsLoad = rackIdealReplicaCount.get(rack);
        if (rackConflict || (rackLoad > rackIdealsLoad)) {
            Optional<Integer> replaceOptional = antiAffinityBrokerIds.stream()
                .filter(b -> !Objects.equals(b, brokerId) && !replicas.contains(b) && involvedBrokerIds.contains(b))
                .min(Comparator.comparingInt(id -> {
                    int currentLoad = brokerReplicaCount.getOrDefault(id, 0);
                    int idealLoad = brokerIdealReplicaCount.getOrDefault(id, 0);
                    return Integer.compare(currentLoad - idealLoad, 0);
                }));
            return replaceOptional.orElse(null);
        } else {
            Set<Integer> standBrokers = rackBrokerMapping.get(rack);
            Optional<Integer> minLoadBrokerId = standBrokers.stream()
                .filter(b -> !Objects.equals(b, brokerId) && !replicas.contains(b))
                .min(Comparator.comparing(id -> {
                    int currentLoad = brokerReplicaCount.getOrDefault(id, 0);
                    int idealLoad = brokerIdealReplicaCount.getOrDefault(id, 0);
                    return Integer.compare(currentLoad - idealLoad, 0);
                }));
            return minLoadBrokerId.orElseGet(() -> findReplacementBroker(brokerId, antiAffinityBrokerIds, true, replicas));
        }
    }

    /**
     * Identifies existing partition replicas and their locations across brokers.
     */
    private void identifyBrokerPartitionAssignments() {
        brokerPartitionAssignments = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            Integer partition = entry.getKey();
            List<Integer> replica = entry.getValue();
            for (Integer brokerId : replica) {
                Map<Integer, List<Integer>> brokerAssignment = brokerPartitionAssignments.computeIfAbsent(brokerId, k -> new HashMap<>());
                brokerAssignment.put(partition, replica);
            }
        }
    }


    /**
     * Builds a mapping of broker IDs to their rack assignments based on the following rules:
     * <ol>
     *   <li>If rack awareness is enabled ({@link #enableRackAwareness} is true):
     *     <ol type="a">
     *       <li>Use the broker's configured rack if available.</li>
     *       <li>If the broker has no rack configured, assign it to a unique virtual rack
     *           (to exclude it from rack anti-affinity constraints).</li>
     *     </ol>
     *   </li>
     *   <li>If rack awareness is disabled ({@link #enableRackAwareness} is false):
     *     <ul>
     *       <li>Assign each broker to a unique virtual rack, effectively treating all brokers
     *           as if they are on separate racks.</li>
     *     </ul>
     *   </li>
     * </ol>
     * <p>
     * For nodes with unspecified rack information, rack anti-affinity constraints are not considered.
     * This unifies the logic for both rack-aware and non-rack-aware scenarios.
     */
    private void computeBrokerRackMap() {
        Set<Integer> existingBrokerIds = usableBrokers.stream()
            .map(UsableBroker::id)
            .collect(Collectors.toSet());
        involvedBrokerIds.stream().filter(brokerId -> !existingBrokerIds.contains(brokerId)).forEach(unknownBrokerId -> usableBrokers.add(new UsableBroker(unknownBrokerId, Optional.of(UUID.randomUUID().toString()), false)));
        brokerRackMapping = usableBrokers.stream()
            .filter(b -> involvedBrokerIds.contains(b.id()))
            .collect(Collectors.toMap(
                UsableBroker::id,
                broker -> enableRackAwareness
                    ? broker.rack().orElse(UUID.randomUUID().toString())
                    : UUID.randomUUID().toString()
            ));
    }

    private void buildRackBrokerMapping() {
        rackBrokerMapping = brokerRackMapping.entrySet().stream()
            .filter(entry -> targetBrokerIds.contains(entry.getKey()))
            .collect(Collectors.groupingBy(
                Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
            ));
    }

    private void computeRackBrokerCount() {
        rackBrokerCount = rackBrokerMapping.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().size(),
                (e1, e2) -> e1,
                LinkedHashMap::new
            ))
            .entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (e1, e2) -> e1,
                LinkedHashMap::new
            ));
    }

    private void computeCurrentRackReplicaDistribution() {
        Map<String, List<Integer>> rackToBrokersMap = brokerRackMapping.entrySet().stream()
            .collect(Collectors.groupingBy(
                Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey, Collectors.toList())
            ));

        rackReplicaCount = rackToBrokersMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> (int) assignment.values().stream()
                    .flatMap(List::stream)
                    .filter(entry.getValue()::contains)
                    .count()
            ));
    }

    /**
     * Finds a set of candidate brokers that satisfy both node-level and rack-level anti-affinity constraints
     * for replica placement. A broker is considered a valid candidate if:
     * <ol>
     *   <li>It is not already hosting a replica for the partition (node-level anti-affinity).</li>
     *   <li>It belongs to a rack that is not already used for the partition (rack-level anti-affinity).</li>
     * </ol>
     *
     * @param existingReplicas List of broker IDs already hosting replicas for the partition.
     * @param usedRacks        Set of rack IDs already used for the partition.
     * @return A set of broker IDs that satisfy both node-level and rack-level anti-affinity constraints.
     */
    private Set<Integer> findAntiAffinityBrokers(List<Integer> existingReplicas, Set<String> usedRacks) {
        return targetBrokerIds.stream()
            .filter(b ->
                !existingReplicas.contains(b) && // Node-level anti-affinity: exclude brokers with existing replicas
                    !usedRacks.contains(brokerRackMapping.get(b)) // Rack-level anti-affinity: exclude brokers in used racks
            ).collect(Collectors.toSet());
    }

    private void computeCurrentBrokerReplicaDistribution() {
        brokerReplicaCount = new HashMap<>();
        assignment.values().forEach(replicas ->
            replicas.forEach(b -> brokerReplicaCount.put(b, brokerReplicaCount.getOrDefault(b, 0) + 1))
        );
        involvedBrokerIds.forEach(b -> {
            brokerReplicaCount.put(b, brokerReplicaCount.getOrDefault(b, 0));
        });
    }

    /**
     * When the number of replicas is not an exact multiple of the number of nodes, a remainder of replicas exists.
     * To minimize data movement during rebalancing, the remainder replicas are preferentially assigned to nodes
     * with a higher deviation from the average replica count per node.
     */
    private void computeBrokerIdealReplicaDistribution() {
        involvedBrokerIds = involvedBrokerIds.stream()
            .sorted(Comparator
                .comparingInt(b -> {
                    int currentLoad = brokerReplicaCount.getOrDefault(b, 0);
                    int idealLoad = brokerAverageReplicaCount.getOrDefault(b, Integer.MAX_VALUE);
                    return currentLoad - idealLoad;
                }).reversed()
            )
            .collect(Collectors.toCollection(LinkedHashSet::new));
        for (Integer brokerId : involvedBrokerIds) {
            if (!targetBrokerIds.contains(brokerId)) {
                brokerIdealReplicaCount.put(brokerId, 0);
                continue;
            }
            String rack = brokerToRemainderRackMapping.get(brokerId);
            Integer replicaRemainder = rackToRemainderReplicaCount.get(rack);
            if (replicaRemainder > 0) {
                brokerIdealReplicaCount.put(brokerId, brokerAverageReplicaCount.get(brokerId) + 1);
                rackToRemainderReplicaCount.put(rack, --replicaRemainder);
            } else {
                brokerIdealReplicaCount.put(brokerId, brokerAverageReplicaCount.get(brokerId));
            }
        }
    }

    /**
     * Generates priority-ordered list of brokers for rebalancing based on:
     * <ul>
     *   <li>Severity of rack awareness violations.</li>
     *   <li>Current replica load deviation from ideal.</li>
     * </ul>
     */
    private void prioritizeRebalanceTargets() {
        involvedBrokerIds = involvedBrokerIds.stream()
            .sorted(Comparator
                .comparingInt((Integer b) -> rackAwarenessViolations.containsKey(b) ? 0 : 1)
                .thenComparing(b -> {
                    int currentLoad = brokerReplicaCount.getOrDefault(b, 0);
                    int idealLoad = brokerIdealReplicaCount.getOrDefault(b, Integer.MAX_VALUE);
                    return Integer.compare(currentLoad - idealLoad, 0);
                }, Comparator.reverseOrder())
            )
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static int divideAndCeil(int a, int b) {
        return (a % b == 0) ? (a / b) : (a / b + 1);
    }

}

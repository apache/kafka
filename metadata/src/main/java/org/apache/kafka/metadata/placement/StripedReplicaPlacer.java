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

package org.apache.kafka.metadata.placement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.metadata.OptionalStringComparator;


/**
 * The striped replica placer.
 * <p>
 * <h3>Goals</h3>
 * <p>The design of this placer attempts to satisfy a few competing goals. Firstly, we want
 * to spread the replicas as evenly as we can across racks. In the simple case where
 * broker racks have not been configured, this goal is a no-op, of course. But it is the
 * highest priority goal in multi-rack clusters.
 *
 * <p>Our second goal is to spread the replicas evenly across brokers. Since we are placing
 * multiple partitions, we try to avoid putting each partition on the same set of
 * replicas, even if it does satisfy the rack placement goal. If any specific broker is
 * fenced, we would like the new leaders to distributed evenly across the remaining
 * brokers.
 *
 * <p>However, we treat the rack placement goal as higher priority than this goal-- if you
 * configure 10 brokers in rack A and B, and 1 broker in rack C, you will end up with a
 * lot of partitions on that one broker in rack C.  If you were to place a lot of
 * partitions with replication factor 3, each partition would try to get a replica there.
 * In general racks are supposed to be about the same size -- if they aren't, this is a
 * user error.
 *
 * <p>Finally, we would prefer to place replicas on unfenced brokers, rather than on fenced
 * brokers.
 * <p>
 * <h3>Constraints</h3>
 * In addition to these goals, we have two constraints. Unlike the goals, these are not
 * optional -- they are mandatory. Placement will fail if a constraint cannot be
 * satisfied. The first constraint is that we can't place more than one replica on the
 * same broker. This imposes an upper limit on replication factor-- for example, a 3-node
 * cluster can't have any topics with replication factor 4. This constraint comes from
 * Kafka's internal design.
 *
 * <p>The second constraint is that the leader of each partition must be an unfenced broker.
 * This constraint is a bit arbitrary. In theory, we could allow people to create
 * new topics even if every broker were fenced. However, this would be confusing for
 * users.
 * <p>
 * <h3>Algorithm</h3>
 * <p>The StripedReplicaPlacer constructor loads the broker data into rack objects. Each
 * rack object contains a sorted list of fenced brokers, and a separate sorted list of
 * unfenced brokers. The racks themselves are organized into a sorted list, stored inside
 * the top-level RackList object.
 *
 * <p>The general idea is that we place replicas on to racks in a round-robin fashion. So if
 * we had racks A, B, C, and D, and we were creating a new partition with replication
 * factor 3, our first replica might come from A, our second from B, and our third from C.
 * Of course our placement would not be very fair if we always started with rack A.
 * Therefore, we generate a random starting offset when the RackList is created. So one
 * time we might go B, C, D. Another time we might go C, D, A. And so forth.
 *
 * <p>Note that each partition we generate advances the starting offset by one.
 * So in our 4-rack cluster, with 3 partitions, we might choose these racks:
 * <pre>
 * partition 1: A, B, C
 * partition 2: B, C, A
 * partition 3: C, A, B
 * </pre>
 * This is what generates the characteristic "striped" pattern of this placer.
 *
 * <p>So far I haven't said anything about how we choose a replica from within a rack.  In
 * fact, this is also done in a round-robin fashion. So if rack A had replica A0, A1, A2,
 * and A3, we might return A0 the first time, A1, the second, A2 the third, and so on.
 * Just like with the racks, we add a random starting offset to mix things up a bit.
 *
 * <p>So let's say you had a cluster with racks A, B, and C, and each rack had 3 replicas,
 * for 9 nodes in total.
 * If all the offsets were 0, you'd get placements like this:
 * <pre>
 * partition 1: A0, B0, C0
 * partition 2: B1, C1, A1
 * partition 3: C2, A2, B2
 * </pre>
 * <p>One additional complication with choosing a replica within a rack is that we want to
 * choose the unfenced replicas first. In a big cluster with lots of nodes available,
 * we'd prefer not to place a new partition on a node that is fenced. Therefore, we
 * actually maintain two lists, rather than the single list I described above.
 * We only start using the fenced node list when the unfenced node list is totally
 * exhausted.
 *
 * <p>Furthermore, we cannot place the first replica (the leader) of a new partition on a
 * fenced replica. Therefore, we have some special logic to ensure that this doesn't
 * happen.
 */
public class StripedReplicaPlacer implements ReplicaPlacer {
    /**
     * A list of brokers that we can iterate through.
     */
    static class BrokerList {
        final static BrokerList EMPTY = new BrokerList();
        private final List<Integer> brokers = new ArrayList<>(0);

        /**
         * How many brokers we have retrieved from the list during the current iteration epoch.
         */
        private int index = 0;

        /**
         * The offset to add to the index in order to calculate the list entry to fetch.  The
         * addition is done modulo the list size.
         */
        private int offset = 0;

        /**
         * The last known iteration epoch. If we call next with a different epoch than this, the
         * index and offset will be reset.
         */
        private int epoch = 0;

        BrokerList add(int broker) {
            this.brokers.add(broker);
            return this;
        }

        /**
         * Initialize this broker list by sorting it and randomizing the start offset.
         *
         * @param random    The random number generator.
         */
        void initialize(Random random) {
            if (!brokers.isEmpty()) {
                brokers.sort(Integer::compareTo);
                this.offset = random.nextInt(brokers.size());
            }
        }

        /**
         * Randomly shuffle the brokers in this list.
         */
        void shuffle(Random random) {
            Collections.shuffle(brokers, random);
        }

        /**
         * @return          The number of brokers in this list.
         */
        int size() {
            return brokers.size();
        }

        /**
         * Get the next broker in this list, or -1 if there are no more elements to be
         * returned.
         *
         * @param epoch     The current iteration epoch.
         *
         * @return          The broker ID, or -1 if there are no more brokers to be
         *                  returned in this epoch.
         */
        int next(int epoch) {
            if (brokers.isEmpty()) return -1;
            if (this.epoch != epoch) {
                this.epoch = epoch;
                this.index = 0;
                this.offset = (offset + 1) % brokers.size();
            }
            if (index >= brokers.size()) return -1;
            int broker = brokers.get((index + offset) % brokers.size());
            index++;
            return broker;
        }
    }

    /**
     * A rack in the cluster, which contains brokers.
     */
    static class Rack {
        private final BrokerList fenced = new BrokerList();
        private final BrokerList unfenced = new BrokerList();

        /**
         * Initialize this rack.
         *
         * @param random    The random number generator.
         */
        void initialize(Random random) {
            fenced.initialize(random);
            unfenced.initialize(random);
        }

        void shuffle(Random random) {
            fenced.shuffle(random);
            unfenced.shuffle(random);
        }

        BrokerList fenced() {
            return fenced;
        }

        BrokerList unfenced() {
            return unfenced;
        }

        /**
         * Get the next unfenced broker in this rack, or -1 if there are no more brokers
         * to be returned.
         *
         * @param epoch     The current iteration epoch.
         *
         * @return          The broker ID, or -1 if there are no more brokers to be
         *                  returned in this epoch.
         */
        int nextUnfenced(int epoch) {
            return unfenced.next(epoch);
        }

        /**
         * Get the next broker in this rack, or -1 if there are no more brokers to be
         * returned.
         *
         * @param epoch     The current iteration epoch.
         *
         * @return          The broker ID, or -1 if there are no more brokers to be
         *                  returned in this epoch.
         */
        int next(int epoch) {
            int result = unfenced.next(epoch);
            if (result >= 0) return result;
            return fenced.next(epoch);
        }
    }

    /**
     * A list of racks that we can iterate through.
     */
    static class RackList {
        /**
         * The random number generator.
         */
        private final Random random;

        /**
         * A map from rack names to the brokers contained within them.
         */
        private final Map<Optional<String>, Rack> racks = new HashMap<>();

        /**
         * The names of all the racks in the cluster.
         * Racks which have at least one unfenced broker come first (in sorted order),
         * followed by racks which have only fenced brokers (also in sorted order).
         */
        private final List<Optional<String>> rackNames = new ArrayList<>();

        /**
         * The total number of brokers in the cluster, both fenced and unfenced.
         */
        private final int numTotalBrokers;

        /**
         * The total number of unfenced brokers in the cluster.
         */
        private final int numUnfencedBrokers;

        /**
         * The iteration epoch.
         */
        private int epoch = 0;

        /**
         * The offset we use to determine which rack is returned first.
         */
        private int offset;

        RackList(Random random, Iterator<UsableBroker> iterator) {
            this.random = random;
            int numTotalBrokersCount = 0, numUnfencedBrokersCount = 0;
            while (iterator.hasNext()) {
                UsableBroker broker = iterator.next();
                Rack rack = racks.get(broker.rack());
                if (rack == null) {
                    rackNames.add(broker.rack());
                    rack = new Rack();
                    racks.put(broker.rack(), rack);
                }
                if (broker.fenced()) {
                    rack.fenced().add(broker.id());
                } else {
                    numUnfencedBrokersCount++;
                    rack.unfenced().add(broker.id());
                }
                numTotalBrokersCount++;
            }
            for (Rack rack : racks.values()) {
                rack.initialize(random);
            }
            this.rackNames.sort(OptionalStringComparator.INSTANCE);
            this.numTotalBrokers = numTotalBrokersCount;
            this.numUnfencedBrokers = numUnfencedBrokersCount;
            this.offset = rackNames.isEmpty() ? 0 : random.nextInt(rackNames.size());
        }

        int numTotalBrokers() {
            return numTotalBrokers;
        }

        int numUnfencedBrokers() {
            return numUnfencedBrokers;
        }

        // VisibleForTesting
        List<Optional<String>> rackNames() {
            return rackNames;
        }

        List<Integer> place(int replicationFactor) {
            throwInvalidReplicationFactorIfNonPositive(replicationFactor);
            throwInvalidReplicationFactorIfTooFewBrokers(replicationFactor, numTotalBrokers());
            throwInvalidReplicationFactorIfZero(numUnfencedBrokers());
            // If we have returned as many assignments as there are unfenced brokers in
            // the cluster, shuffle the rack list and broker lists to try to avoid
            // repeating the same assignments again.
            // But don't reset the iteration epoch for a single unfenced broker -- otherwise we would loop forever
            if (epoch == numUnfencedBrokers && numUnfencedBrokers > 1) {
                shuffle();
                epoch = 0;
            }
            if (offset == rackNames.size()) {
                offset = 0;
            }
            List<Integer> brokers = new ArrayList<>(replicationFactor);
            int firstRackIndex = offset;
            while (true) {
                Optional<String> name = rackNames.get(firstRackIndex);
                Rack rack = racks.get(name);
                int result = rack.nextUnfenced(epoch);
                if (result >= 0) {
                    brokers.add(result);
                    break;
                }
                firstRackIndex++;
                if (firstRackIndex == rackNames.size()) {
                    firstRackIndex = 0;
                }
            }
            int rackIndex = offset;
            for (int replica = 1; replica < replicationFactor; replica++) {
                int result = -1;
                do {
                    if (rackIndex == firstRackIndex) {
                        firstRackIndex = -1;
                    } else {
                        Optional<String> rackName = rackNames.get(rackIndex);
                        Rack rack = racks.get(rackName);
                        result = rack.next(epoch);
                    }
                    rackIndex++;
                    if (rackIndex == rackNames.size()) {
                        rackIndex = 0;
                    }
                } while (result < 0);
                brokers.add(result);
            }
            epoch++;
            offset++;
            return brokers;
        }

        void shuffle() {
            Collections.shuffle(rackNames, random);
            for (Rack rack : racks.values()) {
                rack.shuffle(random);
            }
        }
    }

    private static void throwInvalidReplicationFactorIfNonPositive(int replicationFactor) {
        if (replicationFactor <= 0) {
            throw new InvalidReplicationFactorException("Invalid replication factor " +
                    replicationFactor + ": the replication factor must be positive.");
        }
    }

    private static void throwInvalidReplicationFactorIfZero(int numUnfenced) {
        if (numUnfenced == 0) {
            throw new InvalidReplicationFactorException("All brokers are currently fenced.");
        }
    }

    private static void throwInvalidReplicationFactorIfTooFewBrokers(int replicationFactor, int numTotalBrokers) {
        if (replicationFactor > numTotalBrokers) {
            throw new InvalidReplicationFactorException("The target replication factor " +
                    "of " + replicationFactor + " cannot be reached because only " +
                    numTotalBrokers + " broker(s) are registered.");
        }
    }

    private final Random random;

    public StripedReplicaPlacer(Random random) {
        this.random = random;
    }

    @Override
    public TopicAssignment place(
        PlacementSpec placement,
        ClusterDescriber cluster
    ) throws InvalidReplicationFactorException {
        RackList rackList = new RackList(random, cluster.usableBrokers());
        throwInvalidReplicationFactorIfNonPositive(placement.numReplicas());
        throwInvalidReplicationFactorIfZero(rackList.numUnfencedBrokers());
        throwInvalidReplicationFactorIfTooFewBrokers(placement.numReplicas(),
            rackList.numTotalBrokers());
        List<List<Integer>> placements = new ArrayList<>(placement.numPartitions());
        for (int partition = 0; partition < placement.numPartitions(); partition++) {
            placements.add(rackList.place(placement.numReplicas()));
        }
        return new TopicAssignment(
            placements.stream().map(replicas -> new PartitionAssignment(replicas, cluster)).collect(Collectors.toList())
        );
    }
}

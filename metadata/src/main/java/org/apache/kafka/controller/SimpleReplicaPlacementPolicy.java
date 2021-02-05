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

package org.apache.kafka.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.metadata.UsableBroker;


/**
 * A simple uniformly random placement policy.
 *
 * TODO: implement the current "striped" placement policy, plus rack aware placement
 * policies, etc.
 */
public class SimpleReplicaPlacementPolicy implements ReplicaPlacementPolicy {
    private final Random random;

    public SimpleReplicaPlacementPolicy(Random random) {
        this.random = random;
    }

    @Override
    public List<List<Integer>> createPlacement(int numPartitions,
                                               short numReplicas,
                                               Iterator<UsableBroker> iterator) {
        List<UsableBroker> usable = new ArrayList<>();
        while (iterator.hasNext()) {
            usable.add(iterator.next());
        }
        if (usable.size() < numReplicas) {
            throw new InvalidReplicationFactorException("there are only " + usable.size() +
                " usable brokers");
        }
        List<List<Integer>> results = new ArrayList<>();
        for (int p = 0; p < numPartitions; p++) {
            List<Integer> choices = new ArrayList<>();
            // TODO: rack-awareness
            List<Integer> indexes = new ArrayList<>();
            int initialIndex = random.nextInt(usable.size());
            for (int i = 0; i < numReplicas; i++) {
                indexes.add((initialIndex + i) % usable.size());
            }
            indexes.sort(Integer::compareTo);
            Iterator<UsableBroker> iter = usable.iterator();
            for (int i = 0; choices.size() < indexes.size(); i++) {
                int brokerId = iter.next().id();
                if (indexes.get(choices.size()) == i) {
                    choices.add(brokerId);
                }
            }
            Collections.shuffle(choices, random);
            results.add(choices);
        }
        return results;
    }
}

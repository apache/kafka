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

package org.apache.kafka.stream.kstream.internals;

import org.apache.kafka.stream.processor.KafkaProcessor;
import org.apache.kafka.stream.processor.PTopologyBuilder;
import org.apache.kafka.stream.KStream;
import org.apache.kafka.stream.Predicate;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

class KStreamBranch<K, V> extends KafkaProcessor<K, V, K, V> {

    private static final String BRANCH_NAME = "KAFKA-BRANCH-";
    private static final AtomicInteger BRANCH_INDEX = new AtomicInteger(1);

    private final PTopologyBuilder topology;
    private final Predicate<K, V>[] predicates;
    private final KStreamFilter<K, V>[] branches;

    @SuppressWarnings("unchecked")
    public KStreamBranch(Predicate<K, V>[] predicates, PTopologyBuilder topology, String parent) {
        super(BRANCH_NAME);

        this.topology = topology;
        this.predicates = Arrays.copyOf(predicates, predicates.length);
        this.branches = (KStreamFilter<K, V>[]) Array.newInstance(KStreamFilter.class, predicates.length);

        // NOTE that branches here is just a list of predicates, hence not necessarily mutual exclusive
        for (int i = 0; i < branches.length; i++) {
            branches[i] = new KStreamFilter<>(predicates[i], false);
            topology.addProcessor(BRANCH_NAME + BRANCH_INDEX.getAndIncrement(), branches[i], parent);
        }
    }

    @Override
    public void process(K key, V value) {
        forward(key, value);
    }

    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branches() {
        KStream<K, V>[] streams = (KStreamSource<K, V>[]) Array.newInstance(KStreamSource.class, branches.length);
        for (int i = 0; i < branches.length; i++) {
            streams[i] = new KStreamSource<>(topology, branches[i]);
        }
        return streams;
    }
}

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

package org.apache.kafka.stream.topology.internals;

import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.internals.Receiver;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.Predicate;

import java.lang.reflect.Array;
import java.util.Arrays;

class KStreamBranch<K, V> implements Receiver {

    private final Predicate<K, V>[] predicates;
    final KStreamSource<K, V>[] branches;

    @SuppressWarnings("unchecked")
    KStreamBranch(Predicate<K, V>[] predicates, KStreamTopology topology) {
        this.predicates = Arrays.copyOf(predicates, predicates.length);
        this.branches = (KStreamSource<K, V>[]) Array.newInstance(KStreamSource.class, predicates.length);
        for (int i = 0; i < branches.length; i++) {
            branches[i] = new KStreamSource<>(null, topology);
        }
    }

    @Override
    public void bind(KStreamContext context, KStreamMetadata metadata) {
        for (KStreamSource<K, V> branch : branches) {
            branch.bind(context, metadata);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void receive(Object key, Object value, long timestamp) {
        for (int i = 0; i < predicates.length; i++) {
            Predicate<K, V> predicate = predicates[i];
            if (predicate.apply((K) key, (V) value)) {
                branches[i].receive(key, value, timestamp);
                return;
            }
        }
    }

    @Override
    public void close() {
        for (KStreamSource<K, V> branch : branches) {
            branch.close();
        }
    }

}

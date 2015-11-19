/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStreamWindowed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.WindowSupplier;

import java.util.HashSet;
import java.util.Set;

public final class KStreamWindowedImpl<K, V> extends KStreamImpl<K, V> implements KStreamWindowed<K, V> {

    private final WindowSupplier<K, V> windowSupplier;

    public KStreamWindowedImpl(KStreamBuilder topology, String name, Set<String> sourceNodes, WindowSupplier<K, V> windowSupplier) {
        super(topology, name, sourceNodes);
        this.windowSupplier = windowSupplier;
    }

    @Override
    public <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, ValueJoiner<V, V1, V2> valueJoiner) {
        String thisWindowName = this.windowSupplier.name();
        String otherWindowName = ((KStreamWindowedImpl<K, V1>) other).windowSupplier.name();
        Set<String> thisSourceNodes = this.sourceNodes;
        Set<String> otherSourceNodes = ((KStreamWindowedImpl<K, V1>) other).sourceNodes;

        if (thisSourceNodes == null || otherSourceNodes == null)
            throw new KafkaException("not joinable");

        Set<String> allSourceNodes = new HashSet<>(sourceNodes);
        allSourceNodes.addAll(((KStreamWindowedImpl<K, V1>) other).sourceNodes);

        KStreamJoin<K, V2, V, V1> joinThis = new KStreamJoin<>(otherWindowName, valueJoiner);
        KStreamJoin<K, V2, V1, V> joinOther = new KStreamJoin<>(thisWindowName, KStreamJoin.reverseJoiner(valueJoiner));
        KStreamPassThrough<K, V2> joinMerge = new KStreamPassThrough<>();

        String joinThisName = topology.newName(JOINTHIS_NAME);
        String joinOtherName = topology.newName(JOINOTHER_NAME);
        String joinMergeName = topology.newName(MERGE_NAME);

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, ((KStreamImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        topology.copartitionSources(allSourceNodes);

        return new KStreamImpl<>(topology, joinMergeName, allSourceNodes);
    }
}

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

package org.apache.kafka.streaming.kstream.internals;

import org.apache.kafka.streaming.kstream.KStream;
import org.apache.kafka.streaming.kstream.KStreamWindowed;
import org.apache.kafka.streaming.kstream.ValueJoiner;
import org.apache.kafka.streaming.kstream.WindowDef;
import org.apache.kafka.streaming.processor.TopologyBuilder;

public final class KStreamWindowedImpl<K, V> extends KStreamImpl<K, V> implements KStreamWindowed<K, V> {

    private final WindowDef<K, V> windowDef;

    public KStreamWindowedImpl(TopologyBuilder topology, String name, WindowDef<K, V> windowDef) {
        super(topology, name);
        this.windowDef = windowDef;
    }

    @Override
    public <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, ValueJoiner<V, V1, V2> valueJoiner) {
        return join(other, false, valueJoiner);
    }

    @Override
    public <V1, V2> KStream<K, V2> joinPrior(KStreamWindowed<K, V1> other, ValueJoiner<V, V1, V2> valueJoiner) {
        return join(other, true, valueJoiner);
    }

    private <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, boolean prior, ValueJoiner<V, V1, V2> valueJoiner) {
        String thisWindowName = this.windowDef.name();
        String otherWindowName = ((KStreamWindowedImpl<K, V1>) other).windowDef.name();

        KStreamJoin<K, V2, V, V1> join = new KStreamJoin<>(thisWindowName, otherWindowName, prior, valueJoiner);

        String joinName = JOIN_NAME + INDEX.getAndIncrement();
        String joinOtherName = JOINOTHER_NAME + INDEX.getAndIncrement();
        topology.addProcessor(joinName, join, this.name);
        topology.addProcessor(joinOtherName, join.processorDefForOtherStream, ((KStreamImpl) other).name);

        return new KStreamImpl<>(topology, joinName);
    }
}

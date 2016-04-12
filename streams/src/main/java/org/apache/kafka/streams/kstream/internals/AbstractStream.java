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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractStream<K> {

    protected final KStreamBuilder topology;
    protected final String name;
    protected final Set<String> sourceNodes;

    public AbstractStream(KStreamBuilder topology, String name, Set<String> sourceNodes) {
        this.topology = topology;
        this.name = name;
        this.sourceNodes = sourceNodes;
    }

    /**
     * @throws TopologyBuilderException if the streams are not joinable
     */
    protected Set<String> ensureJoinableWith(AbstractStream<K> other) {
        Set<String> thisSourceNodes = sourceNodes;
        Set<String> otherSourceNodes = other.sourceNodes;

        if (thisSourceNodes == null || otherSourceNodes == null)
            throw new TopologyBuilderException(this.name + " and " + other.name + " are not joinable");

        Set<String> allSourceNodes = new HashSet<>();
        allSourceNodes.addAll(thisSourceNodes);
        allSourceNodes.addAll(otherSourceNodes);

        topology.copartitionSources(allSourceNodes);

        return allSourceNodes;
    }

    public static <T2, T1, R> ValueJoiner<T2, T1, R> reverseJoiner(final ValueJoiner<T1, T2, R> joiner) {
        return new ValueJoiner<T2, T1, R>() {
            @Override
            public R apply(T2 value2, T1 value1) {
                return joiner.apply(value1, value2);
            }
        };
    }

}

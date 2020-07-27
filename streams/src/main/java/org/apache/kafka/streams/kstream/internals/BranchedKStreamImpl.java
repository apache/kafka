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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Map;

public class BranchedKStreamImpl<K, V> implements BranchedKStream<K, V> {

    private final KStream<K, V> source;
    private final NamedInternal named;

    BranchedKStreamImpl(final KStream<K, V> source, final NamedInternal named) {
        this.source = source;
        this.named = named;
    }

    @Override
    public BranchedKStream<K, V> branch(final Predicate predicate) {
        return null;
    }

    @Override
    public BranchedKStream<K, V> branch(final Predicate predicate, final Branched branched) {
        return null;
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch() {
        return null;
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch(final Branched<K, V> branched) {
        return null;
    }

    @Override
    public Map<String, KStream<K, V>> noDefaultBranch() {
        return null;
    }
}

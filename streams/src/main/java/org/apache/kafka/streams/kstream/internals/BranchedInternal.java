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
import org.apache.kafka.streams.kstream.KStream;

import java.util.function.Consumer;
import java.util.function.Function;

class BranchedInternal<K, V> extends Branched<K, V> {
    BranchedInternal(final Branched<K, V> branched) {
        super(branched);
    }

    BranchedInternal() {
        super(null, null, null);
    }

    static <K, V> BranchedInternal<K, V> empty() {
        return new BranchedInternal<>();
    }

    String name() {
        return name;
    }

    public Function<? super KStream<K, V>, ? extends KStream<K, V>> chainFunction() {
        return chainFunction;
    }

    public Consumer<? super KStream<K, V>> chainConsumer() {
        return chainConsumer;
    }
}

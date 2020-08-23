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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.NamedInternal;

public class DistinctParameters<K, V, I> extends Named {
    protected final TimeWindows timeWindows;
    protected final KeyValueMapper<K, V, I> idExtractor;
    protected final boolean isPersistent;
    protected final Serde<I> idSerde;

    protected DistinctParameters(final DistinctParameters<K, V, I> distinctParameters) {
        super(distinctParameters);
        this.timeWindows = distinctParameters.timeWindows;
        this.idExtractor = distinctParameters.idExtractor;
        this.isPersistent = distinctParameters.isPersistent;
        this.idSerde = distinctParameters.idSerde;
    }

    private DistinctParameters(final Named named,
                               final TimeWindows timeWindows,
                               final KeyValueMapper<K, V, I> idExtractor,
                               final Serde<I> idSerde,
                               final boolean isPersistent) {
        super(named);
        this.timeWindows = timeWindows;
        this.idExtractor = idExtractor;
        this.isPersistent = isPersistent;
        this.idSerde = idSerde;
    }


    public static <K, V> DistinctParameters<K, V, K> with(final TimeWindows timeWindows) {
        return new DistinctParameters<>(NamedInternal.empty(),
                timeWindows, null, null, true);
    }

    public static <K, V, I> DistinctParameters<K, V, I> with(final TimeWindows timeWindows,
                                                             final KeyValueMapper<K, V, I> idExtractor,
                                                             final Serde<I> idSerde) {
        return new DistinctParameters<>(NamedInternal.empty(),
                timeWindows,
                idExtractor,
                idSerde,
                true);
    }

    public static <K, V, I> DistinctParameters<K, V, I> with(final TimeWindows timeWindows,
                                                             final KeyValueMapper<K, V, I> idExtractor,
                                                             final Serde<I> idSerde,
                                                             final boolean isPersistent) {
        return new DistinctParameters<>(NamedInternal.empty(),
                timeWindows,
                idExtractor,
                idSerde,
                false);
    }
}

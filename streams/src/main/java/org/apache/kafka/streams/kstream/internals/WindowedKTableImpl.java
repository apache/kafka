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

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedKTable;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Set;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
class WindowedKTableImpl<K, S, V> extends KTableImpl<Windowed<K>, S, V> implements WindowedKTable<K, V> {

    WindowedKTableImpl(final InternalStreamsBuilder builder,
                       final String name,
                       final ProcessorSupplier<?, ?> processorSupplier,
                       final Set<String> sourceNodes,
                       final String queryableStoreName,
                       final boolean isQueryable) {
        super(builder, name, processorSupplier, sourceNodes, queryableStoreName, isQueryable);
    }
}

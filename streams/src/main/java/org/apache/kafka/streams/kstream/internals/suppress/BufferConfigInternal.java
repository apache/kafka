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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;

import static org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy.SHUT_DOWN;

abstract class BufferConfigInternal<BC extends Suppressed.BufferConfig<BC>> implements Suppressed.BufferConfig<BC> {
    public abstract long maxRecords();

    public abstract long maxBytes();

    @SuppressWarnings("unused")
    public abstract BufferFullStrategy bufferFullStrategy();

    @Override
    public Suppressed.StrictBufferConfig withNoBound() {
        return new StrictBufferConfigImpl(
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            SHUT_DOWN // doesn't matter, given the bounds
        );
    }

    @Override
    public Suppressed.StrictBufferConfig shutDownWhenFull() {
        return new StrictBufferConfigImpl(maxRecords(), maxBytes(), SHUT_DOWN);
    }

    @Override
    public Suppressed.EagerBufferConfig emitEarlyWhenFull() {
        return new EagerBufferConfigImpl(maxRecords(), maxBytes());
    }
}

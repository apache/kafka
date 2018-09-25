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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public class KTableSuppressProcessor<K, V> implements Processor<K, Change<V>> {
    private final SuppressedImpl<K> suppress;
    private InternalProcessorContext internalProcessorContext;

    private final Serde<K> keySerde;
    private final Serde<Change<V>> valueSerde;

    public KTableSuppressProcessor(final SuppressedImpl<K> suppress,
                                   final Serde<K> keySerde,
                                   final Serde<Change<V>> valueSerde) {
        this.suppress = requireNonNull(suppress);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;
    }

    @Override
    public void process(final K key, final Change<V> value) {
        if (suppress.getTimeToWaitForMoreEvents() == Duration.ZERO && definedRecordTime(key) <= internalProcessorContext.streamTime()) {
            if (shouldForward(value)) {
                internalProcessorContext.forward(key, value);
            } // else skip
        } else {
            throw new NotImplementedException();
        }
    }

    private boolean shouldForward(final Change<V> value) {
        return !(value.newValue == null && suppress.suppressTombstones());
    }

    private long definedRecordTime(final K key) {
        return suppress.getTimeDefinition().time(internalProcessorContext, key);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "KTableSuppressProcessor{" +
            "suppress=" + suppress +
            ", keySerde=" + keySerde +
            ", valueSerde=" + valueSerde +
            '}';
    }

    public static class NotImplementedException extends RuntimeException {
        NotImplementedException() {
            super();
        }
    }
}
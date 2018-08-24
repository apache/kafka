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

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

import java.time.Duration;

public class KTableSuppressProcessor<K, V> implements Processor<K, Change<V>> {
    private final SuppressedImpl<K> suppress;
    private InternalProcessorContext internalProcessorContext;

    public KTableSuppressProcessor(final SuppressedImpl<K> suppress) {
        this.suppress = suppress;
    }

    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;
    }

    @Override
    public void process(final K key, final Change<V> value) {
        if (suppress.getTimeToWaitForMoreEvents() == Duration.ZERO && definedRecordTime(key) <= internalProcessorContext.streamTime()) {
            internalProcessorContext.forward(key, value);
        } else {
            throw new NotImplementedException();
        }
    }

    private long definedRecordTime(final K key) {
        return suppress.getTimeDefinition().time(internalProcessorContext, key);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "KTableSuppressProcessor{suppress=" + suppress + '}';
    }

    static class NotImplementedException extends RuntimeException {
        NotImplementedException() {
            super();
        }
    }
}
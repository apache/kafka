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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.internals.CacheFlushListener;

class SessionCacheFlushListener<K, V> implements CacheFlushListener<Windowed<K>, V> {
    private final InternalProcessorContext context;
    private final ProcessorNode myNode;

    SessionCacheFlushListener(final ProcessorContext context) {
        this.context = (InternalProcessorContext) context;
        myNode = this.context.currentNode();
    }

    @Override
    public void apply(final Windowed<K> key,
                      final V newValue,
                      final V oldValue,
                      final long timestamp) {
        final ProcessorNode prev = context.currentNode();
        context.setCurrentNode(myNode);
        try {
            context.forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(key.window().end()));
        } finally {
            context.setCurrentNode(prev);
        }
    }
}

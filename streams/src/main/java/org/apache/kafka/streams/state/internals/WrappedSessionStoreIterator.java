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
package org.apache.kafka.streams.state.internals;

import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

class WrappedSessionStoreIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {

    private final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final Function<Bytes, Windowed<Bytes>> windowConstructor;

    WrappedSessionStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator) {
        this(bytesIterator, SessionKeySchema::from);
    }

    WrappedSessionStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                                final Function<Bytes, Windowed<Bytes>> windowConstructor) {
        this.bytesIterator = bytesIterator;
        this.windowConstructor = windowConstructor;
    }

    @Override
    public void close() {
        bytesIterator.close();
    }

    @Override
    public Windowed<Bytes> peekNextKey() {
        return windowConstructor.apply(bytesIterator.peekNextKey());
    }

    @Override
    public boolean hasNext() {
        return bytesIterator.hasNext();
    }

    @Override
    public KeyValue<Windowed<Bytes>, byte[]> next() {
        final KeyValue<Bytes, byte[]> next = bytesIterator.next();
        return KeyValue.pair(windowConstructor.apply(next.key), next.value);
    }
}
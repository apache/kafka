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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class WindowedKeyValueIterator extends AbstractIterator<KeyValue<Windowed<Bytes>, byte[]>> implements KeyValueIterator<Windowed<Bytes>, byte[]> {

    private KeyValue<Windowed<Bytes>, byte[]> next;
    private final Iterator<KeyValue<Windowed<Bytes>, byte[]>> windows;

    public WindowedKeyValueIterator(final Iterator<KeyValue<Windowed<Bytes>, byte[]>> windows) {
        this.windows = windows;
    }

    @Override
    protected KeyValue<Windowed<Bytes>, byte[]> makeNext() {
        if (!windows.hasNext())
            return allDone();
        next = windows.next();
        return next;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public Windowed<Bytes> peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next.key;
    }
}

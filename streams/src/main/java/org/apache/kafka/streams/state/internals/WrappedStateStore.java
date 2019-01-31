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

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.TimestampedBytesStore;

/**
 * A storage engine wrapper for utilities like logging, caching, and metering.
 */
public class WrappedStateStore<S extends StateStore> implements StateStore {
    public static boolean isTimestamped(StateStore stateStore) {
        if (stateStore instanceof TimestampedBytesStore) {
            return true;
        } else if (stateStore instanceof WrappedStateStore) {
            return isTimestamped(((WrappedStateStore) stateStore).wrappedStore());
        } else {
            return false;
        }
    }

    private final S innerState;

    public WrappedStateStore(final S innerState) {
        this.innerState = innerState;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        innerState.init(context, root);
    }

    @Override
    public String name() {
        return innerState.name();
    }

    @Override
    public boolean persistent() {
        return innerState.persistent();
    }

    @Override
    public boolean isOpen() {
        return innerState.isOpen();
    }

    public void validateStoreOpen() {
        if (!innerState.isOpen()) {
            throw new InvalidStateStoreException("Store " + innerState.name() + " is currently closed.");
        }
    }

    @Override
    public void flush() {
        innerState.flush();
    }

    @Override
    public void close() {
        innerState.close();
    }

    public S wrappedStore() {
        return innerState;
    }
}

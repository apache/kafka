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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

/**
 * Simple wrapper around a {@link SessionStore} to support writing
 * updates to a changelog
 */
public class ChangeLoggingSessionBytesStore
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, byte[], byte[]>
    implements SessionStore<Bytes, byte[]> {

    private InternalProcessorContext<?, ?> internalContext;

    ChangeLoggingSessionBytesStore(final SessionStore<Bytes, byte[]> bytesStore) {
        super(bytesStore);
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        internalContext = asInternalProcessorContext(stateStoreContext);
        super.init(stateStoreContext, root);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return wrapped().backwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom, final Bytes keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom, final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return wrapped().backwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        wrapped().remove(sessionKey);
        internalContext.logChange(name(), SessionKeySchema.toBinary(sessionKey), null, internalContext.recordContext().timestamp(), wrapped().getPosition());
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        wrapped().put(sessionKey, aggregate);
        internalContext.logChange(name(), SessionKeySchema.toBinary(sessionKey), aggregate, internalContext.recordContext().timestamp(), wrapped().getPosition());
    }

    @Override
    public byte[] fetchSession(final Bytes key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return wrapped().fetchSession(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final long earliestSessionEndTime,
                                                                  final long latestSessionEndTime) {
        return wrapped().findSessions(earliestSessionEndTime, latestSessionEndTime);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
        return wrapped().backwardFetch(key);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return wrapped().fetch(key);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
        return wrapped().backwardFetch(keyFrom, keyTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
        return wrapped().fetch(keyFrom, keyTo);
    }
}
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Optional;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

/**
 * Simple wrapper around a {@link SessionStore} to support writing
 * updates to a changelog
 */
class ChangeLoggingSessionBytesStore
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, byte[], byte[]>
    implements SessionStore<Bytes, byte[]> {

    private InternalProcessorContext context;
    private Position position;
    private boolean consistencyEnabled = false;

    ChangeLoggingSessionBytesStore(final SessionStore<Bytes, byte[]> bytesStore) {
        super(bytesStore);
        this.position = Position.emptyPosition();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                context.appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false);
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
    @SuppressWarnings("unchecked")
    public void remove(final Windowed<Bytes> sessionKey) {
        if (context.recordMetadata().isPresent()) {
            final RecordMetadata meta = context.recordMetadata().get();
            position = position.update(meta.topic(), meta.partition(), meta.offset());
        }
        wrapped().remove(sessionKey);
        Optional<Position> optionalPosition = Optional.empty();
        if (consistencyEnabled) {
            optionalPosition = Optional.of(position);
        }
        context.logChange(name(), SessionKeySchema.toBinary(sessionKey), null, context.timestamp(), optionalPosition);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        if (context.recordMetadata().isPresent()) {
            final RecordMetadata meta = context.recordMetadata().get();
            position = position.update(meta.topic(), meta.partition(), meta.offset());
        }
        wrapped().put(sessionKey, aggregate);
        Optional<Position> optionalPosition = Optional.empty();
        if (consistencyEnabled) {
            optionalPosition = Optional.of(position);
        }
        context.logChange(
                name(), SessionKeySchema.toBinary(sessionKey), aggregate, context.timestamp(), optionalPosition);
    }

    @Override
    public byte[] fetchSession(final Bytes key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return wrapped().fetchSession(key, earliestSessionEndTime, latestSessionStartTime);
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

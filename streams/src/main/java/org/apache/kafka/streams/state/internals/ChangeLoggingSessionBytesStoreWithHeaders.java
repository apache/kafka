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
import org.apache.kafka.streams.state.SessionStore;

import static org.apache.kafka.streams.state.internals.AggregationWithHeadersDeserializer.headers;
import static org.apache.kafka.streams.state.internals.AggregationWithHeadersDeserializer.rawAggregation;

/**
 * Change-logging wrapper for a session bytes store whose values also carry headers.
 * <p>
 * The header-aware serialized value format is produced by {@link AggregationWithHeadersSerializer}.
 * <p>
 * Semantics:
 *  - The inner store value format is:
 *        [headersSize(varint)][headersBytes][aggregationBytes]
 *  - The changelog record value logged via {@code logChange(...)} is just the {@code aggregation}
 *    (no headers prefix), and the headers are logged separately.
 */
public class ChangeLoggingSessionBytesStoreWithHeaders
    extends ChangeLoggingSessionBytesStore {

    ChangeLoggingSessionBytesStoreWithHeaders(final SessionStore<Bytes, byte[]> bytesStore) {
        super(bytesStore);
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        wrapped().remove(sessionKey);
        internalContext.logChange(
            name(),
            SessionKeySchema.toBinary(sessionKey),
            null,
            internalContext.recordContext().timestamp(),
            internalContext.recordContext().headers(),
            wrapped().getPosition()
        );
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregationWithHeaders) {
        wrapped().put(sessionKey, aggregationWithHeaders);
        internalContext.logChange(
            name(),
            SessionKeySchema.toBinary(sessionKey),
            rawAggregation(aggregationWithHeaders),
            internalContext.recordContext().timestamp(),
            aggregationWithHeaders == null
                ? internalContext.recordContext().headers()
                : headers(aggregationWithHeaders),
            wrapped().getPosition()
        );
    }
}

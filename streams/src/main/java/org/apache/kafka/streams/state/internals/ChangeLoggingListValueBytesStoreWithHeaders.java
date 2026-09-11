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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * The HEADERS-mode changelog store for the outer-join {@link ListValueStore}.
 * <p>
 * The local store holds {@code [headersSize][headers][flag][value]} per list element, but that format
 * must never reach the changelog: the changelog topic is the only durable copy of the state, so its
 * value format is a permanent compatibility contract. If we logged the local bytes verbatim, an old
 * PLAIN reader — after a version downgrade, or simply after flipping {@code dsl.store.format} back to
 * PLAIN — would read each element's leading empty-headers {@code 0x00} as the {@code LeftOrRightValue}
 * flag and silently mistake left values for right ones.
 * <p>
 * So this store does what every other KIP-1271 changelog store does (see
 * {@link ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders}, which logs
 * {@link Utils#rawPlainValue(byte[])}): it keeps the headers out of the value and puts them in a record
 * header instead. The list makes that a little more involved — one changelog record holds the whole
 * list, so N sets of headers have to share one header field — which is why the stripped prefixes are
 * concatenated into a single self-delimiting blob under
 * {@link ListValueStoreUpgradeUtils#LIST_VALUE_HEADERS_HEADER_KEY} rather than unpacked into individual
 * {@code RecordHeader}s.
 * <p>
 * Implements {@link HeadersAwareListValueStore} purely so {@code StateManagerUtil.converterForStore}
 * selects {@link RecordConverters#rawListValueToHeadersListValue()}, which performs the inverse join on
 * restore.
 */
public class ChangeLoggingListValueBytesStoreWithHeaders
    extends ChangeLoggingListValueBytesStore
    implements HeadersAwareListValueStore {

    ChangeLoggingListValueBytesStoreWithHeaders(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        wrapped().put(key, value);
        // As in the parent, a tombstone deletes the whole list, so there is nothing to read back and
        // no per-element headers to carry.
        if (value == null) {
            log(key, null, internalContext.recordContext().timestamp(), changelogHeaders(null));
        } else {
            final ListValueStoreUpgradeUtils.SplitListBlob split =
                ListValueStoreUpgradeUtils.splitHeadersListBlob(wrapped().get(key));
            log(key, split.plainListBlob, internalContext.recordContext().timestamp(), changelogHeaders(split.elementHeaders));
        }
    }

    private Headers changelogHeaders(final byte[] elementHeaders) {
        // Copy, for two reasons: the live record headers are forwarded downstream, so neither our
        // control header nor the vector clock that ProcessorContextImpl#logChange appends to whatever
        // instance we hand it may leak into them.
        final Headers headers = new RecordHeaders(internalContext.recordContext().headers());
        headers.remove(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY);
        if (elementHeaders != null) {
            headers.add(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY, elementHeaders);
        }
        return headers;
    }
}

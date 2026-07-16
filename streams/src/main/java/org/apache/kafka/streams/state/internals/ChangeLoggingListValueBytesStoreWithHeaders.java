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
 * It stamps a reserved {@link ListValueStoreUpgradeUtils#LIST_VALUE_FORMAT_HEADER_KEY format marker}
 * onto every changelog record it writes, so that on restore the
 * {@link RecordConverters#rawListValueToHeadersListValue() converter} can distinguish an already
 * headers-format blob (produced by this version) from a legacy PLAIN blob (produced before the
 * upgrade) and only convert the latter.
 * <p>
 * Implements {@link HeadersAwareListValueStore} purely so {@code StateManagerUtil.converterForStore}
 * selects that converter.
 */
public class ChangeLoggingListValueBytesStoreWithHeaders
    extends ChangeLoggingListValueBytesStore
    implements HeadersAwareListValueStore {

    ChangeLoggingListValueBytesStoreWithHeaders(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    protected Headers changelogHeaders() {
        // Copy so we never mutate the live record headers (which are forwarded downstream).
        final Headers headers = new RecordHeaders(internalContext.recordContext().headers());
        headers.remove(ListValueStoreUpgradeUtils.LIST_VALUE_FORMAT_HEADER_KEY);
        headers.add(ListValueStoreUpgradeUtils.LIST_VALUE_FORMAT_HEADER_KEY, ListValueStoreUpgradeUtils.HEADERS_FORMAT_MARKER);
        return headers;
    }
}

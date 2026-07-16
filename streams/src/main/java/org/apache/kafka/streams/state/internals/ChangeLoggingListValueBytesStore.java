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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;

public class ChangeLoggingListValueBytesStore extends ChangeLoggingKeyValueBytesStore {

    // Unlike ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders, this store reads the headers to
    // log straight from internalContext.recordContext() rather than parsing them back out of the
    // stored value bytes. That is safe here because this store is always built with caching disabled
    // (see OuterStreamJoinStoreFactory.withCachingDisabled()): log() runs synchronously within the
    // same process() call as the put, so recordContext() still refers to the record currently being
    // processed — i.e. the very record whose headers were just stored. There is therefore no need for
    // a dedicated *WithHeaders subclass that recovers the headers from the value blob.
    ChangeLoggingListValueBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        wrapped().put(key, value);
        // the provided new value will be added to the list in the inner put()
        // we need to log the full new list and thus call get() on the inner store below
        // if the value is a tombstone, we delete the whole list and thus can save the get call
        if (value == null) {
            log(key, null, internalContext.recordContext().timestamp(), changelogHeaders());
        } else {
            log(key, wrapped().get(key), internalContext.recordContext().timestamp(), changelogHeaders());
        }
    }

    /**
     * The headers to attach to the changelog record. Defaults to the current record's headers; the
     * headers-aware subclass overrides this to also stamp the list-value format marker.
     */
    protected Headers changelogHeaders() {
        return internalContext.recordContext().headers();
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        final byte[] oldValue = wrapped().get(key);

        if (oldValue != null) {
            put(key, value);
        }

        // TODO: here we always return null so that deser would not fail.
        //       we only do this since we know the only caller (stream-stream join processor)
        //       would not need the actual value at all
        return null;
    }

}

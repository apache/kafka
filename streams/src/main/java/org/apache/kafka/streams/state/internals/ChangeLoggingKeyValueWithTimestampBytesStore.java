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
import org.apache.kafka.streams.state.KeyValueStore;

// We know the inner store's byte format is with timestamps,
// but we don't want to send them in the value to the changelog,
// so this class's job is to strip them off before logging.
class ChangeLoggingKeyValueWithTimestampBytesStore extends ChangeLoggingKeyValueBytesStore implements StoreWithTimestamps {
    ChangeLoggingKeyValueWithTimestampBytesStore(final KeyValueStore<Bytes, byte[]> innerStoreWithTimestamps) {
        super(innerStoreWithTimestamps);
    }

    @Override
    void logChange(final Bytes key, final byte[] valueAndTimestamp) {
        final byte[] rawValue = new byte[valueAndTimestamp.length - 8];

        System.arraycopy(valueAndTimestamp, 8, rawValue, 0, valueAndTimestamp.length - 8);

        // note, I removed the timestamp here. IIUC, it should always be the same as the one in the context anyway...
        changeLogger.logChange(key, rawValue);
    }
}
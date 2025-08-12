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
import org.apache.kafka.streams.state.WindowStore;

import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.timestamp;

class ChangeLoggingTimestampedWindowBytesStore extends ChangeLoggingWindowBytesStore {

    ChangeLoggingTimestampedWindowBytesStore(final WindowStore<Bytes, byte[]> bytesStore,
                                             final boolean retainDuplicates) {
        super(bytesStore, retainDuplicates, WindowKeySchema::toStoreKeyBinary);
    }

    @Override
    void log(final Bytes key,
             final byte[] valueAndTimestamp) {
        internalContext.logChange(
            name(),
            key,
            rawValue(valueAndTimestamp),
            valueAndTimestamp != null ? timestamp(valueAndTimestamp) : internalContext.recordContext().timestamp(),
            wrapped().getPosition()
        );
    }
}

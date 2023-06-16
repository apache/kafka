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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.Bytes;

/**
 * A representation of a versioned key-value store as a {@link KeyValueStore} of type &lt;Bytes, byte[]&gt;.
 * See {@link VersionedBytesStoreSupplier} for more.
 */
public interface VersionedBytesStore extends KeyValueStore<Bytes, byte[]>, TimestampedBytesStore {

    /**
     * The analog of {@link VersionedKeyValueStore#put(Object, Object, long)}.
     */
    long put(Bytes key, byte[] value, long timestamp);

    /**
     * The analog of {@link VersionedKeyValueStore#get(Object, long)}.
     */
    byte[] get(Bytes key, long asOfTimestamp);

    /**
     * The analog of {@link VersionedKeyValueStore#delete(Object, long)}.
     */
    byte[] delete(Bytes key, long timestamp);
}
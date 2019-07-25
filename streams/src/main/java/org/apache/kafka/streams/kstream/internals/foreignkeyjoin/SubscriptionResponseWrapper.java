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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.errors.UnsupportedVersionException;

public class SubscriptionResponseWrapper<FV> {
    final public static byte CURRENT_VERSION = 0x00;
    //Max version is limited by how many bytes we have available in the Serde.
    //Note: Do not change this such that the value goes below CURRENT_VERSION.
    final private static byte MAXIMUM_VERSION_INCLUSIVE = (byte) (2 ^ SubscriptionResponseWrapperSerde.VERSION_BITS);

    final private long[] originalValueHash;
    final private FV foreignValue;
    final private byte version;

    public SubscriptionResponseWrapper(final long[] originalValueHash, final FV foreignValue) {
        this(originalValueHash, foreignValue, CURRENT_VERSION);
    }

    public SubscriptionResponseWrapper(final long[] originalValueHash, final FV foreignValue, final byte version) {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedVersionException("SubscriptionWrapper does not support version " + version);
        }
        this.originalValueHash = originalValueHash;
        this.foreignValue = foreignValue;
        this.version = version;
    }

    public long[] getOriginalValueHash() {
        return originalValueHash;
    }

    public FV getForeignValue() {
        return foreignValue;
    }

    public byte getVersion() {
        return version;
    }
}

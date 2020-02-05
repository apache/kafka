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

import java.util.Arrays;

public class SubscriptionResponseWrapper<FV> {
    final static byte CURRENT_VERSION = 0x00;
    private final long[] originalValueHash;
    private final FV foreignValue;
    private final byte version;

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

    @Override
    public String toString() {
        return "SubscriptionResponseWrapper{" +
            "version=" + version +
            ", foreignValue=" + foreignValue +
            ", originalValueHash=" + Arrays.toString(originalValueHash) +
            '}';
    }
}

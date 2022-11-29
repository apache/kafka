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
package org.apache.kafka.metadata.util;

import java.util.Collection;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;

/**
 * Reason for generating a snapshot.
 */
public final class SnapshotReason {
    static public final SnapshotReason UNKNOWN = new SnapshotReason("unknown reason");

    static public SnapshotReason maxBytesExceeded(long bytes, long maxBytes) {
        return new SnapshotReason(String.format("%s bytes exceeded the maximum bytes of %s", bytes, maxBytes));
    }

    static public SnapshotReason maxIntervalExceeded(long interval, long maxInterval) {
        return new SnapshotReason(
            String.format("%s ms exceeded the maximum snapshot interval of %s ms", interval, maxInterval)
        );
    }

    static public SnapshotReason metadataVersionChanged(MetadataVersion metadataVersion) {
        return new SnapshotReason(String.format("metadata version was changed to %s", metadataVersion));
    }

    /**
     * Converts a collection of reasons into a string.
     */
    static public String stringFromReasons(Collection<SnapshotReason> reasons) {
        return Utils.join(reasons, ", ");
    }

    private final String reason;

    private SnapshotReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return reason;
    }
}

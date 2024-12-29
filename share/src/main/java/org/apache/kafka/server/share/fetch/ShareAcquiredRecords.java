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

package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;

import java.util.Collections;
import java.util.List;

/**
 * The ShareAcquiredRecords class is used to send the acquired records and associated metadata.
 */
public class ShareAcquiredRecords {

    private static final ShareAcquiredRecords EMPTY_SHARE_ACQUIRED_RECORDS = new ShareAcquiredRecords();

    /**
     * The list of acquired records.
     */
    private final List<AcquiredRecords> acquiredRecords;
    /**
      * The number of offsets acquired. The acquired records has a first and last offset, and the count
      * is the actual number of offsets acquired.
     */
    private final int count;

    public ShareAcquiredRecords(
        List<AcquiredRecords> acquiredRecords,
        int count
    ) {
        this.acquiredRecords = acquiredRecords;
        this.count = count;
    }

    private ShareAcquiredRecords() {
        this.acquiredRecords = Collections.emptyList();
        this.count = 0;
    }

    public List<AcquiredRecords> acquiredRecords() {
        return acquiredRecords;
    }

    public int count() {
        return count;
    }

    public static ShareAcquiredRecords empty() {
        return EMPTY_SHARE_ACQUIRED_RECORDS;
    }

    public static ShareAcquiredRecords fromAcquiredRecords(AcquiredRecords acquiredRecords) {
        return new ShareAcquiredRecords(
            List.of(acquiredRecords), (int) (acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1)
        );
    }
}

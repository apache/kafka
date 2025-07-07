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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.util.List;
import java.util.Optional;

public class FetchDataInfo {
    public final LogOffsetMetadata fetchOffsetMetadata;
    public final Records records;
    public final boolean firstEntryIncomplete;
    public final Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions;
    public final Optional<RemoteStorageFetchInfo> delayedRemoteStorageFetch;

    public FetchDataInfo(LogOffsetMetadata fetchOffsetMetadata,
                         Records records) {
        this(fetchOffsetMetadata, records, false, Optional.empty());
    }

    public FetchDataInfo(LogOffsetMetadata fetchOffsetMetadata,
                         Records records,
                         boolean firstEntryIncomplete,
                         Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions) {
        this(fetchOffsetMetadata, records, firstEntryIncomplete, abortedTransactions, Optional.empty());
    }

    public FetchDataInfo(LogOffsetMetadata fetchOffsetMetadata,
                         Records records,
                         boolean firstEntryIncomplete,
                         Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions,
                         Optional<RemoteStorageFetchInfo> delayedRemoteStorageFetch) {
        this.fetchOffsetMetadata = fetchOffsetMetadata;
        this.records = records;
        this.firstEntryIncomplete = firstEntryIncomplete;
        this.abortedTransactions = abortedTransactions;
        this.delayedRemoteStorageFetch = delayedRemoteStorageFetch;
    }

    public static FetchDataInfo empty(long fetchOffset) {
        return new FetchDataInfo(new LogOffsetMetadata(fetchOffset), MemoryRecords.EMPTY);
    }
}

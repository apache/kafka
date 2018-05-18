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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.WriteableRecords;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Describes a Fetch Response to be sent over the wire. This implementation exposes serialization-only primitives for the
 * partition data and underlying records. See {@link FetchResponse} for a more generic implementation that exposes
 * record reading and processing methods for the underlying partition data, in addition to serialization primitives.
 */
public class WriteableFetchResponse<T extends WriteableFetchResponse.PartitionData> extends FetchResponse<T> {
    public WriteableFetchResponse(Errors error,
                                  LinkedHashMap<TopicPartition, T> responseData,
                                  int throttleTimeMs,
                                  int sessionId) {
        super(error, responseData, throttleTimeMs, sessionId);
    }

    public static class PartitionData extends FetchResponse.PartitionData {
        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             List<AbortedTransaction> abortedTransactions,
                             WriteableRecords records) {
            super(error, highWatermark, lastStableOffset, logStartOffset, abortedTransactions, records);
        }

        @Override
        public WriteableRecords records() {
            return (WriteableRecords) super.records();
        }
    }
}

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

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Optional;

public final class RequestUtils {

    private RequestUtils() {}

    public static Optional<Integer> getLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
            Optional.empty() : Optional.of(leaderEpoch);
    }

    // visible for testing
    public static boolean hasIdempotentRecords(ProduceRequest request) {
        return flags(request).getKey();
    }

    // visible for testing
    public static boolean hasTransactionalRecords(ProduceRequest request) {
        return flags(request).getValue();
    }

    /**
     * Get both hasIdempotentRecords flag and hasTransactionalRecords flag from produce request.
     * Noted that we find all flags at once to avoid duplicate loop and record batch construction.
     * @return first flag is "hasIdempotentRecords" and another is "hasTransactionalRecords"
     */
    public static AbstractMap.SimpleEntry<Boolean, Boolean> flags(ProduceRequest request) {
        boolean hasIdempotentRecords = false;
        boolean hasTransactionalRecords = false;
        for (ProduceRequestData.TopicProduceData tpd : request.data().topicData()) {
            for (ProduceRequestData.PartitionProduceData ppd : tpd.partitionData()) {
                BaseRecords records = ppd.records();
                if (records instanceof Records) {
                    Iterator<? extends RecordBatch> iterator = ((Records) records).batches().iterator();
                    if (iterator.hasNext()) {
                        RecordBatch batch = iterator.next();
                        hasIdempotentRecords = hasIdempotentRecords || batch.hasProducerId();
                        hasTransactionalRecords = hasTransactionalRecords || batch.isTransactional();
                    }
                }
                // return early
                if (hasIdempotentRecords && hasTransactionalRecords)
                    return new AbstractMap.SimpleEntry<>(true, true);
            }
        }
        return new AbstractMap.SimpleEntry<>(hasIdempotentRecords, hasTransactionalRecords);
    }

    public static ByteBuffer serialize(
        Message header,
        short headerVersion,
        Message apiMessage,
        short apiVersion
    ) {
        ObjectSerializationCache cache = new ObjectSerializationCache();

        int headerSize = header.size(cache, headerVersion);
        int messageSize = apiMessage.size(cache, apiVersion);
        ByteBufferAccessor writable = new ByteBufferAccessor(ByteBuffer.allocate(headerSize + messageSize));

        header.write(writable, cache, headerVersion);
        apiMessage.write(writable, cache, apiVersion);

        writable.flip();
        return writable.buffer();
    }
}

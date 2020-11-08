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

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;

public final class RequestUtils {

    private RequestUtils() {}

    static void setLeaderEpochIfExists(Struct struct, Field.Int32 leaderEpochField, Optional<Integer> leaderEpoch) {
        struct.setIfExists(leaderEpochField, leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH));
    }

    static Optional<Integer> getLeaderEpoch(Struct struct, Field.Int32 leaderEpochField) {
        int leaderEpoch = struct.getOrElse(leaderEpochField, RecordBatch.NO_PARTITION_LEADER_EPOCH);
        return getLeaderEpoch(leaderEpoch);
    }

    static Optional<Integer> getLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
            Optional.empty() : Optional.of(leaderEpoch);
    }

    public static ByteBuffer serialize(Struct headerStruct, Struct bodyStruct) {
        ByteBuffer buffer = ByteBuffer.allocate(headerStruct.sizeOf() + bodyStruct.sizeOf());
        headerStruct.writeTo(buffer);
        bodyStruct.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }

    public static boolean hasIdempotentRecords(ProduceRequest request) {
        return anyMatch(request, RecordBatch::hasProducerId);
    }

    public static boolean hasTransactionalRecords(ProduceRequest request) {
        return anyMatch(request, RecordBatch::isTransactional);
    }

    private static boolean anyMatch(ProduceRequest request, Predicate<RecordBatch> predicate) {
        return request.dataOrException().topicData()
                .stream()
                .anyMatch(topicProduceData -> topicProduceData.partitionData()
                    .stream()
                    .anyMatch(partitionProduceData -> {
                        BaseRecords records = partitionProduceData.records();
                        if (records instanceof Records) {
                            Iterator<? extends RecordBatch> iterator = ((Records) records).batches().iterator();
                            return iterator.hasNext() && predicate.test(iterator.next());
                        } else return false;
                    }));
    }


}
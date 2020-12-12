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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ProduceResponse.INVALID_OFFSET;

public class ProduceRequest extends AbstractRequest {

    public static Builder forMagic(byte magic, ProduceRequestData data) {
        // Message format upgrades correspond with a bump in the produce request version. Older
        // message format versions are generally not supported by the produce request versions
        // following the bump.

        final short minVersion;
        final short maxVersion;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            minVersion = 2;
            maxVersion = 2;
        } else {
            minVersion = 3;
            maxVersion = ApiKeys.PRODUCE.latestVersion();
        }
        return new Builder(minVersion, maxVersion, data);
    }

    public static Builder forCurrentMagic(ProduceRequestData data) {
        return forMagic(RecordBatch.CURRENT_MAGIC_VALUE, data);
    }

    public static class Builder extends AbstractRequest.Builder<ProduceRequest> {
        private final ProduceRequestData data;

        public Builder(short minVersion,
                       short maxVersion,
                       ProduceRequestData data) {
            super(ApiKeys.PRODUCE, minVersion, maxVersion);
            this.data = data;
        }

        @Override
        public ProduceRequest build(short version) {
            return build(version, true);
        }

        // Visible for testing only
        public ProduceRequest buildUnsafe(short version) {
            return build(version, false);
        }

        private ProduceRequest build(short version, boolean validate) {
            if (validate) {
                // Validate the given records first
                data.topicData().forEach(tpd ->
                        tpd.partitionData().forEach(partitionProduceData ->
                                ProduceRequest.validateRecords(version, partitionProduceData.records())));
            }
            return new ProduceRequest(data, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ProduceRequest")
                    .append(", acks=").append(data.acks())
                    .append(", timeout=").append(data.timeoutMs())
                    .append(", partitionRecords=(").append(data.topicData().stream().flatMap(d -> d.partitionData().stream()).collect(Collectors.toList()))
                    .append("), transactionalId='").append(data.transactionalId() != null ? data.transactionalId() : "")
                    .append("'");
            return bld.toString();
        }
    }

    /**
     * We have to copy acks, timeout, transactionalId and partitionSizes from data since data maybe reset to eliminate
     * the reference to ByteBuffer but those metadata are still useful.
     */
    private final short acks;
    private final int timeout;
    private final String transactionalId;
    // This is set to null by `clearPartitionRecords` to prevent unnecessary memory retention when a produce request is
    // put in the purgatory (due to client throttling, it can take a while before the response is sent).
    // Care should be taken in methods that use this field.
    private volatile ProduceRequestData data;
    // the partitionSizes is lazily initialized since it is used by server-side in production.
    private volatile Map<TopicPartition, Integer> partitionSizes;

    public ProduceRequest(ProduceRequestData produceRequestData, short version) {
        super(ApiKeys.PRODUCE, version);
        this.data = produceRequestData;
        this.acks = data.acks();
        this.timeout = data.timeoutMs();
        this.transactionalId = data.transactionalId();
    }

    // visible for testing
    Map<TopicPartition, Integer> partitionSizes() {
        if (partitionSizes == null) {
            // this method may be called by different thread (see the comment on data)
            synchronized (this) {
                if (partitionSizes == null) {
                    partitionSizes = new HashMap<>();
                    data.topicData().forEach(topicData ->
                        topicData.partitionData().forEach(partitionData ->
                            partitionSizes.compute(new TopicPartition(topicData.name(), partitionData.index()),
                                (ignored, previousValue) ->
                                    partitionData.records().sizeInBytes() + (previousValue == null ? 0 : previousValue))
                        )
                    );
                }
            }
        }
        return partitionSizes;
    }

    /**
     * @return data or IllegalStateException if the data is removed (to prevent unnecessary memory retention).
     */
    @Override
    public ProduceRequestData data() {
        // Store it in a local variable to protect against concurrent updates
        ProduceRequestData tmp = data;
        if (tmp == null)
            throw new IllegalStateException("The partition records are no longer available because clearPartitionRecords() has been invoked.");
        return tmp;
    }

    @Override
    public String toString(boolean verbose) {
        // Use the same format as `Struct.toString()`
        StringBuilder bld = new StringBuilder();
        bld.append("{acks=").append(acks)
                .append(",timeout=").append(timeout);

        if (verbose)
            bld.append(",partitionSizes=").append(Utils.mkString(partitionSizes(), "[", "]", "=", ","));
        else
            bld.append(",numPartitions=").append(partitionSizes().size());

        bld.append("}");
        return bld.toString();
    }

    @Override
    public ProduceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        /* In case the producer doesn't actually want any response */
        if (acks == 0) return null;
        ApiError apiError = ApiError.fromThrowable(e);
        ProduceResponseData data = new ProduceResponseData().setThrottleTimeMs(throttleTimeMs);
        partitionSizes().forEach((tp, ignored) -> {
            ProduceResponseData.TopicProduceResponse tpr = data.responses().find(tp.topic());
            if (tpr == null) {
                tpr = new ProduceResponseData.TopicProduceResponse().setName(tp.topic());
                data.responses().add(tpr);
            }
            tpr.partitionResponses().add(new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(tp.partition())
                    .setRecordErrors(Collections.emptyList())
                    .setBaseOffset(INVALID_OFFSET)
                    .setLogAppendTimeMs(RecordBatch.NO_TIMESTAMP)
                    .setLogStartOffset(INVALID_OFFSET)
                    .setErrorMessage(apiError.message())
                    .setErrorCode(apiError.error().code()));
        });
        return new ProduceResponse(data);
    }

    @Override
    public Map<Errors, Integer> errorCounts(Throwable e) {
        Errors error = Errors.forException(e);
        return Collections.singletonMap(error, partitionSizes().size());
    }

    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public void clearPartitionRecords() {
        // lazily initialize partitionSizes.
        partitionSizes();
        data = null;
    }

    public static void validateRecords(short version, BaseRecords baseRecords) {
        if (version >= 3) {
            if (baseRecords instanceof Records) {
                Records records = (Records) baseRecords;
                Iterator<? extends RecordBatch> iterator = records.batches().iterator();
                if (!iterator.hasNext())
                    throw new InvalidRecordException("Produce requests with version " + version + " must have at least " +
                            "one record batch");

                RecordBatch entry = iterator.next();
                if (entry.magic() != RecordBatch.MAGIC_VALUE_V2)
                    throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to " +
                            "contain record batches with magic version 2");
                if (version < 7 && entry.compressionType() == CompressionType.ZSTD) {
                    throw new UnsupportedCompressionTypeException("Produce requests with version " + version + " are not allowed to " +
                            "use ZStandard compression");
                }

                if (iterator.hasNext())
                    throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to " +
                            "contain exactly one record batch");
            }
        }

        // Note that we do not do similar validation for older versions to ensure compatibility with
        // clients which send the wrong magic version in the wrong version of the produce request. The broker
        // did not do this validation before, so we maintain that behavior here.
    }

    public static ProduceRequest parse(ByteBuffer buffer, short version) {
        return new ProduceRequest(new ProduceRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static byte requiredMagicForVersion(short produceRequestVersion) {
        if (produceRequestVersion < ApiKeys.PRODUCE.oldestVersion() || produceRequestVersion > ApiKeys.PRODUCE.latestVersion())
            throw new IllegalArgumentException("Magic value to use for produce request version " +
                    produceRequestVersion + " is not known");

        switch (produceRequestVersion) {
            case 0:
            case 1:
                return RecordBatch.MAGIC_VALUE_V0;

            case 2:
                return RecordBatch.MAGIC_VALUE_V1;

            default:
                return RecordBatch.MAGIC_VALUE_V2;
        }
    }
}

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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ProduceResponse.INVALID_OFFSET;

public class ProduceRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ProduceRequest> {
        private final short acks;
        private final int timeout;
        private final Map<TopicPartition, MemoryRecords> partitionRecords;
        private final String transactionalId;

        public static Builder forCurrentMagic(short acks,
                                              int timeout,
                                              Map<TopicPartition, MemoryRecords> partitionRecords) {
            return forMagic(RecordBatch.CURRENT_MAGIC_VALUE, acks, timeout, partitionRecords, null);
        }

        public static Builder forMagic(byte magic,
                                       short acks,
                                       int timeout,
                                       Map<TopicPartition, MemoryRecords> partitionRecords,
                                       String transactionalId) {
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
            return new Builder(minVersion, maxVersion, acks, timeout, partitionRecords, transactionalId);
        }

        public Builder(short minVersion,
                       short maxVersion,
                       short acks,
                       int timeout,
                       Map<TopicPartition, MemoryRecords> partitionRecords,
                       String transactionalId) {
            super(ApiKeys.PRODUCE, minVersion, maxVersion);
            this.acks = acks;
            this.timeout = timeout;
            this.partitionRecords = partitionRecords;
            this.transactionalId = transactionalId;
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
                for (MemoryRecords records : partitionRecords.values()) {
                    ProduceRequest.validateRecords(version, records);
                }
            }

            List<ProduceRequestData.TopicProduceData> tpd = partitionRecords
                .entrySet()
                .stream()
                .collect(Collectors.groupingBy(e -> e.getKey().topic()))
                .entrySet()
                .stream()
                .map(e -> new ProduceRequestData.TopicProduceData()
                    .setTopic(e.getKey())
                    .setData(e.getValue().stream()
                        .map(tpAndRecord -> new ProduceRequestData.PartitionProduceData()
                            .setPartition(tpAndRecord.getKey().partition())
                            .setRecordSet(tpAndRecord.getValue()))
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());

            return new ProduceRequest(new ProduceRequestData()
                .setAcks(acks)
                .setTimeout(timeout)
                .setTransactionalId(transactionalId)
                .setTopicData(tpd), version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ProduceRequest")
                    .append(", acks=").append(acks)
                    .append(", timeout=").append(timeout)
                    .append(", partitionRecords=(").append(partitionRecords)
                    .append("), transactionalId='").append(transactionalId != null ? transactionalId : "")
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
    // visible for testing
    final Map<TopicPartition, Integer> partitionSizes;
    // This is set to null by `clearPartitionRecords` to prevent unnecessary memory retention when a produce request is
    // put in the purgatory (due to client throttling, it can take a while before the response is sent).
    // Care should be taken in methods that use this field.
    private volatile ProduceRequestData data;

    public ProduceRequest(ProduceRequestData produceRequestData, short version) {
        super(ApiKeys.PRODUCE, version);
        this.data = produceRequestData;
        this.acks = data.acks();
        this.timeout = data.timeout();
        this.transactionalId = data.transactionalId();
        this.partitionSizes = data.topicData()
            .stream()
            .flatMap(e -> e.data()
                .stream()
                .map(p -> new AbstractMap.SimpleEntry<>(new TopicPartition(e.topic(), p.partition()), p.recordSet().sizeInBytes())))
            .collect(Collectors.groupingBy(AbstractMap.SimpleEntry::getKey, Collectors.summingInt(AbstractMap.SimpleEntry::getValue)));
    }

    /**
     * @return data or IllegalStateException if the data is removed (to prevent unnecessary memory retention).
     */
    public ProduceRequestData dataOrException() {
        // Store it in a local variable to protect against concurrent updates
        ProduceRequestData tmp = data;
        if (tmp == null)
            throw new IllegalStateException("The partition records are no longer available because clearPartitionRecords() has been invoked.");
        return tmp;
    }

    /**
     * Visible for testing.
     */
    @Override
    public Struct toStruct() {
        return dataOrException().toStruct(version());
    }

    @Override
    public String toString(boolean verbose) {
        // Use the same format as `Struct.toString()`
        StringBuilder bld = new StringBuilder();
        bld.append("{acks=").append(acks)
                .append(",timeout=").append(timeout);

        if (verbose)
            bld.append(",partitionSizes=").append(Utils.mkString(partitionSizes, "[", "]", "=", ","));
        else
            bld.append(",numPartitions=").append(partitionSizes.size());

        bld.append("}");
        return bld.toString();
    }

    @Override
    public ProduceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        /* In case the producer doesn't actually want any response */
        if (acks == 0) return null;
        Errors error = Errors.forException(e);
        return new ProduceResponse(new ProduceResponseData()
            .setResponses(partitions().stream().collect(Collectors.groupingBy(TopicPartition::topic)).entrySet()
                .stream()
                .map(entry -> new ProduceResponseData.TopicProduceResponse()
                    .setPartitionResponses(entry.getValue().stream().map(p -> new ProduceResponseData.PartitionProduceResponse()
                        .setPartition(p.partition())
                        .setRecordErrors(Collections.emptyList())
                        .setBaseOffset(INVALID_OFFSET)
                        .setLogAppendTime(RecordBatch.NO_TIMESTAMP)
                        .setLogStartOffset(INVALID_OFFSET)
                        .setErrorMessage(e.getMessage())
                        .setErrorCode(error.code()))
                        .collect(Collectors.toList()))
                    .setTopic(entry.getKey()))
                .collect(Collectors.toList()))
            .setThrottleTimeMs(throttleTimeMs));
    }

    @Override
    public Map<Errors, Integer> errorCounts(Throwable e) {
        Errors error = Errors.forException(e);
        return Collections.singletonMap(error, partitions().size());
    }

    private Collection<TopicPartition> partitions() {
        return partitionSizes.keySet();
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
        data = null;
    }

    public static void validateRecords(short version, Records records) {
        if (version >= 3) {
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

        // Note that we do not do similar validation for older versions to ensure compatibility with
        // clients which send the wrong magic version in the wrong version of the produce request. The broker
        // did not do this validation before, so we maintain that behavior here.
    }

    public static ProduceRequest parse(ByteBuffer buffer, short version) {
        return new ProduceRequest(new ProduceRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static byte requiredMagicForVersion(short produceRequestVersion) {
        switch (produceRequestVersion) {
            case 0:
            case 1:
                return RecordBatch.MAGIC_VALUE_V0;

            case 2:
                return RecordBatch.MAGIC_VALUE_V1;

            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
                return RecordBatch.MAGIC_VALUE_V2;

            default:
                // raise an exception if the version has not been explicitly added to this method.
                // this ensures that we cannot accidentally use the wrong magic value if we forget
                // to update this method on a bump to the produce request version.
                throw new IllegalArgumentException("Magic value to use for produce request version " +
                        produceRequestVersion + " is not known");
        }
    }

}

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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.HOST;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

/**
 * Possible error codes:
 *
 * - {@link Errors#UNSUPPORTED_FOR_MESSAGE_FORMAT} If the message format does not support lookup by timestamp
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have DESCRIBE access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker which is not a replica
 * - {@link Errors#NOT_LEADER_FOR_PARTITION} If the broker is not a leader and either the provided leader epoch
 *     matches the known leader epoch on the broker or is empty
 * - {@link Errors#FENCED_LEADER_EPOCH} If the epoch is lower than the broker's epoch
 * - {@link Errors#UNKNOWN_LEADER_EPOCH} If the epoch is larger than the broker's epoch
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} If the broker does not have metadata for a topic or partition
 * - {@link Errors#KAFKA_STORAGE_ERROR} If the log directory for one of the requested partitions is offline
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 */
public class RDMAConsumeAddressResponse extends AbstractResponse {
    public static final long UNKNOWN_ADDRESS = -1L;
    public static final long UNKNOWN_OFFSET = -1L;
    public static final int UNKNOWN_RKEY = -1;
    public static final int UNKNOWN_LENGTH = -1;


    private static final Field.Int32 PORT = new Field.Int32("port",
            "rdmaport");
    // top level fields
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("responses",
            "The listed offsets by topic");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partition_responses",
            "The listed offsets by partition");

    // partition level fields
    // This key is only used by ListOffsetResponse v0

    private static final Field.Int64 ADDRESS = new Field.Int64("address",
            "The address associated with the returned offset");
    private static final Field.Int64 BASEOFFSET = new Field.Int64("baseoffset",
            "The offset associated with the returned address");
    private static final Field.Int64 POSITION = new Field.Int64("position",
            "The address associated with the returned offset");

    private static final Field.Int32 RKEY = new Field.Int32("rkey",
            "The address associated with the returned offset");

    private static final Field.Bool IS_SEALED = new Field.Bool("IS_SEALED",
            "The address associated with the returned offset");

    private static final Field.Int64 SLOTADDRESS = new Field.Int64("slotAddress",
            "The address associated with the returned offset");
    private static final Field.Int32 SLOTRKEY = new Field.Int32("slotRkey",
            "The address associated with the returned offset");

    private static final Field.Int64 WATERMARK_POSITION = new Field.Int64("watermarkPosition",
            "The address associated with the returned offset");
    private static final Field.Int64 WATERMARK_OFFSET = new Field.Int64("watermarkOffset",
            "The address associated with the returned offset");
    private static final Field.Int64 LAST_STABLE_POSITION = new Field.Int64("laststableposition",
            "The address associated with the returned offset");
    private static final Field.Int64 LAST_STABLE_OFFSET = new Field.Int64("laststableoffset",
            "The address associated with the returned offset");
    private static final Field.Int64 WRITTEN_POSITION = new Field.Int64("writtenposition",
            "Length of the address");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            ERROR_CODE,
            ADDRESS,
            BASEOFFSET,
            POSITION,
            WATERMARK_POSITION,
            WATERMARK_OFFSET,
            LAST_STABLE_POSITION,
            LAST_STABLE_OFFSET,
            WRITTEN_POSITION,
            RKEY, IS_SEALED,
            SLOTADDRESS, SLOTRKEY);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema RDMA_ADDRESS_RESPONSE_V0 = new Schema(
            HOST, PORT, THROTTLE_TIME_MS,
            TOPICS_V0);



    public static Schema[] schemaVersions() {
        return new Schema[] {RDMA_ADDRESS_RESPONSE_V0};
    }

    public static final class PartitionData {
        public final Errors error;

        public final Long address;
        public final Long baseOffset;
        public final Long position;

        public final Long watermarkPosition;
        public final Long watermarkOffset;
        public final Long lastStablePosition;
        public final Long lastStableOffset;
        public final Long writtenPosition;

        public final Integer rkey;
        public final Boolean fileIsSealed;

        public final Long slotAddress;
        public final Integer slotRkey;

        /**
         * Constructor
         */
        public PartitionData(Errors error, long address, long baseOffset, long position, long watermarkPosition, long watermarkOffset,
                             long lastStablePosition, long lastStableOffset,
                             long writtenPosition,  int rkey, boolean fileIsSealed, long slotAddress, int slotRkey) {
            this.error = error;
            this.address = address;
            this.baseOffset = baseOffset;
            this.position = position;

            this.watermarkPosition = watermarkPosition;
            this.watermarkOffset = watermarkOffset;
            this.lastStablePosition = lastStablePosition;
            this.lastStableOffset = lastStableOffset;
            this.writtenPosition = writtenPosition;

            this.rkey = rkey;
            this.fileIsSealed = fileIsSealed;

            this.slotAddress = slotAddress;
            this.slotRkey = slotRkey;
        }
        public PartitionData(Errors error) {
            this.error = error;
            this.address = UNKNOWN_ADDRESS;
            this.baseOffset = UNKNOWN_OFFSET;
            this.position = UNKNOWN_ADDRESS;
            this.rkey = UNKNOWN_RKEY;
            this.watermarkPosition = UNKNOWN_ADDRESS;
            this.watermarkOffset = UNKNOWN_OFFSET;
            this.lastStablePosition = UNKNOWN_ADDRESS;
            this.lastStableOffset = UNKNOWN_OFFSET;
            this.writtenPosition = UNKNOWN_ADDRESS;
            this.fileIsSealed = false;
            this.slotAddress = UNKNOWN_ADDRESS;
            this.slotRkey = UNKNOWN_RKEY;
        }


        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("PartitionData(").
                    append("errorCode: ").append((int) error.code());

            bld.append(", address: ").append(address).
                    append(", baseOffset: ").append(baseOffset).
                    append(", rkey: ").append(rkey);

            bld.append(")");
            return bld.toString();
        }
    }


    private final String hostname;
    private final int rdmaport;
    private final int throttleTimeMs;
    private final Map<TopicPartition, PartitionData> responseData;

    /**
     * Constructor for all versions without throttle time
     */
    public RDMAConsumeAddressResponse(String hostname, int rdmaport, Map<TopicPartition, PartitionData> responseData) {
        this(hostname, rdmaport, DEFAULT_THROTTLE_TIME, responseData);
    }

    public RDMAConsumeAddressResponse(String hostname, int rdmaport, int throttleTimeMs, Map<TopicPartition, PartitionData> responseData) {
        this.hostname = hostname;
        this.rdmaport = rdmaport;
        this.throttleTimeMs = throttleTimeMs;
        this.responseData = responseData;
    }

    public RDMAConsumeAddressResponse(Struct struct) {
        this.hostname = struct.get(HOST);
        this.rdmaport = struct.get(PORT);
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        responseData = new HashMap<>();
        for (Object topicResponseObj : struct.get(TOPICS)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionResponse.get(ERROR_CODE));
                PartitionData partitionData;

                long address = partitionResponse.get(ADDRESS);

                long baseOffset = partitionResponse.get(BASEOFFSET);
                long position = partitionResponse.get(POSITION);

                long watermarkPosition = partitionResponse.get(WATERMARK_POSITION);
                long watermarkOffset = partitionResponse.get(WATERMARK_OFFSET);
                long lastStablePosition = partitionResponse.get(LAST_STABLE_POSITION);
                long lastStableOffset = partitionResponse.get(LAST_STABLE_OFFSET);

                long writtenPosition = partitionResponse.get(WRITTEN_POSITION);

                int rkey = partitionResponse.get(RKEY);
                boolean fileIsSealed = partitionResponse.get(IS_SEALED);
                long slotAddress = partitionResponse.get(SLOTADDRESS);
                int slotRkey = partitionResponse.get(SLOTRKEY);

                partitionData = new PartitionData(error, address, baseOffset, position, watermarkPosition, watermarkOffset,
                        lastStablePosition, lastStableOffset, writtenPosition, rkey, fileIsSealed, slotAddress, slotRkey);

                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public int getRdmaPort() {
        return this.rdmaport;
    }
    public String getHostName() {
        return this.hostname;
    }


    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (PartitionData response : responseData.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    public static RDMAConsumeAddressResponse parse(ByteBuffer buffer, short version) {
        return new RDMAConsumeAddressResponse(ApiKeys.CONSUMER_RDMA_REGISTER.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CONSUMER_RDMA_REGISTER.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(HOST, hostname);
        struct.set(PORT, rdmaport);

        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupPartitionDataByTopic(responseData);

        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(ERROR_CODE, offsetPartitionData.error.code());

                partitionData.set(ADDRESS, offsetPartitionData.address);
                partitionData.set(BASEOFFSET, offsetPartitionData.baseOffset);
                partitionData.set(POSITION, offsetPartitionData.position);


                partitionData.set(WATERMARK_POSITION, offsetPartitionData.watermarkPosition);
                partitionData.set(WATERMARK_OFFSET, offsetPartitionData.watermarkOffset);
                partitionData.set(LAST_STABLE_POSITION, offsetPartitionData.lastStablePosition);
                partitionData.set(LAST_STABLE_OFFSET, offsetPartitionData.lastStableOffset);
                partitionData.set(WRITTEN_POSITION, offsetPartitionData.writtenPosition);

                partitionData.set(RKEY, offsetPartitionData.rkey);
                partitionData.set(IS_SEALED, offsetPartitionData.fileIsSealed);

                partitionData.set(SLOTADDRESS, offsetPartitionData.slotAddress);
                partitionData.set(SLOTRKEY, offsetPartitionData.slotRkey);

                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());

        return struct;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("(type=RDMAConsumeAddressResponse")
            .append(", throttleTimeMs=").append(throttleTimeMs)
            .append(", responseData=").append(responseData)
            .append(")");
        return bld.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 3;
    }
}

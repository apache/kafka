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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class TestUtil {
    public static MemoryRecords records(
        long timestamp,
        Compression compression,
        String... records
    ) {
        return records(timestamp, compression, Arrays.stream(records).toList());
    }


    public static MemoryRecords records(
        long timestamp,
        String... records
    ) {
        return records(timestamp, Compression.NONE, Arrays.stream(records).toList());
    }

    public static MemoryRecords records(
        long timestamp,
        List<String> records
    ) {
        return records(timestamp, Compression.NONE, records);
    }

    public static MemoryRecords records(
        long timestamp,
        Compression compression,
        List<String> records
    ) {
        if (records.isEmpty())
            return MemoryRecords.EMPTY;

        List<SimpleRecord> simpleRecords = records.stream().map(record ->
            new SimpleRecord(timestamp, record.getBytes(Charset.defaultCharset()))
        ).toList();

        int sizeEstimate = AbstractRecords.estimateSizeInBytes(
            RecordVersion.current().value,
            compression.type(),
            simpleRecords
        );

        ByteBuffer buffer = ByteBuffer.allocate(sizeEstimate);

        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer,
            RecordVersion.current().value,
            compression,
            TimestampType.CREATE_TIME,
            0L,
            timestamp,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            0,
            false,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );

        simpleRecords.forEach(builder::append);

        return builder.build();
    }

    public static MemoryRecords transactionalRecords(
        long producerId,
        short producerEpoch,
        long timestamp,
        String... records
    ) {
        return transactionalRecords(
            producerId,
            producerEpoch,
            timestamp,
            Arrays.stream(records).toList()
        );
    }

    public static MemoryRecords transactionalRecords(
        long producerId,
        short producerEpoch,
        long timestamp,
        List<String> records
    ) {
        if (records.isEmpty())
            return MemoryRecords.EMPTY;

        List<SimpleRecord> simpleRecords = records.stream().map(record ->
            new SimpleRecord(timestamp, record.getBytes(Charset.defaultCharset()))
        ).toList();

        int sizeEstimate = AbstractRecords.estimateSizeInBytes(
            RecordVersion.current().value,
            CompressionType.NONE,
            simpleRecords
        );

        ByteBuffer buffer = ByteBuffer.allocate(sizeEstimate);

        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer,
            RecordVersion.current().value,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            0L,
            timestamp,
            producerId,
            producerEpoch,
            0,
            true,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );

        simpleRecords.forEach(builder::append);

        return builder.build();
    }

    public static MemoryRecords endTransactionMarker(
        long producerId,
        short producerEpoch,
        long timestamp,
        int coordinatorEpoch,
        ControlRecordType result
    ) {
        return MemoryRecords.withEndTransactionMarker(
            timestamp,
            producerId,
            producerEpoch,
            new EndTransactionMarker(
                result,
                coordinatorEpoch
            )
        );
    }

    public static RequestContext requestContext(
        ApiKeys apiKey
    ) {
        return new RequestContext(
            new RequestHeader(
                apiKey,
                apiKey.latestVersion(),
                "client",
                0
            ),
            "1",
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );
    }

    public static RequestContext requestContext(
            ApiKeys apiKey,
            Short version
    ) {
        return new RequestContext(
            new RequestHeader(
                apiKey,
                version,
                "client",
                0
            ),
            "1",
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );
    }
}

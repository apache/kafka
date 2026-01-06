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
package org.apache.kafka.storage.internals.log.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.kafka.common.record.Records.OFFSET_LENGTH;

public class KafkaEntryFormatter {
    private static final Logger log = LoggerFactory.getLogger(KafkaEntryFormatter.class);

    private static final Commands.ChecksumType CHECKSUM_TYPE = Commands.ChecksumType.None;

    public static ByteBuf encode(LogAppendInfo appendInfo, MemoryRecords records) {
        final long numMessages = appendInfo.numMessages();
        final ByteBuf payload = wrapByteBuffer(records.buffer());
        final MessageMetadata metadata = metadata(numMessages, records);
        return serializeMetadataAndPayload(CHECKSUM_TYPE, metadata, payload);
    }

    public static MemoryRecords decode(List<Entry> entries) {
        int totalSize = 0;
        int numberOfMessages = 0;
        // batched ByteBuf should be released after sending to client
        ByteBuf batchedByteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer(totalSize);
        for (Entry entry : entries) {
            try {
                final ByteBuf byteBuf = entry.getDataBuffer();
                long startOffset = MessageMetadataUtils.peekBaseOffsetFromEntry(entry);
                final MessageMetadata metadata = MessageMetadataUtils.parseMessageMetadata(byteBuf);
                numberOfMessages += metadata.hasNumMessagesInBatch() ? metadata.getNumMessagesInBatch() : 1;

                // not need down converted, batch magic retains the magic value written in production
                // Skip the first OFFSET_LENGTH bytes, which is the offset of the entry
                ByteBuf buf = byteBuf.slice(byteBuf.readerIndex(), byteBuf.readableBytes());
                totalSize += buf.readableBytes();
                // Write the start offset at the beginning of the entry, SEE: OFFSET_OFFSET
                batchedByteBuf.writeLong(startOffset);
                batchedByteBuf.writeBytes(buf.skipBytes(OFFSET_LENGTH));
                // Almost all exceptions in Kafka inherit from KafkaException and will be captured
                // and processed in KafkaApis. Here, whether it is down-conversion or the IOException
                // in builder.appendWithOffset in decodePulsarEntryToKafkaRecords will be caught by Kafka
                // and the KafkaException will be thrown. So we need to catch KafkaException here.
            } catch (KafkaException e) { // skip failed decode entry
                log.error("[{}:{}] Failed to decode entry. ", entry.getLedgerId(), entry.getEntryId(), e);
            } finally {
                entry.release();
            }
        }

        return MemoryRecords.readableRecords(batchedByteBuf.nioBuffer());
    }

    public static MessageMetadata metadata(long numberOfMessages, MemoryRecords records) {
        MessageMetadata metadata = new MessageMetadata();
        metadata.setNumMessagesInBatch((int) numberOfMessages);
        return metadata;
    }

    private static ByteBuf wrapByteBuffer(ByteBuffer payload) {
        ByteBuf copy = PulsarByteBufAllocator.DEFAULT.directBuffer(payload.remaining(), payload.remaining());
        copy.writeBytes(payload);
        return copy;
    }

    @VisibleForTesting
    public static ByteBuf serializeMetadataAndPayload(Commands.ChecksumType checksumType,
                                                      MessageMetadata msgMetadata, ByteBuf payload) {
        // / Wire format
        // [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
        int msgMetadataSize = msgMetadata.getSerializedSize();
        int magicAndChecksumLength = 0;
        int headerContentSize = magicAndChecksumLength + 4 + msgMetadataSize; // magicLength +
        // checksumSize + msgMetadataLength +
        // msgMetadataSize

        ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer(headerContentSize, headerContentSize);

        // Write metadata
        header.writeInt(msgMetadataSize);
        msgMetadata.writeTo(header);

        CompositeByteBuf headerAndPayload = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
        headerAndPayload.addComponent(true, header);
        headerAndPayload.addComponent(true, payload);
        return headerAndPayload;
    }
}

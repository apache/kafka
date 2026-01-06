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

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

import javax.annotation.Nullable;

public class MessageMetadataUtils {
    public static long getCurrentOffset(ManagedLedger managedLedger) {
        return ((ManagedLedgerInterceptorImpl) managedLedger.getManagedLedgerInterceptor()).getIndex();
    }

    public static long getHighWatermark(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static long getLogEndOffset(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static long getPublishTime(final ByteBuf byteBuf) {
        final int readerIndex = byteBuf.readerIndex();
        final MessageMetadata metadata = parseMessageMetadata(byteBuf);
        byteBuf.readerIndex(readerIndex);
        return metadata.getPublishTime();
    }

    public static long peekOffsetFromEntry(Entry entry) {
        return peekOffset(entry.getDataBuffer(), entry.getPosition());
    }

    public static long peekOffset(ByteBuf buf, @Nullable Position position) {
        final BrokerEntryMetadata brokerEntryMetadata =
                Commands.peekBrokerEntryMetadataIfExist(buf); // might throw IllegalArgumentException
        return brokerEntryMetadata.getIndex(); // might throw IllegalStateException
    }

    public static long peekBaseOffsetFromEntry(Entry entry) {
        return peekBaseOffset(entry.getDataBuffer(), entry.getPosition());
    }

    private static long peekBaseOffset(ByteBuf buf, @Nullable Position position) {
        MessageMetadata metadata = Commands.peekMessageMetadata(buf, null, 0);
        return peekBaseOffset(buf, position, metadata.getNumMessagesInBatch());
    }

    private static long peekBaseOffset(ByteBuf buf, @Nullable Position position, int numMessages) {
        return peekOffset(buf, position) - (numMessages - 1);
    }

    public static long peekBaseOffset(ByteBuf buf, int numMessages) {
        return peekBaseOffset(buf, null, numMessages);
    }

    public static MessageMetadata parseMessageMetadata(ByteBuf buf) {
        return Commands.parseMessageMetadata(buf);
    }
}

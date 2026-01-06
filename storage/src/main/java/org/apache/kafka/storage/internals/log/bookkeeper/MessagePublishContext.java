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
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class MessagePublishContext {
    private static final Logger log = LoggerFactory.getLogger(MessagePublishContext.class);

    private final CompletableFuture<Long> offsetFuture;
    private final int numberOfMessages;
    private long baseOffset = -1;
    private final BookkeeperLocalLog localLog;

    public MessagePublishContext(CompletableFuture<Long> offsetFuture, int numberOfMessages, BookkeeperLocalLog localLog) {
        this.offsetFuture = offsetFuture;
        this.numberOfMessages = numberOfMessages;
        this.localLog = localLog;
    }

    public void setMetadata(ByteBuf entry) {
        try {
            baseOffset = MessageMetadataUtils.peekBaseOffset(entry, numberOfMessages);
        } catch (Throwable t) {
            //  ignore
        }
    }



    public void complete(Exception ex, long ledgerId, long entryId) {
        if (ex != null) {
            offsetFuture.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception(ex.getMessage()));
            return;
        }

        if (baseOffset >= 0 && entryId == 0) {
            localLog.index.asyncAddLedgerBaseOffset(baseOffset, ledgerId)
                    .exceptionally(t -> {
                        log.warn("Failed to add ledger base offset {} to index, ledger id {}", baseOffset, ledgerId, t);
                        return null;
                    });
        }
        offsetFuture.complete(baseOffset);
    }
}

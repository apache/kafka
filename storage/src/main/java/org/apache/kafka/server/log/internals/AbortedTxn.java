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
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.message.FetchResponseData;

import java.nio.ByteBuffer;
import java.util.Objects;

public class AbortedTxn {
    static final int VERSION_OFFSET = 0;
    static final int VERSION_SIZE = 2;
    static final int PRODUCER_ID_OFFSET = VERSION_OFFSET + VERSION_SIZE;
    static final int PRODUCER_ID_SIZE = 8;
    static final int FIRST_OFFSET_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_SIZE;
    static final int FIRST_OFFSET_SIZE = 8;
    static final int LAST_OFFSET_OFFSET = FIRST_OFFSET_OFFSET + FIRST_OFFSET_SIZE;
    static final int LAST_OFFSET_SIZE = 8;
    static final int LAST_STABLE_OFFSET_OFFSET = LAST_OFFSET_OFFSET + LAST_OFFSET_SIZE;
    static final int LAST_STABLE_OFFSET_SIZE = 8;
    static final int TOTAL_SIZE = LAST_STABLE_OFFSET_OFFSET + LAST_STABLE_OFFSET_SIZE;

    public static final short CURRENT_VERSION = 0;

    final ByteBuffer buffer;

    AbortedTxn(ByteBuffer buffer) {
        Objects.requireNonNull(buffer);
        this.buffer = buffer;
    }

    public AbortedTxn(CompletedTxn completedTxn, long lastStableOffset) {
        this(completedTxn.producerId, completedTxn.firstOffset, completedTxn.lastOffset, lastStableOffset);
    }

    public AbortedTxn(long producerId, long firstOffset, long lastOffset, long lastStableOffset) {
        this(toByteBuffer(producerId, firstOffset, lastOffset, lastStableOffset));
    }

    private static ByteBuffer toByteBuffer(long producerId, long firstOffset, long lastOffset, long lastStableOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(TOTAL_SIZE);
        buffer.putShort(CURRENT_VERSION);
        buffer.putLong(producerId);
        buffer.putLong(firstOffset);
        buffer.putLong(lastOffset);
        buffer.putLong(lastStableOffset);
        buffer.flip();
        return buffer;
    }

    public short version() {
        return buffer.get(VERSION_OFFSET);
    }

    public long producerId() {
        return buffer.getLong(PRODUCER_ID_OFFSET);
    }

    public long firstOffset() {
        return buffer.getLong(FIRST_OFFSET_OFFSET);
    }

    public long lastOffset() {
        return buffer.getLong(LAST_OFFSET_OFFSET);
    }

    public long lastStableOffset() {
        return buffer.getLong(LAST_STABLE_OFFSET_OFFSET);
    }

    public FetchResponseData.AbortedTransaction asAbortedTransaction() {
        return new FetchResponseData.AbortedTransaction()
            .setProducerId(producerId())
            .setFirstOffset(firstOffset());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AbortedTxn that = (AbortedTxn) o;
        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    @Override
    public String toString() {
        return "AbortedTxn(version=" + version()
            + ", producerId=" + producerId()
            + ", firstOffset=" + firstOffset()
            + ", lastOffset=" + lastOffset()
            + ", lastStableOffset=" + lastStableOffset()
            + ")";
    }

}

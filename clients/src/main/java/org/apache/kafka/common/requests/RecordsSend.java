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

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransportLayers;
import org.apache.kafka.common.record.Records;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class RecordsSend implements Send {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    private final String destination;
    private final Records records;
    private int remaining;
    private boolean pending = false;

    public RecordsSend(String destination, Records records) {
        this.destination = destination;
        this.records = records;
        this.remaining = records.sizeInBytes();
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = 0;

        if (remaining > 0) {
            written = records.writeTo(channel, size() - remaining, remaining);
            if (written < 0)
                throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
            remaining -= written;
        }

        pending = TransportLayers.hasPendingWrites(channel);
        if (remaining <= 0 && pending)
            channel.write(EMPTY_BYTE_BUFFER);

        return written;
    }

    @Override
    public long size() {
        return records.sizeInBytes();
    }
}

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
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * A send backed by a file channel
 */
public class FileChannelSend implements Send {

    private final FileChannel fileChannel;
    private final long position;
    private final long maxBytesToWrite;
    private long remaining;
    private boolean pending = false;

    public FileChannelSend(FileChannel channel, long position, int maxBytesToWrite) {
        this.fileChannel = channel;
        this.position = position;
        this.maxBytesToWrite = maxBytesToWrite;
        this.remaining = maxBytesToWrite;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.maxBytesToWrite;
    }

    @Override
    public long writeTo(TransferableChannel writeChannel) throws IOException {
        long written = writeChannel.transferFrom(fileChannel, position + maxBytesToWrite - remaining, remaining);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        remaining -= written;
        pending = writeChannel.hasPendingWrites();
        return written;
    }

    public long remaining() {
        return remaining;
    }

    @Override
    public String toString() {
        return "FileChannelSend(" +
                ", size=" + size() +
                ", remaining=" + remaining +
                ", pending=" + pending +
                ')';
    }

}

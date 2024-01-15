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
package org.apache.kafka.common.record;

import org.apache.kafka.common.network.TransferableChannel;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Represents a file record set which is not necessarily offset-aligned
 */
public class UnalignedFileRecords implements UnalignedRecords {

    private final FileChannel channel;
    private final long position;
    private final int size;

    public UnalignedFileRecords(FileChannel channel, long position, int size) {
        this.channel = channel;
        this.position = position;
        this.size = size;
    }

    @Override
    public int sizeInBytes() {
        return size;
    }

    @Override
    public int writeTo(TransferableChannel destChannel, int previouslyWritten, int remaining) throws IOException {
        long position = this.position + previouslyWritten;
        int count = Math.min(remaining, sizeInBytes() - previouslyWritten);
        // safe to cast to int since `count` is an int
        return (int) destChannel.transferFrom(channel, position, count);
    }
}

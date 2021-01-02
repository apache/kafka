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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Base interface for accessing raft snapshot which could be file region or an in-memory buffers.
 */
public interface BaseRegion {

    /**
     * @return The size of the region.
     */
    int sizeInBytes();

    /**
     * Write the region data to the dest channel using zero-copy, this is only used
     * when transfer data from raft leader snapshot to follower.
     *
     * @see FileRegion#writeTo(TransferableChannel, long, int)
     * @param channel the dest channel
     * @param offset the start offset of the src region
     * @param length the data length to write
     * @return the size of written data
     */
    long writeTo(TransferableChannel channel, long offset, int length) throws IOException;

    /**
     * Write the region data to file, this is only used when writing fetch snapshot
     * response data to follower file.
     *
     * @see MemoryRegion#writeTo(FileChannel)
     * @param channel the dest channel
     * @return the size of written data
     */
    long writeTo(FileChannel channel) throws IOException;

    /**
     * Get the buffer of the region, this is only used when parse region from network buffer.
     */
    ByteBuffer buffer() throws IOException;
}

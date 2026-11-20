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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

/**
 * Thrown when a file being transferred to a network channel cannot be read, as opposed to the network channel
 * failing to be written to. Failing to get the size of the file counts as failing to read it. It is thrown by
 * {@link TransferableChannel#transferFrom} if the channel {@link TransferableChannel#distinguishesFileReadFailures()},
 * and by {@link org.apache.kafka.common.record.internal.FileRecords#writeTo} if it cannot get the size of the file
 * before the transfer. A file closed concurrently, for instance by a deletion or a shutdown, is not a failed read: its
 * {@link ClosedChannelException} is thrown as it is, see {@link #ofReadFailure}.
 */
public class FileReadException extends IOException {
    private static final long serialVersionUID = 1L;

    private final long position;

    /**
     * @param position The absolute position in the file at which the failed read started, or, if the size of the file
     *                 could not be read, at which the transfer was to start
     * @param cause The failure of the read
     */
    public FileReadException(long position, IOException cause) {
        super("Failed to read the file at position " + position, cause);
        this.position = position;
    }

    /**
     * @return The absolute position in the file at which the failed read started, or, if the size of the file could
     *         not be read, at which the transfer was to start
     */
    public long position() {
        return position;
    }

    /**
     * Returns the exception to throw when a read of a file being transferred to a network channel fails: the failure
     * itself if it is a {@link ClosedChannelException}, since a file closed concurrently, for instance by a deletion or
     * a shutdown, says nothing about the medium; otherwise a FileReadException with the failure as its cause.
     *
     * @param position The absolute position in the file at which the failed read started, or, if the size of the file
     *                 could not be read, at which the transfer was to start
     * @param failure The failure of the read
     */
    public static IOException ofReadFailure(long position, IOException failure) {
        return failure instanceof ClosedChannelException ? failure : new FileReadException(position, failure);
    }
}

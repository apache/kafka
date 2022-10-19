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

package org.apache.kafka.image.writer;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.ApiMessageAndVersion;


/**
 * Writes out a metadata image.
 */
public interface ImageWriter extends AutoCloseable {
    /**
     * Write a record.
     *
     * @param version                       The version of the record to write out.
     *                                      For convenience, this is an int rather than a short.
     * @param message                       The message of the record to write out.
     *
     * @throws ImageWriterClosedException   If the writer has already been completed or closed.
     */
    default void write(int version, ApiMessage message) {
        write(new ApiMessageAndVersion(message, (short) version));
    }

    /**
     * Write a record.
     *
     * @param record                The versioned record to write out.
     *
     * @throws ImageWriterClosedException   If the writer has already been completed or closed.
     */
    void write(ApiMessageAndVersion record);

    /**
     * Close the image writer, dicarding all progress. Calling this function more than once has
     * no effect.
     */
    default void close() {
        close(false);
    }

   /**
    * Close the image writer. Calling this function more than once has no effect.
    *
    * @param complete               True if we should complete the image successfully.
    *                               False if we should discard all progress.
    */
    void close(boolean complete);
}

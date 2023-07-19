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

/**
 * Represents a record set which can be transferred to a channel
 * @see Records
 * @see UnalignedRecords
 */
public interface TransferableRecords extends BaseRecords {

    /**
     * Attempts to write the contents of this buffer to a channel.
     * @param channel The channel to write to
     * @param position The position in the buffer to write from
     * @param length The number of bytes to write
     * @return The number of bytes actually written
     * @throws IOException For any IO errors
     */
    int writeTo(TransferableChannel channel, int position, int length) throws IOException;
}

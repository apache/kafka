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

public class DefaultRecordsSend<T extends TransferableRecords> extends RecordsSend<T> {
    public DefaultRecordsSend(T records) {
        this(records, records.sizeInBytes());
    }

    public DefaultRecordsSend(T records, int maxBytesToWrite) {
        super(records, maxBytesToWrite);
    }

    @Override
    protected int writeTo(TransferableChannel channel, int previouslyWritten, int remaining) throws IOException {
        return records().writeTo(channel, previouslyWritten, remaining);
    }
}

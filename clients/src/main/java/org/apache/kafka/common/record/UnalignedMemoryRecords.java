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

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents a memory record set which is not necessarily offset-aligned
 */
public class UnalignedMemoryRecords implements UnalignedRecords {

    private final ByteBuffer buffer;

    private UnalignedMemoryRecords(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public int sizeInBytes() {
        return buffer.remaining();
    }

    public static UnalignedMemoryRecords readableRecords(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer should not be null");
        return new UnalignedMemoryRecords(buffer);
    }

}

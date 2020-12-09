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
package org.apache.kafka.common.protocol;

/**
 * Helper class which facilitates zero-copy network transmission. See {@link SendBuilder}.
 */
public class MessageSizeAccumulator {
    private int totalSize = 0;
    private int zeroCopySize = 0;

    /**
     * Get the total size of the message.
     *
     * @return total size in bytes
     */
    public int totalSize() {
        return totalSize;
    }

    /**
     * Size excluding zero copy fields as specified by {@link #zeroCopySize}. This is typically the size of the byte
     * buffer used to serialize messages.
     */
    public int sizeExcludingZeroCopy() {
        return totalSize - zeroCopySize;
    }

    /**
     * Get the total "zero-copy" size of the message. This is the summed
     * total of all fields which have either have a type of 'bytes' with
     * 'zeroCopy' enabled, or a type of 'records'
     *
     * @return total size of zero-copy data in the message
     */
    public int zeroCopySize() {
        return zeroCopySize;
    }

    public void addZeroCopyBytes(int size) {
        zeroCopySize += size;
        totalSize += size;
    }

    public void addBytes(int size) {
        totalSize += size;
    }

    public void add(MessageSizeAccumulator size) {
        this.totalSize += size.totalSize;
        this.zeroCopySize += size.zeroCopySize;
    }

}

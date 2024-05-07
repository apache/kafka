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

import org.apache.kafka.common.protocol.types.RawTaggedField;

import java.util.List;

/**
 * An object that can serialize itself.  The serialization protocol is versioned.
 * Messages also implement toString, equals, and hashCode.
 */
public interface Message {
    /**
     * Returns the lowest supported API key of this message, inclusive.
     */
    short lowestSupportedVersion();

    /**
     * Returns the highest supported API key of this message, inclusive.
     */
    short highestSupportedVersion();

    /**
     * Returns the number of bytes it would take to write out this message.
     *
     * @param cache         The serialization size cache to populate.
     * @param version       The version to use.
     *
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    default int size(ObjectSerializationCache cache, short version) {
        MessageSizeAccumulator size = new MessageSizeAccumulator();
        addSize(size, cache, version);
        return size.totalSize();
    }

    /**
     * Add the size of this message to an accumulator.
     *
     * @param size          The size accumulator to add to
     * @param cache         The serialization size cache to populate.
     * @param version       The version to use.
     */
    void addSize(MessageSizeAccumulator size, ObjectSerializationCache cache, short version);

    /**
     * Writes out this message to the given Writable.
     *
     * @param writable      The destination writable.
     * @param cache         The object serialization cache to use.  You must have
     *                      previously populated the size cache using #{Message#size()}.
     * @param version       The version to use.
     *
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    void write(Writable writable, ObjectSerializationCache cache, short version);

    /**
     * Reads this message from the given Readable.  This will overwrite all
     * relevant fields with information from the byte buffer.
     *
     * @param readable      The source readable.
     * @param version       The version to use.
     *
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    void read(Readable readable, short version);

    /**
     * Returns a list of tagged fields which this software can't understand.
     *
     * @return              The raw tagged fields.
     */
    List<RawTaggedField> unknownTaggedFields();

    /**
     * Make a deep copy of the message.
     *
     * @return              A copy of the message which does not share any mutable fields.
     */
    Message duplicate();
}

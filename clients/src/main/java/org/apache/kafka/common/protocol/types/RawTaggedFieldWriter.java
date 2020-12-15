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

package org.apache.kafka.common.protocol.types;

import org.apache.kafka.common.protocol.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * The RawTaggedFieldWriter is used by Message subclasses to serialize their
 * lists of raw tags.
 */
public class RawTaggedFieldWriter {
    private static final RawTaggedFieldWriter EMPTY_WRITER =
        new RawTaggedFieldWriter(new ArrayList<>(0));

    private final List<RawTaggedField> fields;
    private final ListIterator<RawTaggedField> iter;
    private int prevTag;

    public static RawTaggedFieldWriter forFields(List<RawTaggedField> fields) {
        if (fields == null) {
            return EMPTY_WRITER;
        }
        return new RawTaggedFieldWriter(fields);
    }

    private RawTaggedFieldWriter(List<RawTaggedField> fields) {
        this.fields = fields;
        this.iter = this.fields.listIterator();
        this.prevTag = -1;
    }

    public int numFields() {
        return fields.size();
    }

    public void writeRawTags(Writable writable, int nextDefinedTag) {
        while (iter.hasNext()) {
            RawTaggedField field = iter.next();
            int tag = field.tag();
            if (tag >= nextDefinedTag) {
                if (tag == nextDefinedTag) {
                    // We must not have a raw tag field that duplicates the tag of another field.
                    throw new RuntimeException("Attempted to use tag " + tag + " as an " +
                        "undefined tag.");
                }
                iter.previous();
                return;
            }
            if (tag <= prevTag) {
                // The raw tag field list must be sorted by tag, and there must not be
                // any duplicate tags.
                throw new RuntimeException("Invalid raw tag field list: tag " + tag +
                    " comes after tag " + prevTag + ", but is not higher than it.");
            }
            writable.writeUnsignedVarint(field.tag());
            writable.writeUnsignedVarint(field.data().length);
            writable.writeByteArray(field.data());
            prevTag = tag;
        }
    }
}

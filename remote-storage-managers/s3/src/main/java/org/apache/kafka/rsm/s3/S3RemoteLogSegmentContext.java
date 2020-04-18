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
package org.apache.kafka.rsm.s3;

import java.nio.ByteBuffer;

import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentContext;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

/**
 * {@link RemoteLogSegmentContext} for {@link S3RemoteStorageManager}.
 *
 * The storage schema supports versions. Currently, there is only one version, 0.
 *
 * The version 0 schema consists of:
 * <ul>
 *     <li>version;</li>
 *     <li>file name base offset (e.g. {@code 00000000000000000123}).</li>
 * </ul>
 */
public class S3RemoteLogSegmentContext implements RemoteLogSegmentContext {

    private static final short VERSION_0 = 0;

    public static final String VERSION_KEY_NAME = "version";
    public static final Schema VERSION_SCHEMA = new Schema(
        new Field(VERSION_KEY_NAME, Type.INT16)
    );

    public static final String BASE_OFFSET_KEY_NAME = "base_offset";
    public static final Schema SCHEMA_V0 = new Schema(
        new Field(BASE_OFFSET_KEY_NAME, Type.INT64)
    );
    private final long baseOffset;

    public S3RemoteLogSegmentContext(final long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public byte[] asBytes() {
        final Struct versionStruct = new Struct(VERSION_SCHEMA);
        versionStruct.set(VERSION_KEY_NAME, VERSION_0);

        final Struct payloadStruct = new Struct(SCHEMA_V0);
        payloadStruct.set(BASE_OFFSET_KEY_NAME, baseOffset);

        final ByteBuffer buf = ByteBuffer.allocate(
                VERSION_SCHEMA.sizeOf(versionStruct) + SCHEMA_V0.sizeOf(payloadStruct));
        versionStruct.writeTo(buf);
        payloadStruct.writeTo(buf);
        return buf.array();
    }

    public static S3RemoteLogSegmentContext fromBytes(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final Struct versionStruct = VERSION_SCHEMA.read(buffer);
        final Short version = versionStruct.getShort(VERSION_KEY_NAME);
        if (version == VERSION_0) {
            final Struct struct = SCHEMA_V0.read(buffer);
            return new S3RemoteLogSegmentContext(
                    struct.getLong(BASE_OFFSET_KEY_NAME)
            );
        } else {
            throw new IllegalArgumentException("Unknown schema version " + version);
        }
    }
}

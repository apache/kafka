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

import org.apache.kafka.common.compress.KafkaLZ4BlockInputStream;
import org.apache.kafka.common.compress.Lz4OutputStream;
import org.apache.kafka.common.compress.LZ4Config;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompressionTypeTest {

    public static final LZ4Config LZ4_CONFIG = CompressionConfig.lz4().build();

    @Test
    public void testLZ4FramingMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        Lz4OutputStream out = (Lz4OutputStream) LZ4_CONFIG.wrapForOutput(
                new ByteBufferOutputStream(buffer), RecordBatch.MAGIC_VALUE_V0);
        assertTrue(out.useBrokenFlagDescriptorChecksum());

        buffer.rewind();

        KafkaLZ4BlockInputStream in = (KafkaLZ4BlockInputStream) LZ4_CONFIG.wrapForInput(
                buffer, RecordBatch.MAGIC_VALUE_V0, BufferSupplier.NO_CACHING);
        assertTrue(in.ignoreFlagDescriptorChecksum());
    }

    @Test
    public void testLZ4FramingMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        Lz4OutputStream out = (Lz4OutputStream) LZ4_CONFIG.wrapForOutput(
                new ByteBufferOutputStream(buffer), RecordBatch.MAGIC_VALUE_V1);
        assertFalse(out.useBrokenFlagDescriptorChecksum());

        buffer.rewind();

        KafkaLZ4BlockInputStream in = (KafkaLZ4BlockInputStream) LZ4_CONFIG.wrapForInput(
                buffer, RecordBatch.MAGIC_VALUE_V1, BufferSupplier.create());
        assertFalse(in.ignoreFlagDescriptorChecksum());
    }
}

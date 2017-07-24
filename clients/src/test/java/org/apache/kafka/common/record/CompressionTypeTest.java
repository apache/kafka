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

import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompressionTypeTest {

    @Test
    public void testLZ4FramingMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        KafkaLZ4BlockOutputStream out = (KafkaLZ4BlockOutputStream) CompressionType.LZ4.wrapForOutput(
                new ByteBufferOutputStream(buffer), RecordBatch.MAGIC_VALUE_V0);
        assertTrue(out.useBrokenFlagDescriptorChecksum());

        buffer.rewind();

        KafkaLZ4BlockInputStream in = (KafkaLZ4BlockInputStream) CompressionType.LZ4.wrapForInput(
                buffer, RecordBatch.MAGIC_VALUE_V0, BufferSupplier.NO_CACHING);
        assertTrue(in.ignoreFlagDescriptorChecksum());
    }

    @Test
    public void testLZ4FramingMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        KafkaLZ4BlockOutputStream out = (KafkaLZ4BlockOutputStream) CompressionType.LZ4.wrapForOutput(
                new ByteBufferOutputStream(buffer), RecordBatch.MAGIC_VALUE_V1);
        assertFalse(out.useBrokenFlagDescriptorChecksum());

        buffer.rewind();

        KafkaLZ4BlockInputStream in = (KafkaLZ4BlockInputStream) CompressionType.LZ4.wrapForInput(
                buffer, RecordBatch.MAGIC_VALUE_V1, BufferSupplier.create());
        assertFalse(in.ignoreFlagDescriptorChecksum());
    }
}

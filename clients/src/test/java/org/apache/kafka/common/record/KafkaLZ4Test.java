/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import net.jpountz.xxhash.XXHashFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class KafkaLZ4Test {

    private final boolean useBrokenFlagDescriptorChecksum;
    private final boolean ignoreFlagDescriptorChecksum;
    private final byte[] payload;

    public KafkaLZ4Test(boolean useBrokenFlagDescriptorChecksum, boolean ignoreFlagDescriptorChecksum, byte[] payload) {
        this.useBrokenFlagDescriptorChecksum = useBrokenFlagDescriptorChecksum;
        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
        this.payload = payload;
    }

    @Parameters
    public static Collection<Object[]> data() {
        byte[] payload = new byte[1000];
        Arrays.fill(payload, (byte) 1);
        List<Object[]> values = new ArrayList<Object[]>();
        for (boolean broken : Arrays.asList(false, true))
            for (boolean ignore : Arrays.asList(false, true))
                values.add(new Object[] {broken, ignore, payload});
        return values;
    }

    @Test
    public void testKafkaLZ4() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        KafkaLZ4BlockOutputStream lz4 = new KafkaLZ4BlockOutputStream(output, this.useBrokenFlagDescriptorChecksum);
        lz4.write(this.payload, 0, this.payload.length);
        lz4.flush();
        byte[] compressed = output.toByteArray();

        // Check magic bytes stored as little-endian
        int offset = 0;
        assertEquals(compressed[offset++], 0x04);
        assertEquals(compressed[offset++], 0x22);
        assertEquals(compressed[offset++], 0x4D);
        assertEquals(compressed[offset++], 0x18);

        // Check flg descriptor
        byte flg = compressed[offset++];

        // 2-bit version must be 01
        int version = (flg >>> 6) & 3;
        assertEquals(version, 1);

        // Reserved bits should always be 0
        int reserved = flg & 3;
        assertEquals(reserved, 0);

        // Check block descriptor
        byte bd = compressed[offset++];

        // Block max-size
        int blockMaxSize = (bd >>> 4) & 7;
        // Only supported values are 4 (64KB), 5 (256KB), 6 (1MB), 7 (4MB)
        assertTrue(blockMaxSize >= 4);
        assertTrue(blockMaxSize <= 7);

        // Multiple reserved bit ranges in block descriptor
        reserved = bd & 15;
        assertEquals(reserved, 0);
        reserved = (bd >>> 7) & 1;
        assertEquals(reserved, 0);

        // If flg descriptor sets content size flag
        // there are 8 additional bytes before checksum
        boolean contentSize = ((flg >>> 3) & 1) != 0;
        if (contentSize)
            offset += 8;

        // Checksum applies to frame descriptor: flg, bd, and optional contentsize
        // so initial offset should be 4 (for magic bytes)
        int off = 4;
        int len = offset - 4;

        // Initial implementation of checksum incorrectly applied to full header
        // including magic bytes
        if (this.useBrokenFlagDescriptorChecksum) {
            off = 0;
            len = offset;
        }

        int hash = XXHashFactory.fastestInstance().hash32().hash(compressed, off, len, 0);

        byte hc = compressed[offset++];
        assertEquals(hc, (byte) ((hash >> 8) & 0xFF));

        ByteArrayInputStream input = new ByteArrayInputStream(compressed);
        try {
            KafkaLZ4BlockInputStream decompressed = new KafkaLZ4BlockInputStream(input, this.ignoreFlagDescriptorChecksum);
            byte[] testPayload = new byte[this.payload.length];
            int ret = decompressed.read(testPayload, 0, this.payload.length);
            assertEquals(ret, this.payload.length);
            assertArrayEquals(this.payload, testPayload);
        } catch (IOException e) {
            assertTrue(this.useBrokenFlagDescriptorChecksum && !this.ignoreFlagDescriptorChecksum);
        }
    }
}

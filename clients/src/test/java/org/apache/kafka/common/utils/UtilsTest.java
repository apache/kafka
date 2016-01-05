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
package org.apache.kafka.common.utils;

import java.util.Arrays;
import java.util.Collections;
import java.nio.ByteBuffer;

import org.junit.Test;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.common.utils.Utils.formatAddress;
import static org.junit.Assert.assertEquals;

public class UtilsTest {

    @Test
    public void testGetHost() {
        assertEquals("127.0.0.1", getHost("127.0.0.1:8000"));
        assertEquals("mydomain.com", getHost("PLAINTEXT://mydomain.com:8080"));
        assertEquals("MyDomain.com", getHost("PLAINTEXT://MyDomain.com:8080"));
        assertEquals("::1", getHost("[::1]:1234"));
        assertEquals("2001:db8:85a3:8d3:1319:8a2e:370:7348", getHost("PLAINTEXT://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678"));
        assertEquals("2001:DB8:85A3:8D3:1319:8A2E:370:7348", getHost("PLAINTEXT://[2001:DB8:85A3:8D3:1319:8A2E:370:7348]:5678"));
    }

    @Test
    public void testGetPort() {
        assertEquals(8000, getPort("127.0.0.1:8000").intValue());
        assertEquals(8080, getPort("mydomain.com:8080").intValue());
        assertEquals(8080, getPort("MyDomain.com:8080").intValue());
        assertEquals(1234, getPort("[::1]:1234").intValue());
        assertEquals(5678, getPort("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678").intValue());
        assertEquals(5678, getPort("[2001:DB8:85A3:8D3:1319:8A2E:370:7348]:5678").intValue());
    }

    @Test
    public void testFormatAddress() {
        assertEquals("127.0.0.1:8000", formatAddress("127.0.0.1", 8000));
        assertEquals("mydomain.com:8080", formatAddress("mydomain.com", 8080));
        assertEquals("[::1]:1234", formatAddress("::1", 1234));
        assertEquals("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678", formatAddress("2001:db8:85a3:8d3:1319:8a2e:370:7348", 5678));
    }

    @Test
    public void testJoin() {
        assertEquals("", Utils.join(Collections.emptyList(), ","));
        assertEquals("1", Utils.join(Arrays.asList("1"), ","));
        assertEquals("1,2,3", Utils.join(Arrays.asList(1, 2, 3), ","));
    }

    @Test
    public void testAbs() {
        assertEquals(0, Utils.abs(Integer.MIN_VALUE));
        assertEquals(10, Utils.abs(-10));
        assertEquals(10, Utils.abs(10));
        assertEquals(0, Utils.abs(0));
        assertEquals(1, Utils.abs(-1));
    }

    private void subTest(ByteBuffer buffer) {
        // The first byte should be 'A'
        assertEquals('A', (Utils.readBytes(buffer, 0, 1))[0]);

        // The offset is 2, so the first 2 bytes should be skipped.
        byte[] results = Utils.readBytes(buffer, 2, 3);
        assertEquals('y', results[0]);
        assertEquals(' ', results[1]);
        assertEquals('S', results[2]);
        assertEquals(3, results.length);

        // test readBytes without offset and length specified.
        results = Utils.readBytes(buffer);
        assertEquals('A', results[0]);
        assertEquals('t', results[buffer.limit() - 1]);
        assertEquals(buffer.limit(), results.length);
    }

    @Test
    public void testReadBytes() {
        byte[] myvar = "Any String you want".getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(myvar.length);
        buffer.put(myvar);
        buffer.rewind();

        this.subTest(buffer);

        // test readonly buffer, different path
        buffer = ByteBuffer.wrap(myvar).asReadOnlyBuffer();
        this.subTest(buffer);
    }

    @Test
    public void testMin() {
        assertEquals(1, Utils.min(1));
        assertEquals(1, Utils.min(1, 2, 3));
        assertEquals(1, Utils.min(2, 1, 3));
        assertEquals(1, Utils.min(2, 3, 1));
    }
}

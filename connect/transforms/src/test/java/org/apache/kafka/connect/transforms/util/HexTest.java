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

package org.apache.kafka.connect.transforms.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

public class HexTest {
    @Test
    public void testEncodeEmpty() {
        final byte[] bytes = new byte[0];
        assertEquals("", Hex.encode(bytes));
    }

    @Test
    public void testEncodeSingleByte() {
        final byte[] bytes = new byte[1];
        for (int i = 0; i < 256; i++) {
            final byte b = (byte) i;
            bytes[0] = b;
            assertEquals(String.format("%02x", b), Hex.encode(bytes));
        }
    }

    @Test
    public void testEncodeFromStrings() throws IOException, URISyntaxException {
        final URL resource = getClass().getClassLoader().getResource("blns.txt");
        final List<String> strings = Files.readAllLines(Paths.get(resource.toURI()));
        for (final String s : strings) {
            // Use the string as a byte array and hex-encode it.
            final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            final String encoded = Hex.encode(bytes);
            assertEquals(bytes.length * 2, encoded.length());

            // Decode the string back and compare to the original.
            final char[] encodedChars = encoded.toCharArray();
            final byte[] decodedBytes = new byte[bytes.length];
            for (int i = 0; i < encoded.length(); i += 2) {
                final String s1 = new String(encodedChars, i, 2);
                decodedBytes[i / 2] = (byte) Integer.parseInt(s1, 16);
            }
            assertEquals(new String(decodedBytes, StandardCharsets.UTF_8), s);
        }
    }
}

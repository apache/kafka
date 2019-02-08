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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public final class MessageUtilTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testSimpleUtf8Lengths() {
        validateUtf8Length("");
        validateUtf8Length("abc");
        validateUtf8Length("This is a test string.");
    }

    @Test
    public void testMultibyteUtf8Lengths() {
        validateUtf8Length("A\u00ea\u00f1\u00fcC");
        validateUtf8Length("\ud801\udc00");
        validateUtf8Length("M\u00fcO");
    }

    private void validateUtf8Length(String string) {
        byte[] arr = string.getBytes(StandardCharsets.UTF_8);
        assertEquals(arr.length, MessageUtil.serializedUtf8Length(string));
    }

    @Test
    public void testDeepToString() {
        assertEquals("[1, 2, 3]",
            MessageUtil.deepToString(Arrays.asList(1, 2, 3).iterator()));
        assertEquals("[foo]",
            MessageUtil.deepToString(Arrays.asList("foo").iterator()));
    }
}

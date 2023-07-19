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

package org.apache.kafka.message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(120)
public class CodeBufferTest {

    @Test
    public void testWrite() throws Exception {
        CodeBuffer buffer = new CodeBuffer();
        buffer.printf("public static void main(String[] args) throws Exception {%n");
        buffer.incrementIndent();
        buffer.printf("System.out.println(\"%s\");%n", "hello world");
        buffer.decrementIndent();
        buffer.printf("}%n");
        StringWriter stringWriter = new StringWriter();
        buffer.write(stringWriter);
        assertEquals(
            stringWriter.toString(),
            String.format("public static void main(String[] args) throws Exception {%n") +
            String.format("    System.out.println(\"hello world\");%n") +
            String.format("}%n"));
    }

    @Test
    public void testEquals() {
        CodeBuffer buffer1 = new CodeBuffer();
        CodeBuffer buffer2 = new CodeBuffer();
        assertEquals(buffer1, buffer2);
        buffer1.printf("hello world");
        assertNotEquals(buffer1, buffer2);
        buffer2.printf("hello world");
        assertEquals(buffer1, buffer2);
        buffer1.printf("foo, bar, and baz");
        buffer2.printf("foo, bar, and baz");
        assertEquals(buffer1, buffer2);
    }

    @Test
    public void testIndentMustBeNonNegative() {
        CodeBuffer buffer = new CodeBuffer();
        buffer.incrementIndent();
        buffer.decrementIndent();
        RuntimeException e = assertThrows(RuntimeException.class, buffer::decrementIndent);
        assertTrue(e.getMessage().contains("Indent < 0"));
    }
}

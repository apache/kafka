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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ControlRecordTypeTest {

    @Test
    public void testParseUnknownType() {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.putShort(ControlRecordType.CURRENT_CONTROL_RECORD_KEY_VERSION);
        buffer.putShort((short) 337);
        buffer.flip();
        ControlRecordType type = ControlRecordType.parse(buffer);
        assertEquals(ControlRecordType.UNKNOWN, type);
    }

    @Test
    public void testParseUnknownVersion() {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.putShort((short) 5);
        buffer.putShort(ControlRecordType.ABORT.type);
        buffer.putInt(23432); // some field added in version 5
        buffer.flip();
        ControlRecordType type = ControlRecordType.parse(buffer);
        assertEquals(ControlRecordType.ABORT, type);
    }

}

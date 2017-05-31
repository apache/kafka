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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class EndTransactionMarkerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownControlTypeNotAllowed() {
        new EndTransactionMarker(ControlRecordType.UNKNOWN, 24);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotDeserializeUnknownControlType() {
        EndTransactionMarker.deserializeValue(ControlRecordType.UNKNOWN, ByteBuffer.wrap(new byte[0]));
    }

    @Test(expected = InvalidRecordException.class)
    public void testIllegalNegativeVersion() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) -1);
        buffer.flip();
        EndTransactionMarker.deserializeValue(ControlRecordType.ABORT, buffer);
    }

    @Test(expected = InvalidRecordException.class)
    public void testNotEnoughBytes() {
        EndTransactionMarker.deserializeValue(ControlRecordType.COMMIT, ByteBuffer.wrap(new byte[0]));
    }

    @Test
    public void testSerde() {
        int coordinatorEpoch = 79;
        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        ByteBuffer buffer = marker.serializeValue();
        EndTransactionMarker deserialized = EndTransactionMarker.deserializeValue(ControlRecordType.COMMIT, buffer);
        assertEquals(coordinatorEpoch, deserialized.coordinatorEpoch());
    }

    @Test
    public void testDeserializeNewerVersion() {
        int coordinatorEpoch = 79;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putShort((short) 5);
        buffer.putInt(coordinatorEpoch);
        buffer.putShort((short) 0); // unexpected data
        buffer.flip();
        EndTransactionMarker deserialized = EndTransactionMarker.deserializeValue(ControlRecordType.COMMIT, buffer);
        assertEquals(coordinatorEpoch, deserialized.coordinatorEpoch());
    }
}

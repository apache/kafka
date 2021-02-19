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

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiRecordsSendTest {

    @Test
    public void testSendsFreedAfterWriting() throws IOException {
        int numChunks = 4;
        int chunkSize = 32;
        int totalSize = numChunks * chunkSize;

        Queue<Send> sends = new LinkedList<>();
        ByteBuffer[] chunks = new ByteBuffer[numChunks];

        for (int i = 0; i < numChunks; i++) {
            ByteBuffer buffer = ByteBuffer.wrap(TestUtils.randomBytes(chunkSize));
            chunks[i] = buffer;
            sends.add(new ByteBufferSend(buffer));
        }

        MultiRecordsSend send = new MultiRecordsSend(sends);
        assertEquals(totalSize, send.size());

        for (int i = 0; i < numChunks; i++) {
            assertEquals(numChunks - i, send.numResidentSends());
            NonOverflowingByteBufferChannel out = new NonOverflowingByteBufferChannel(chunkSize);
            send.writeTo(out);
            out.close();
            assertEquals(chunks[i], out.buffer());
        }

        assertEquals(0, send.numResidentSends());
        assertTrue(send.completed());
    }

    private static class NonOverflowingByteBufferChannel extends org.apache.kafka.common.requests.ByteBufferChannel {

        private NonOverflowingByteBufferChannel(long size) {
            super(size);
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            // Instead of overflowing, this channel refuses additional writes once the buffer is full,
            // which allows us to test the MultiRecordsSend behavior on a per-send basis.
            if (!buffer().hasRemaining())
                return 0;
            return super.write(srcs);
        }
    }

}

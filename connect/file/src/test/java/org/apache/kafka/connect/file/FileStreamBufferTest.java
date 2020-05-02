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
package org.apache.kafka.connect.file;

import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import static org.junit.Assert.assertEquals;

public class FileStreamBufferTest extends EasyMockSupport {

    /**
     *  Test buffer expand only once.
     */
    @Test
    public void testBufferExpandOnce() throws IOException, InterruptedException {
        InputStream stream = createMock(InputStream.class);
        BufferedReader reader = createMock(BufferedReader.class);
        FileStreamBuffer fileStreamBuffer = new FileStreamBuffer(stream, reader);

        FileStreamSourceTask task = new FileStreamSourceTask(fileStreamBuffer);
        EasyMock.expect(reader.ready()).andReturn(true).times(2);
        EasyMock.expect(reader.read((char[]) EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt())).andReturn(1024).times(2);
        stream.close();
        EasyMock.expectLastCall();
        replayAll();

        task.poll();
        task.poll();
        assertEquals(2048, fileStreamBuffer._bufferSize());
        task.stop();

        verifyAll();

    }

    /**
     * This test buffer that will expand from 1024->2048->4096->8192->16384 4 times.
     */
    @Test
    public void testBufferExpandMaxout() throws IOException, InterruptedException {
        FileStreamBuffer.maxBufferSize = 16384;
        InputStream stream = createMock(InputStream.class);
        BufferedReader reader = createMock(BufferedReader.class);
        FileStreamBuffer fileStreamBuffer = new FileStreamBuffer(stream, reader);

        FileStreamSourceTask task = new FileStreamSourceTask(fileStreamBuffer);

        EasyMock.expect(reader.ready()).andReturn(true).times(5);
        int x = 1024;
        EasyMock.expect(reader.read((char[]) EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt())).andReturn(x);
        for (int i = 0; i < 4; i++) {
            EasyMock.expect(reader.read((char[]) EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt())).andReturn(x);
            x *= 2;
        }

        stream.close();
        EasyMock.expectLastCall();
        replayAll();

        for (int i = 0; i < 20; i++) {
            task.poll();
        }

        assertEquals(FileStreamBuffer.maxBufferSize, fileStreamBuffer._bufferSize());
        task.stop();

        verifyAll();

    }


}

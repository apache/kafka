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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

class RemoteLogIndexEntryTest {
    @Test
    public void testWriteAndReadInputStream() throws Exception {
        List<RemoteLogIndexEntry> entries = Arrays.asList(
                new RemoteLogIndexEntry((short) 0, 99, 0, 999, 50, 100, 20, "aaa".getBytes(StandardCharsets.UTF_8)),
                new RemoteLogIndexEntry((short) 100, 199, 1000, 1999, 50, 100, 20, "bbb".getBytes(StandardCharsets.UTF_8)),
                new RemoteLogIndexEntry((short) 200, 299, 2000, 2999, 50, 100, 20, "ccc".getBytes(StandardCharsets.UTF_8))
        );

        Stream<ByteBuffer> buffers = entries.stream().map(RemoteLogIndexEntry::asBuffer);

        // It's OK to not close the streams, they're ephemeral.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(buffers.map(byteBuffer -> byteBuffer.limit()).mapToInt(x -> x).sum());
        buffers.forEach(b -> {
            try {
                outputStream.write(b.array());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        List<RemoteLogIndexEntry> readEntries = RemoteLogIndexEntry.readAll(inputStream);
        Assert.assertEquals(entries, readEntries);
    }

    @Test
    public void testWriteAndReadFileChannel() throws Exception {
        List<RemoteLogIndexEntry> entries = Arrays.asList(
                new RemoteLogIndexEntry((short) 0, 99, 0, 999, 50, 100, 20, "aaa".getBytes(StandardCharsets.UTF_8)),
                new RemoteLogIndexEntry((short) 100, 199, 1000, 1999, 50, 100, 20, "bbb".getBytes(StandardCharsets.UTF_8)),
                new RemoteLogIndexEntry((short) 200, 299, 2000, 2999, 50, 100, 20, "ccc".getBytes(StandardCharsets.UTF_8))
        );


        FileChannel channel = FileChannel.open(TestUtils.tempFile().toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        entries.forEach(e -> {
            try {
                channel.write(e.asBuffer());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
        channel.position(0);

        List<RemoteLogIndexEntry> readEntries = RemoteLogIndexEntry.readAll(channel);
        Assert.assertEquals(entries, readEntries);
    }
}

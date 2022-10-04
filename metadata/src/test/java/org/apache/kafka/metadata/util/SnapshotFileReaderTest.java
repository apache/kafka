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

package org.apache.kafka.metadata.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;

final public class SnapshotFileReaderTest {
    @Test
    public void testHeaderFooterControlRecord() throws Exception {
        SnapshotFileReader mockReader = Mockito.mock(SnapshotFileReader.class);
        Mockito.doCallRealMethod().when(mockReader).handleControlBatch(any(FileLogInputStream.FileChannelRecordBatch.class));

        SnapshotHeaderRecord snapshotHeaderMessage = new SnapshotHeaderRecord().setVersion((short) 0).setLastContainedLogTimestamp(1);
        SnapshotFooterRecord snapshotFooterMessage = new SnapshotFooterRecord().setVersion((short) 0);
        Record snapshotHeaderRecord = generateControlRecord(snapshotHeaderMessage, ControlRecordType.SNAPSHOT_HEADER.getType());
        Record snapshotFooterRecord = generateControlRecord(snapshotFooterMessage, ControlRecordType.SNAPSHOT_FOOTER.getType());

        FileLogInputStream.FileChannelRecordBatch mockBatch = Mockito.mock(FileLogInputStream.FileChannelRecordBatch.class);
        Iterator<Record> recordIterator = Arrays.asList(snapshotHeaderRecord, snapshotFooterRecord).iterator();
        Mockito.when(mockBatch.iterator()).thenReturn(recordIterator);

        LogCaptureAppender appender = new LogCaptureAppender();
        Logger.getRootLogger().addAppender(appender);
        try {
            mockReader.handleControlBatch(mockBatch);
            // should not log any messages
            assertThat(appender.getMessages(), not(hasItem(containsString("Ignoring control record"))));
        } finally {
            Logger.getRootLogger().removeAppender(appender);
        }
    }

    private Record generateControlRecord(ApiMessage data, short controlRecordType) throws IOException {
        ByteBuffer valueBuffer = ByteBuffer.allocate(256);
        data.write(new ByteBufferAccessor(valueBuffer), new ObjectSerializationCache(), data.highestSupportedVersion());
        valueBuffer.flip();

        byte[] keyData = new byte[]{0, 0, 0, (byte) controlRecordType};

        ByteBufferOutputStream out = new ByteBufferOutputStream(256);
        DefaultRecord.writeTo(new DataOutputStream(out), 1, 1, ByteBuffer.wrap(keyData), valueBuffer, new Header[0]);

        ByteBuffer buffer = out.buffer();
        buffer.flip();

        return DefaultRecord.readFrom(buffer, 0, 0, 0, null);
    }
}

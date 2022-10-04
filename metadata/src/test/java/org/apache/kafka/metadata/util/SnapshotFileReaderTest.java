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

import kafka.raft.KafkaMetadataLog;
import kafka.server.KafkaRaftServer;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.metadata.util.BatchFileReader.BatchAndType;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

final public class SnapshotFileReaderTest {
    @Test
    public void testHeaderFooter() throws Exception {
        List<ApiMessageAndVersion> metadataRecords = Arrays.asList(
            new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(0).setBrokerEpoch(10), (short) 0),
            new ApiMessageAndVersion(
                new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(20),  (short) 0),
            new ApiMessageAndVersion(
                new TopicRecord().setName("test-topic").setTopicId(Uuid.randomUuid()), (short) 0),
            new ApiMessageAndVersion(
                new PartitionChangeRecord().setTopicId(Uuid.randomUuid()).setLeader(1).
                    setPartitionId(0).setIsr(Arrays.asList(0, 1, 2)), (short) 0)
        );

        SnapshotFileReader mockReader = Mockito.mock(SnapshotFileReader.class);

        Mockito.doCallRealMethod().when(mockReader).handleControlBatch(any(FileLogInputStream.FileChannelRecordBatch.class));
        Mockito.doCallRealMethod().when(mockReader).handleMetadataBatch(any(FileLogInputStream.FileChannelRecordBatch.class));


        val appender = LogCaptureAppender.createAndRegister()
        val previousLevel = LogCaptureAppender.setClassLoggerLevel(classOf[AppInfoParser], Level.WARN)
        try {
            testAclCli(adminArgs)
        } finally {
            LogCaptureAppender.setClassLoggerLevel(classOf[AppInfoParser], previousLevel)
            LogCaptureAppender.unregister(appender)
        }
    }
}

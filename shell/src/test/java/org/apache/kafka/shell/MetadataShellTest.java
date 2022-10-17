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

package org.apache.kafka.shell;

import kafka.server.KafkaRaftServer;
import kafka.utils.MockScheduler;
import kafka.utils.MockTime;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.record.CompressionType;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriter;
import kafka.raft.KafkaMetadataLog;
import kafka.raft.MetadataLogConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataShellTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    private Path tmpDir;
    private File logDir;

    @BeforeEach
    public void setup() throws IOException {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(outContent));
        tmpDir = Files.createTempDirectory(null);
        logDir = new File(tmpDir.toFile(), "test-0");
    }

    @AfterEach
    public void after() {
        System.setOut(originalOut);
        System.setErr(originalErr);
        if (logDir != null) {
            try {
                Utils.delete(logDir);
            } catch (Exception e) {
                System.err.println("error while deleting:" + logDir.getAbsolutePath());
            }
        }

        if (tmpDir != null) {
            try {
                Utils.delete(tmpDir.toFile());
            } catch (Exception e) {
                System.err.println("error while deleting:" + tmpDir.toAbsolutePath());
            }
        }
    }

    @Test
    public void shouldParseSnapshotContentWithoutError() throws Exception {
        String snapshotPath = logDir.getAbsolutePath() + "/00000000000000000000-0000000000.checkpoint";

        generateSnapshotFile();

        MetadataShell.Builder builder = new MetadataShell.Builder();
        builder.setSnapshotPath(snapshotPath);
        MetadataShell shell = builder.build();

        try {
            shell.run(Collections.emptyList());
        } finally {
            shell.close();
        }

        // The output should not contain error messages during parsing snapshot content
        String expectedOutput = "Loading...\nStarting...\n";
        assertTrue(outContent.toString().startsWith(expectedOutput), "console output is not expected: " + outContent);
    }

    private void generateSnapshotFile() {
        List<ApiMessageAndVersion> metadataRecords = Arrays.asList(
            new ApiMessageAndVersion(
                new RegisterBrokerRecord().setBrokerId(0).setBrokerEpoch(10), (short) 0),
            new ApiMessageAndVersion(
                new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(20), (short) 0),
            new ApiMessageAndVersion(
                new TopicRecord().setName("test-topic").setTopicId(Uuid.randomUuid()), (short) 0)
        );

        Time time = new MockTime(0, 0);
        KafkaMetadataLog metadataLog = KafkaMetadataLog.apply(
            KafkaRaftServer.MetadataPartition(),
            KafkaRaftServer.MetadataTopicId(),
            logDir,
            time,
            new MockScheduler(time),
            MetadataLogConfig.apply(
                100 * 1024,
                100 * 1024,
                10 * 1000,
                100 * 1024,
                60 * 1000,
                KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
                KafkaRaftClient.MAX_FETCH_SIZE_BYTES,
                0,
                1
            )
        );

        SnapshotWriter<ApiMessageAndVersion> snapshotWriter = RecordsSnapshotWriter.createWithHeader(
            () -> metadataLog.createNewSnapshot(new OffsetAndEpoch(0, 0)),
            1024,
            MemoryPool.NONE,
            time,
            1,
            CompressionType.NONE,
            new MetadataRecordSerde()
        ).get();

        snapshotWriter.append(metadataRecords);
        snapshotWriter.freeze();
    }
}

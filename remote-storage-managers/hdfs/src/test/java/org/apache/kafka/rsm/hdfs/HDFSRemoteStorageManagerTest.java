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
package org.apache.kafka.rsm.hdfs;

import kafka.log.LogSegment;
import kafka.log.LogUtils;
import kafka.utils.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.LogSegmentData;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteStorageException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HDFSRemoteStorageManagerTest {
    private File logDir;
    private File remoteDir;
    MiniDFSCluster hdfsCluster;
    FileSystem hdfs;
    String baseDir = "/localcluster";
    private HashMap<String, String> config;
    private ArrayList<LogSegment> segments = new ArrayList<>();
    private int leaderEpoch = 1;

    @Before
    public void setup() throws Exception {
        logDir = TestUtils.tempDir();
        remoteDir = TestUtils.tempDir();

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, remoteDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
        hdfs = FileSystem.newInstance(new URI(hdfsURI), conf);

        LogSegment seg1 = LogUtils.createSegment(0, logDir, 4096, Time.SYSTEM);
        appendRecords(seg1, 0, 100, 0);
        appendRecords(seg1, 200, 100, 1000);
        seg1.onBecomeInactiveSegment();
        segments.add(seg1);

        LogSegment seg2 = LogUtils.createSegment(300, logDir, 4096, Time.SYSTEM);
        appendRecords(seg2, 400, 100, 2000);
        seg2.onBecomeInactiveSegment();
        segments.add(seg2);

        config = new HashMap<>();
        config.put(HDFSRemoteStorageManagerConfig.HDFS_URI_PROP, hdfsURI);
        config.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, baseDir);
    }

    private void appendRecords(LogSegment seg, long offset, int numRecords) {
        appendRecords(seg, offset, numRecords, 0L);
    }

    private void appendRecords(LogSegment seg, long offset, int numRecords, long baseTimestamp) {
        SimpleRecord[] records = new SimpleRecord[numRecords];
        long timestamp = baseTimestamp;
        for (int i = 0; i < numRecords; i++) {
            timestamp = baseTimestamp + i;
            records[i] = new SimpleRecord(timestamp, new byte[100]);
        }
        long lastOffset = offset + numRecords - 1;
        seg.append(lastOffset, timestamp, offset,
            MemoryRecords.withRecords(RecordBatch.CURRENT_MAGIC_VALUE, offset, CompressionType.NONE,
                TimestampType.CREATE_TIME, records));
    }

    @After
    public void tearDown() throws Exception {
        hdfsCluster.shutdown();
        Utils.delete(logDir);
        Utils.delete(remoteDir);
    }

    @Test
    public void testConfigAndClose() {
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(config);
        rsm.close();
    }

    private LogSegmentData getLogSegmentData(LogSegment seg) {
        return new LogSegmentData(seg.log().file(), seg.offsetIndex().file(), seg.timeIndex().file());
    }

    private void assertFileEquals(InputStream is, File f) throws Exception {
        byte[] b0 = new byte[1000000];
        byte[] b1 = new byte[1000000];

        try (InputStream is1 = new FileInputStream(f)) {
            long p = 0;
            while (p < f.length()) {
                long a = is.read(b0);
                long b = is1.read(b1);
                assertEquals(a, b);
                assertArrayEquals(b0, b1);
                p += a;
            }
        }
    }

    @Test
    public void testCopyReadAndDelete() throws Exception {
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(config);

        TopicPartition tp = new TopicPartition("test", 1);

        UUID uuid0 = UUID.randomUUID();
        RemoteLogSegmentId id0 = new RemoteLogSegmentId(tp, uuid0);
        LogSegmentData seg0 = getLogSegmentData(segments.get(0));
        rsm.copyLogSegment(id0, seg0);

        assertTrue(hdfs.exists(new Path(baseDir + "/test-1")));
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/" + uuid0 + "/log")));
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/" + uuid0 + "/index")));
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/" + uuid0 + "/timeindex")));

        UUID uuid1 = UUID.randomUUID();
        RemoteLogSegmentId id1 = new RemoteLogSegmentId(tp, uuid1);
        LogSegmentData seg1 = getLogSegmentData(segments.get(1));
        rsm.copyLogSegment(id1, seg1);
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/" + uuid1 + "/index")));

        RemoteLogSegmentMetadata seg0metadata = new RemoteLogSegmentMetadata(id0, 0, 299, 0, 0, new byte[]{});
        try (InputStream is = rsm.fetchLogSegmentData(seg0metadata, 0L, Long.MAX_VALUE)) {
            assertFileEquals(is, segments.get(0).log().file());
        }
        try (InputStream is = rsm.fetchOffsetIndex(seg0metadata)) {
            assertFileEquals(is, segments.get(0).offsetIndex().file());
        }
        try (InputStream is = rsm.fetchTimestampIndex(seg0metadata)) {
            assertFileEquals(is, segments.get(0).timeIndex().file());
        }

        RemoteLogSegmentMetadata seg1metadata = new RemoteLogSegmentMetadata(id1, 300, 499, 0, 0, new byte[]{});
        try (InputStream is = rsm.fetchLogSegmentData(seg1metadata, 0L, Long.MAX_VALUE)) {
            assertFileEquals(is, segments.get(1).log().file());
        }
        try (InputStream is = rsm.fetchOffsetIndex(seg1metadata)) {
            assertFileEquals(is, segments.get(1).offsetIndex().file());
        }
        try (InputStream is = rsm.fetchTimestampIndex(seg1metadata)) {
            assertFileEquals(is, segments.get(1).timeIndex().file());
        }

        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/" + uuid0)));
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/" + uuid1)));
        rsm.deleteLogSegment(seg0metadata);
        rsm.deleteLogSegment(seg1metadata);
        assertFalse(hdfs.exists(new Path(baseDir + "/test-1/" + uuid0)));
        assertFalse(hdfs.exists(new Path(baseDir + "/test-1/" + uuid1)));

        assertThrows(RemoteStorageException.class, () -> {
            rsm.fetchLogSegmentData(seg0metadata, 0L, Long.MAX_VALUE);
        });
        assertThrows(RemoteStorageException.class, () -> {
            rsm.fetchOffsetIndex(seg1metadata);
        });
    }
}

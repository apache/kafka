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
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.utils.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.math.Ordering;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

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
        appendRecords(seg1, 0, 100);
        appendRecords(seg1, 200, 100);
        seg1.onBecomeInactiveSegment();
        segments.add(seg1);

        LogSegment seg2 = LogUtils.createSegment(300, logDir, 4096, Time.SYSTEM);
        appendRecords(seg2, 400, 100);
        seg2.onBecomeInactiveSegment();
        segments.add(seg2);

        config = new HashMap<>();
        config.put(HDFSRemoteStorageManager.HDFS_URI_PROP, hdfsURI);
        config.put(HDFSRemoteStorageManager.HDFS_BASE_DIR_PROP, baseDir);
    }

    private void appendRecords(LogSegment seg, long offset, int numRecords) {
        SimpleRecord[] records = new SimpleRecord[numRecords];
        long timestamp = 0;
        for (int i = 0; i < numRecords; i++) {
            timestamp = (offset + i) * 1000;
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

    @Test
    public void testCopyReadAndDelete() throws Exception {
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(config);

        TopicPartition tp = new TopicPartition("test", 1);
        Seq<RemoteLogIndexEntry> indexEntries = rsm.copyLogSegment(tp, segments.get(0));

        assertEquals(0L, indexEntries.apply(0).firstOffset());
        assertEquals(0L, indexEntries.apply(0).firstTimeStamp());
        assertEquals(299, indexEntries.apply(indexEntries.size() - 1).lastOffset());
        assertEquals(299000, indexEntries.apply(indexEntries.size() - 1).lastTimeStamp());
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1")));
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/00000000000000000000-00000000000000000299/log")));

        indexEntries = rsm.copyLogSegment(tp, segments.get(1));
        assertEquals(1, indexEntries.size());
        assertEquals(400000, indexEntries.apply(0).firstTimeStamp());
        assertEquals(400, indexEntries.apply(0).firstOffset());
        assertEquals(499000, indexEntries.apply(0).lastTimeStamp());
        assertEquals(499, indexEntries.apply(0).lastOffset());
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/00000000000000000300-00000000000000000499/index")));

        Seq<RemoteLogSegmentInfo> remoteSegments = rsm.listRemoteSegments(tp);
        assertEquals(2, remoteSegments.size());

        Ordering<RemoteLogSegmentInfo> cmp = Ordering.comparatorToOrdering(
            (a, b) -> {
                return Long.compare(a.baseOffset(), b.baseOffset());
            });

        indexEntries = rsm.getRemoteLogIndexEntries(remoteSegments.min(cmp));
        Records records = rsm.read(indexEntries.apply(0), 100000, 0, true);
        int count = 0;
        for (Record r : records.records()) {
            assertFalse(r.hasKey());
            assertEquals(100, r.valueSize());
            count++;
        }
        assertEquals(200, count);

        records = rsm.read(indexEntries.apply(0), 100000, 200, true);
        count = 0;
        for (Record r : records.records()) {
            assertFalse(r.hasKey());
            assertEquals(100, r.valueSize());
            count++;
        }
        assertEquals(100, count);

        records = rsm.read(indexEntries.apply(0), 100, 0, true);
        count = 0;
        for (Record r : records.records()) {
            assertFalse(r.hasKey());
            assertEquals(100, r.valueSize());
            count++;
        }
        assertEquals(100, count);

        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/00000000000000000000-00000000000000000299")));
        assertTrue(hdfs.exists(new Path(baseDir + "/test-1/00000000000000000300-00000000000000000499")));
        for (RemoteLogSegmentInfo segment : JavaConverters.asJavaCollection(remoteSegments)) {
            rsm.deleteLogSegment(segment);
        }
        assertFalse(hdfs.exists(new Path(baseDir + "/test-1/00000000000000000000-00000000000000000299")));
        assertFalse(hdfs.exists(new Path(baseDir + "/test-1/00000000000000000300-00000000000000000499")));
    }

    @Test
    public void testReadAfterDelete() throws Exception {
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(config);

        TopicPartition tp = new TopicPartition("test", 1);
        rsm.copyLogSegment(tp, segments.get(0));

        Seq<RemoteLogSegmentInfo> remoteSegments = rsm.listRemoteSegments(tp);
        Seq<RemoteLogIndexEntry> indexEntries = rsm.getRemoteLogIndexEntries(remoteSegments.apply(0));

        Records records = rsm.read(indexEntries.apply(0), 100000, 0, true);
        int count = 0;
        for (Record r : records.records()) {
            assertFalse(r.hasKey());
            assertEquals(100, r.valueSize());
            count++;
        }
        assertEquals(200, count);

        rsm.deleteLogSegment(remoteSegments.apply(0));

        assertThrows(IOException.class, () -> {
            rsm.read(indexEntries.apply(0), 100000, 0, true);
        });

        assertThrows(IOException.class, () -> {
            rsm.getRemoteLogIndexEntries(remoteSegments.apply(0));
        });
    }

    @Test
    public void testCopyTwice() throws Exception {
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(config);
        TopicPartition tp = new TopicPartition("test", 1);
        rsm.copyLogSegment(tp, segments.get(0));

        HDFSRemoteStorageManager rsm2 = new HDFSRemoteStorageManager();
        rsm2.configure(config);
        assertThrows(IOException.class, () -> {
            rsm2.copyLogSegment(tp, segments.get(0));
        });

        assertEquals(1, rsm2.listRemoteSegments(tp).size());
        rsm2.deleteLogSegment(rsm2.listRemoteSegments(tp).apply(0));
    }

    class ConcurrentWriteThread extends Thread {
        private AtomicInteger successCount;
        LogSegment segment;
        private CyclicBarrier barrier;

        ConcurrentWriteThread(AtomicInteger successCount, LogSegment segment, CyclicBarrier barrier) {
            this.successCount = successCount;
            this.segment = segment;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
            rsm.configure(config);
            TopicPartition tp = new TopicPartition("test", 2);

            try {
                barrier.await();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            try {
                Seq<RemoteLogIndexEntry> indexEntries = rsm.copyLogSegment(tp, segment);
                if (indexEntries.nonEmpty()) {
                    successCount.incrementAndGet();
                }
            } catch (IOException e) {
            }
        }
    }

    @Test
    public void testConcurrentWrite() throws Exception {
        File logDir1 = TestUtils.tempDir();
        File logDir2 = TestUtils.tempDir();
        // segment 0-299
        LogSegment seg1 = LogUtils.createSegment(100, logDir1, 4096, Time.SYSTEM);
        appendRecords(seg1, 100, 100);
        appendRecords(seg1, 200, 100);
        seg1.onBecomeInactiveSegment();

        // segment 0-399
        LogSegment seg2 = LogUtils.createSegment(100, logDir2, 4096, Time.SYSTEM);
        appendRecords(seg2, 100, 100);
        appendRecords(seg2, 200, 200);
        seg2.onBecomeInactiveSegment();

        AtomicInteger successCount1 = new AtomicInteger();
        AtomicInteger successCount2 = new AtomicInteger();

        int numThreads = 10;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            if (i < numThreads / 2) {
                threads[i] = new ConcurrentWriteThread(successCount1, seg1, barrier);
            } else {
                threads[i] = new ConcurrentWriteThread(successCount2, seg2, barrier);
            }

            threads[i].start();
        }

        for (Thread t : threads)
            t.join();

        assertEquals(1, successCount1.get());
        assertEquals(1, successCount2.get());

        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(config);
        TopicPartition tp = new TopicPartition("test", 2);
        Seq<RemoteLogSegmentInfo> segments = rsm.listRemoteSegments(tp);
        assertEquals(2, segments.size());
    }

    @Test
    public void testIndexInterval() throws Exception {
        LogSegment seg = LogUtils.createSegment(10000, logDir, 4096, Time.SYSTEM);
        for (int i = 0; i < 100; i++) {
            appendRecords(seg, 10000 + i * 100, 100);
        }
        seg.onBecomeInactiveSegment();

        // default 256KB
        HDFSRemoteStorageManager rsm1 = new HDFSRemoteStorageManager();
        rsm1.configure(config);
        TopicPartition tp1 = new TopicPartition("test", 10);
        Seq<RemoteLogIndexEntry> indexEntries = rsm1.copyLogSegment(tp1, seg);
        assertEquals(5, indexEntries.size());

        // 1 byte
        HDFSRemoteStorageManager rsm2 = new HDFSRemoteStorageManager();
        config.put(HDFSRemoteStorageManager.HDFS_REMOTE_INDEX_INTERVAL_BYTES, "1");
        rsm1.configure(config);
        TopicPartition tp2 = new TopicPartition("test", 20);
        indexEntries = rsm1.copyLogSegment(tp2, seg);
        assertEquals(100, indexEntries.size());

        // 1,000,000 bytes
        HDFSRemoteStorageManager rsm3 = new HDFSRemoteStorageManager();
        config.put(HDFSRemoteStorageManager.HDFS_REMOTE_INDEX_INTERVAL_BYTES, "1000000");
        rsm1.configure(config);
        TopicPartition tp3 = new TopicPartition("test", 30);
        indexEntries = rsm1.copyLogSegment(tp3, seg);
        assertEquals(2, indexEntries.size());

        // 500,001 bytes
        HDFSRemoteStorageManager rsm4 = new HDFSRemoteStorageManager();
        config.put(HDFSRemoteStorageManager.HDFS_REMOTE_INDEX_INTERVAL_BYTES, "500001");
        rsm1.configure(config);
        TopicPartition tp4 = new TopicPartition("test", 40);
        indexEntries = rsm1.copyLogSegment(tp4, seg);
        assertEquals(3, indexEntries.size());
    }
}

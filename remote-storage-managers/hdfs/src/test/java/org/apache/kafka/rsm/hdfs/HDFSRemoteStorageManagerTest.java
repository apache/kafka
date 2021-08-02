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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HDFSRemoteStorageManagerTest {

    private final String baseDir = "/kafka-remote-logs";
    private final TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test", 1));

    private File logDir;
    private File remoteDir;
    private Configuration hadoopConf;
    private MiniDFSCluster hdfsCluster;
    private FileSystem hdfs;
    private Map<String, String> configs;

    private RemoteStorageManager rsm;

    @BeforeEach
    public void setup() throws Exception {
        logDir = TestUtils.tempDirectory();
        remoteDir = TestUtils.tempDirectory();

        int nameNodePort = new Random().nextInt(5000);
        hadoopConf = new Configuration();
        hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, remoteDir.getAbsolutePath());
        hadoopConf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:" + nameNodePort);

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hadoopConf);
        builder.clusterId("test_mini_dfs_cluster");
        hdfsCluster = builder.build();

        configs = new HashMap<>();
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, baseDir);

        rsm = new HDFSRemoteStorageManager();
        ((HDFSRemoteStorageManager) rsm).setHadoopConfiguration(hadoopConf);
        rsm.configure(configs);
        hdfs = ((HDFSRemoteStorageManager) rsm).getFS();
    }

    @AfterEach
    public void tearDown() throws Exception {
        hdfsCluster.shutdown();
        Utils.delete(logDir);
        Utils.delete(remoteDir);
    }

    @Test
    public void testConfigAndClose() throws Exception {
        RemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(configs);
        rsm.close();
    }

    @Test
    @Disabled
    public void testCopySegmentUptoMaxSegmentLimitOfTwoGB() throws Exception {
        verifyUpload(rsm, tp, Uuid.randomUuid(), 0, Integer.MAX_VALUE, true);
    }

    @Test
    public void testSecureLogin() {
        System.setProperty("java.security.krb5.realm", "ATHENA.MIT.EDU");
        System.setProperty("java.security.krb5.kdc", "kerberos.mit.edu:88");
        String user = "test@ATHENA.MIT.EDU";

        Map<String, String> secureConfigs = new HashMap<>(configs);
        secureConfigs.put(HDFSRemoteStorageManagerConfig.HDFS_USER_PROP, user);
        secureConfigs.put(HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP, "test.keytab");

        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.setHadoopConfiguration(configuration);
        Throwable exception = assertThrows(RuntimeException.class, () -> rsm.configure(secureConfigs),
                "Unable to login as user: " + user);
        assertTrue(exception.getCause() instanceof KerberosAuthException);
        UserGroupInformation.setConfiguration(new Configuration());
    }

    @Test
    public void testCopyReadAndDelete() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, true);
        verifyDeleteRemoteLogSegment(rsm, metadata, tp, uuid);
    }

    @Test
    public void testCopyReadAndDeleteWithMultipleSegments() throws Exception {
        Uuid uuid0 = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata0 = verifyUpload(rsm, tp, uuid0, 0, 1000, true);
        Uuid uuid1 = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata1 = verifyUpload(rsm, tp, uuid1, 1000, 2000, true);
        verifyDeleteRemoteLogSegment(rsm, metadata0, tp, uuid0);
        verifyDeleteRemoteLogSegment(rsm, metadata1, tp, uuid1);
    }

    @Test
    public void testCopyReadAndDeleteWithoutOptionalFiles() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, false);
        verifyDeleteRemoteLogSegment(rsm, metadata, tp, uuid);
    }

    @Test
    public void testFetchOnOptionalFile() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, false);
        try (InputStream stream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET)) {
            assertNotEquals(0, stream.available());
        }
        try (InputStream stream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.TRANSACTION)) {
            assertEquals(0, stream.available());
        }
    }

    @Test
    public void testFetchLogSegment() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        int segSize = 1000;
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id, 0L, 100L, 0L, 0, 1L, segSize, Collections.singletonMap(0, 0L));
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
        rsm.copyLogSegmentData(segmentMetadata, segmentData);

        // start and end position are both inclusive in RSM
        // full fetch segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0, 999, 1000);
        // fetch intermediate segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 100, 199, 100);
        // fetch till the end of the segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 990, 999, 10);
        // fetch exceeds the segment size
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 990, 1050, 10);
    }
    
    @Test
    public void testRepeatedFetchReadsFromCacheOnFullSegmentFetch() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(10 * 1048576L);
        // two cache hits due to the additional 25 bytes read for the header exceeds the defined cache line size of 2MB.
        testRepeatedCacheReads(cache, String.valueOf(2 * 1048576), 2097152, 2);
    }

    @Test
    public void testRepeatedFetchReadsFromCacheOnSegmentSizeLessThanCacheLine() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(10 * 1048576L);
        testRepeatedCacheReads(cache, String.valueOf(1048576), 1024, 1);
    }

    @Test
    public void testDeleteOnNonExistentFiles() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, 1L, 1000,
                Collections.singletonMap(0, 0L));
        String path = baseDir + File.separator + tp + File.separator + uuid;
        assertFalse(hdfs.exists(new Path(path)));
        rsm.deleteLogSegmentData(metadata);
        assertFalse(hdfs.exists(new Path(path)));
    }

    private void testRepeatedCacheReads(LRUCacheWithContext cache,
                                        String cacheLineSizeInBytes,
                                        int segSize,
                                        int expectedCacheHit) throws Exception {
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP, cacheLineSizeInBytes);
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.setHadoopConfiguration(hadoopConf);
        rsm.configure(configs);
        rsm.setLRUCache(cache);

        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
        rsm.copyLogSegmentData(segmentMetadata, segmentData);

        assertEquals(0, cache.getCacheHit());
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, segSize);
        assertEquals(0, cache.getCacheHit());

        // read from cache
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, segSize);
        // two cache hits due to the additional 25 bytes read for the header.
        assertEquals(expectedCacheHit, cache.getCacheHit());
    }

    /**
     * Verifies the log segment fetch.
     * @param rsm           remote storage manager
     * @param metadata      metadata about the remote log segment.
     * @param segmentData   segment data.
     * @param startPosition start position to fetch from the segment, inclusive
     * @param endPosition   Fetch data till the end position, inclusive
     * @throws Exception I/O Error, file not found exception.
     */
    private void verifyFetchLogSegment(RemoteStorageManager rsm,
                                       RemoteLogSegmentMetadata metadata,
                                       LogSegmentData segmentData,
                                       int startPosition,
                                       int endPosition,
                                       int size) throws Exception {
        try (InputStream stream = rsm.fetchLogSegment(metadata, startPosition, endPosition)) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
            SeekableByteChannel byteChannel = Files.newByteChannel(segmentData.logSegment());
            byteChannel.position(startPosition);
            byteChannel.read(buffer);
            buffer.rewind();
            assertDataEquals(buffer, stream);
        }
    }

    private RemoteLogSegmentMetadata verifyUpload(RemoteStorageManager rsm,
                                                  TopicIdPartition tp,
                                                  Uuid uuid,
                                                  int startOffset,
                                                  int segSize,
                                                  boolean withOptionalFiles) throws Exception {
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
        LogSegmentData segmentData = TestLogSegmentUtils
                .createLogSegmentData(logDir, startOffset, segSize, withOptionalFiles);
        rsm.copyLogSegmentData(segmentMetadata, segmentData);
        checkFileExistence(uuid);
        checkAssociatedFileContents(rsm, segmentMetadata, segmentData);
        assertTrue(hdfs.exists(new Path(baseDir + File.separator + HDFSRemoteStorageManager.generatePath(tp) + File.separator + uuid)));
        return segmentMetadata;
    }

    private void verifyDeleteRemoteLogSegment(RemoteStorageManager rsm,
                                              RemoteLogSegmentMetadata metadata,
                                              TopicIdPartition tp,
                                              Uuid uuid) throws RemoteStorageException, IOException {
        rsm.deleteLogSegmentData(metadata);
        assertFalse(hdfs.exists(new Path(baseDir + File.separator + tp.toString() + File.separator + uuid)));
        RemoteStorageException ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchLogSegment(metadata, 0));
        assertEquals("Failed to fetch SEGMENT file from remote storage", ex.getMessage());
        ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET));
        assertEquals("Failed to fetch OFFSET_INDEX file from remote storage", ex.getMessage());
    }

    private void checkFileExistence(Uuid uuid) throws IOException {
        Path path = new Path(baseDir, HDFSRemoteStorageManager.generatePath(tp));
        assertTrue(hdfs.exists(path));

        Path filePath = new Path(path, uuid.toString());
        assertTrue(hdfs.exists(filePath));

        int count = 0;
        RemoteIterator<LocatedFileStatus> iter = hdfs.listFiles(filePath, true);
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        assertEquals(1, count);
    }

    private void checkAssociatedFileContents(RemoteStorageManager rsm,
                                             RemoteLogSegmentMetadata metadata,
                                             LogSegmentData segmentData) throws Exception {
        try (InputStream actualStream = rsm.fetchLogSegment(metadata, 0)) {
            assertFileEquals(segmentData.logSegment().toFile(), actualStream);
        }

        try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET)) {
            assertFileEquals(segmentData.offsetIndex().toFile(), actualStream);
        }

        try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.TIMESTAMP)) {
            assertFileEquals(segmentData.timeIndex().toFile(), actualStream);
        }

        try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.LEADER_EPOCH)) {
            ByteBuffer leaderEpochIndex = segmentData.leaderEpochIndex();
            leaderEpochIndex.rewind();
            assertDataEquals(leaderEpochIndex, actualStream);
        }

        if (segmentData.transactionIndex().isPresent()) {
            try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.TRANSACTION)) {
                assertFileEquals(segmentData.transactionIndex().get().toFile(), actualStream);
            }
        }

        if (segmentData.producerSnapshotIndex() != null) {
            try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT)) {
                assertFileEquals(segmentData.producerSnapshotIndex().toFile(), actualStream);
            }
        }
    }

    private void assertFileEquals(File expected, InputStream actual) throws Exception {
        byte[] expectedBuffer = new byte[1_000_000];
        byte[] actualBuffer = new byte[1_000_000];
        try (InputStream is1 = new FileInputStream(expected)) {
            long bytesRead = 0;
            while (bytesRead < expected.length()) {
                long expectedBytesRead = is1.read(expectedBuffer);
                long actualBytesRead = actual.read(actualBuffer);
                assertEquals(expectedBytesRead, actualBytesRead);
                assertArrayEquals(expectedBuffer, actualBuffer);
                bytesRead += expectedBytesRead;
            }
        }
    }

    private void assertDataEquals(ByteBuffer expectedBuffer, InputStream actual) throws Exception {
        ByteBuffer actualBuffer = ByteBuffer.wrap(new byte[actual.available()]);
        int read = actual.read(actualBuffer.array());
        assertEquals(expectedBuffer.limit(), read);
        assertEquals(expectedBuffer, actualBuffer);
    }

    private static class LRUCacheWithContext extends LRUCache {
        private int cacheHit = 0;

        private LRUCacheWithContext(long maxBytes) {
            super(maxBytes);
        }

        synchronized byte[] get(String path, long offset) {
            byte[] result = super.get(path, offset);
            if (result != null) {
                cacheHit++;
            }
            return result;
        }

        public int getCacheHit() {
            return cacheHit;
        }
    }
}

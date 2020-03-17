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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.log.remote.storage.LogSegmentData;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentContext;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteStorageException;
import org.apache.kafka.common.log.remote.storage.RemoteStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

public class HDFSRemoteStorageManager implements RemoteStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRemoteStorageManager.class);
    private static final String LOG_FILE_NAME = "log";
    private static final String OFFSET_INDEX_FILE_NAME = "index";
    private static final String TIME_INDEX_FILE_NAME = "timeindex";

    private URI fsURI = null;
    private String baseDir = null;
    private Configuration hadoopConf = null;
    private ThreadLocal<FileSystem> fs = new ThreadLocal<>();
    private int cacheLineSize;
    private LRUCache readCache;

    @Override
    public RemoteLogSegmentContext copyLogSegment(RemoteLogSegmentId remoteLogSegmentId, LogSegmentData logSegmentData) throws RemoteStorageException {
        try {
            String desDir = getSegmentRemoteDir(remoteLogSegmentId);

            File logFile = logSegmentData.logSegment();
            File offsetIdxFile = logSegmentData.offsetIndex();
            File tsIdxFile = logSegmentData.timeIndex();

            FileSystem fs = getFS();
            fs.mkdirs(new Path(desDir));

            // copy local files to remote temporary directory
            copyFile(fs, logFile, getPath(desDir, LOG_FILE_NAME));
            copyFile(fs, offsetIdxFile, getPath(desDir, OFFSET_INDEX_FILE_NAME));
            copyFile(fs, tsIdxFile, getPath(desDir, TIME_INDEX_FILE_NAME));

            return RemoteLogSegmentContext.EMPTY_CONTEXT;
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to copy log segment to remote storage", e);
        }
    }

    private void copyFile(FileSystem fs, File localFile, Path path) throws IOException {
        fs.copyFromLocalFile(new Path(localFile.getAbsolutePath()), path);
        // keep the original mtime. we will use the mtime to calculate retention time
        fs.setTimes(path, localFile.lastModified(), System.currentTimeMillis());
    }

    @Override
    public InputStream fetchLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata, Long startPosition, Long endPosition) throws RemoteStorageException {
        try {
            String path = getSegmentRemoteDir(remoteLogSegmentMetadata.remoteLogSegmentId());
            Path logFile = getPath(path, LOG_FILE_NAME);
            return new CachedInputStream(logFile, startPosition, endPosition);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to fetch remote log segment", e);
        }
    }

    @Override
    public InputStream fetchOffsetIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        try {
            String path = getSegmentRemoteDir(remoteLogSegmentMetadata.remoteLogSegmentId());
            Path indexFile = getPath(path, OFFSET_INDEX_FILE_NAME);
            return new CachedInputStream(indexFile, 0, Long.MAX_VALUE);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to fetch offset index from remote storage", e);
        }
    }

    @Override
    public InputStream fetchTimestampIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        try {
            String path = getSegmentRemoteDir(remoteLogSegmentMetadata.remoteLogSegmentId());
            Path timeindexFile = getPath(path, TIME_INDEX_FILE_NAME);
            return new CachedInputStream(timeindexFile, 0, Long.MAX_VALUE);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to fetch timestamp index from remote storage", e);
        }
    }

    @Override
    public boolean deleteLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        try {
            String path = getSegmentRemoteDir(remoteLogSegmentMetadata.remoteLogSegmentId());

            FileSystem fs = getFS();
            return fs.delete(new Path(path), true);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to delete remote log segment", e);
        }
    }

    private class CachedInputStream extends InputStream {
        private Path logFile;
        private long currentPos;
        private long endPos;
        private long fileLen;
        private FSDataInputStream inputStream;

        CachedInputStream(Path logFile, long currentPos, long endPos) throws IOException {
            this.logFile = logFile;
            this.currentPos = currentPos;
            this.endPos = endPos;
            FileSystem fs = getFS();
            fileLen = Math.min(fs.getFileStatus(logFile).getLen(), endPos);
        }

        @Override
        public int read() throws IOException {
            if (currentPos >= fileLen)
                throw new EOFException();
            byte[] data = getCachedData(currentPos);
            return data[(int) ((currentPos++) % cacheLineSize)];
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            int pos = 0;
            if (len > fileLen - currentPos)
                len = (int) (fileLen - currentPos);
            while (pos < len) {
                byte[] data = getCachedData(currentPos + pos);
                int length = (int) Math.min(len - pos, data.length - (currentPos + pos) % cacheLineSize);
                System.arraycopy(data, (int) (currentPos + pos) % cacheLineSize,
                    buf, pos + off,
                    length);
                pos += length;
            }
            currentPos += pos;
            return pos;
        }

        private byte[] getCachedData(long position) throws IOException {
            long pos = (position / cacheLineSize) * cacheLineSize;

            byte[] data = readCache.get(logFile.toString(), pos);
            if (data != null)
                return data;

            if (inputStream == null) {
                FileSystem fs = getFS();
                inputStream = fs.open(logFile);
            }
            long dataLength = Math.min(cacheLineSize, fileLen - pos);
            data = new byte[(int) dataLength];
            inputStream.readFully(pos, data);
            readCache.put(logFile.toString(), pos, data);
            return data;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        }
    }

    @Override
    public void close() {
        if (fs.get() != null) {
            try {
                fs.get().close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * Initialize this instance with the given configs
     *
     * @param configs Key-Value pairs of configuration parameters
     */
    @Override
    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);

        fsURI = URI.create(conf.getString(HDFSRemoteStorageManagerConfig.HDFS_URI_PROP));
        baseDir = conf.getString(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP);
        cacheLineSize = conf.getInt(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_MB_PROP) * 1048576;
        long cacheSize = conf.getInt(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_MB_PROP) * 1048576L;
        if (cacheSize < cacheLineSize) {
            throw new IllegalArgumentException(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_MB_PROP +
                " is larger than " + HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_MB_PROP);
        }
        readCache = new LRUCache(cacheSize);

        hadoopConf = new Configuration(); // Load configuration from hadoop configuration files in class path
    }

    private FileSystem getFS() throws IOException {
        if (fs.get() == null) {
            fs.set(FileSystem.newInstance(fsURI, hadoopConf));
        }

        return fs.get();
    }

    private String getSegmentRemoteDir(RemoteLogSegmentId remoteLogSegmentId) {
        return baseDir + "/" + remoteLogSegmentId.topicPartition() + "/" + remoteLogSegmentId.id();
    }

    private Path getPath(String dirPath, String fileName) {
        return new Path(dirPath + "/" + fileName);
    }
}

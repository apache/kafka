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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_USER_PROP;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.OFFSET_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.SEGMENT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TIMESTAMP_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TRANSACTION_INDEX;

public class HDFSRemoteStorageManager implements RemoteStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRemoteStorageManager.class);

    private String baseDir;
    private Configuration hadoopConf;
    private int cacheLineSize;
    private LRUCache readCache;
    private final ThreadLocal<FileSystem> fs = new ThreadLocal<>();

    /**
     * Initialize this instance with the given configs
     *
     * @param configs Key-Value pairs of configuration parameters
     */
    @Override
    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);

        baseDir = conf.getString(HDFS_BASE_DIR_PROP);
        cacheLineSize = conf.getInt(HDFS_REMOTE_READ_BYTES_PROP);
        long cacheSize = conf.getLong(HDFS_REMOTE_READ_CACHE_BYTES_PROP);
        if (cacheSize < cacheLineSize) {
            throw new IllegalArgumentException(String.format("%s is larger than %s", HDFS_REMOTE_READ_BYTES_PROP, HDFS_REMOTE_READ_CACHE_BYTES_PROP));
        }
        readCache = new LRUCache(cacheSize);

        if (hadoopConf == null) {
            // Loads configuration from hadoop configuration files in class path
            hadoopConf = new Configuration();
        }
        String authentication = hadoopConf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
        if (authentication.equalsIgnoreCase("kerberos")) {
            String user = conf.getString(HDFS_USER_PROP);
            String keytabPath = conf.getString(HDFS_KEYTAB_PATH_PROP);
            try {
                UserGroupInformation.setConfiguration(hadoopConf);
                UserGroupInformation.loginUserFromKeytab(user, keytabPath);
            } catch (final Exception ex) {
                throw new RuntimeException(String.format("Unable to login as user: %s", user), ex);
            }
        }
    }

    @Override
    public void copyLogSegmentData(RemoteLogSegmentMetadata metadata, LogSegmentData segmentData) throws RemoteStorageException {
        try {
            final Path dirPath = new Path(getSegmentRemoteDir(metadata.remoteLogSegmentId()));
            final FSDataOutputStream fsOut = getFS().create(dirPath);

            final LogSegmentDataHeader header = LogSegmentDataHeader.create(segmentData);
            byte[] serializedHeader = LogSegmentDataHeader.serialize(header);
            fsOut.write(serializedHeader, 0, serializedHeader.length);
            uploadFile(segmentData.offsetIndex(), fsOut, false);
            uploadFile(segmentData.timeIndex(), fsOut, false);
            uploadData(segmentData.leaderEpochIndex(), fsOut, false);
            uploadFile(segmentData.producerSnapshotIndex(), fsOut, false);
            if (segmentData.transactionIndex().isPresent()) {
                uploadFile(segmentData.transactionIndex().get(), fsOut, false);
            }
            uploadFile(segmentData.logSegment(), fsOut, true);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to copy log segment to remote storage", e);
        }
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       int startPosition) throws RemoteStorageException {
        return fetchData(metadata, SEGMENT, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        return fetchData(metadata, SEGMENT, startPosition, endPosition);
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata metadata, IndexType indexType) throws RemoteStorageException {
        switch (indexType) {
            case OFFSET:
                return fetchData(metadata, OFFSET_INDEX, 0);
            case TIMESTAMP:
                return fetchData(metadata, TIMESTAMP_INDEX, 0);
            case TRANSACTION:
                return fetchData(metadata, TRANSACTION_INDEX, 0);
            case PRODUCER_SNAPSHOT:
                return fetchData(metadata, PRODUCER_SNAPSHOT, 0);
            case LEADER_EPOCH:
                return fetchData(metadata, LEADER_EPOCH_CHECKPOINT, 0);
            default:
                throw new KafkaException("Unknown index type :" + indexType);
        }
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata segmentMetadata) throws RemoteStorageException {
        boolean delete;
        String pathLoc;
        try {
            pathLoc = getSegmentRemoteDir(segmentMetadata.remoteLogSegmentId());
            Path path = new Path(pathLoc);
            FileSystem fs = getFS();
            if (fs.exists(path)) {
                delete = fs.delete(path, true);
            } else {
                delete = true;
                LOGGER.warn("Skipping the call to delete log segment data: {} as the segment file doesn't exists",
                        segmentMetadata);
            }
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to delete remote log segment with id:" +
                    segmentMetadata.remoteLogSegmentId(), e);
        }
        if (!delete) {
            throw new RemoteStorageException("Failed to delete remote log segment with id: " +
                    segmentMetadata.remoteLogSegmentId());
        }
    }

    @Override
    public void close() {
        Utils.closeQuietly(fs.get(), "Hadoop file system");
    }

    @VisibleForTesting
    void setLRUCache(final LRUCache cache) {
        this.readCache = cache;
    }

    @VisibleForTesting
    void setHadoopConfiguration(final Configuration configuration) {
        this.hadoopConf = configuration;
    }

    private void uploadFile(final java.nio.file.Path localSrc,
                            final FSDataOutputStream out,
                            final boolean closeStream) throws IOException {
        if (localSrc != null && localSrc.toFile().exists()) {
            final int bufferSize = hadoopConf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
                    CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
            final byte[] buf = new byte[bufferSize];
            try (final FileInputStream fis = new FileInputStream(localSrc.toFile())) {
                int bytesRead = fis.read(buf);
                while (bytesRead >= 0) {
                    out.write(buf, 0, bytesRead);
                    bytesRead = fis.read(buf);
                }
            }
            if (closeStream && out != null) {
                out.flush();
                Utils.closeAll(out);
            }
        }
    }

    private void uploadData(final ByteBuffer localSrc,
                            final FSDataOutputStream out,
                            final boolean closeStream) throws IOException {
        if (localSrc != null) {
            final int bufferSize = hadoopConf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
                                                     CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);

            final byte[] buf = new byte[bufferSize];
            try (final ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(localSrc)) {
                int bytesRead = byteBufferInputStream.read(buf);
                while (bytesRead >= 0) {
                    out.write(buf, 0, bytesRead);
                    bytesRead = byteBufferInputStream.read(buf);
                }
            }
            if (closeStream && out != null) {
                out.flush();
                Utils.closeAll(out);
            }
        }
    }

    private InputStream fetchData(RemoteLogSegmentMetadata metadata,
                                  LogSegmentDataHeader.FileType fileType,
                                  int startPosition) throws RemoteStorageException {
        return fetchData(metadata, fileType, startPosition, Integer.MAX_VALUE);
    }

    private InputStream fetchData(RemoteLogSegmentMetadata metadata,
                                  LogSegmentDataHeader.FileType fileType,
                                  int startPosition,
                                  int endPosition) throws RemoteStorageException {
        try {
            Path dataFilePath = new Path(getSegmentRemoteDir(metadata.remoteLogSegmentId()));

            return new CachedInputStream(dataFilePath, fileType, startPosition,
                                         // if end position is INT.MAX, convert that into Long.MAX. It indicates until the end of the data for
                                         // that file type.
                                         endPosition == Integer.MAX_VALUE ? Long.MAX_VALUE : addExact(endPosition, 1L));
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format("Failed to fetch %s file from remote storage", fileType), e);
        }
    }

    @VisibleForTesting
    FileSystem getFS() throws IOException {
        if (fs.get() == null) {
            fs.set(FileSystem.newInstance(hadoopConf));
        }
        return fs.get();
    }

    private String getSegmentRemoteDir(RemoteLogSegmentId remoteLogSegmentId) {
        return baseDir + "/" + generatePath(remoteLogSegmentId.topicIdPartition()) + "/" + remoteLogSegmentId.id();
    }

    public static String generatePath(TopicIdPartition topicIdPartition) {
        return topicIdPartition.topicPartition() + "-" + topicIdPartition.topicId();
    }

    private long addExact(long base, long increment) {
        try {
            base = Math.addExact(base, increment);
        } catch (final ArithmeticException swallow) {
            //Always return max value if error.
            base = Long.MAX_VALUE;
        }
        return base;
    }

    private class CachedInputStream extends InputStream {
        private final Path dataPath;
        private final long fileLen;
        private long currentPos;
        private FSDataInputStream inputStream;
        private final boolean fullFetch;

        /**
         * Input Stream which caches the data to serve them locally on repeated reads.
         * @param dataPath   path of the data file.
         * @param fileType   type of the file.
         * @param currentPos current position to read from the stream, inclusive.
         * @param endPos     to read upto the end position, exclusive.
         * @throws IOException IO problems
         */
        CachedInputStream(Path dataPath,
                          LogSegmentDataHeader.FileType fileType,
                          long currentPos,
                          long endPos) throws IOException {
            this.dataPath = dataPath;

            FileSystem fs = getFS();
            // Sends a remote fetch to read the file header.
            inputStream = fs.open(dataPath);
            byte[] buffer = new byte[LogSegmentDataHeader.LENGTH];
            inputStream.readFully(0, buffer);

            LogSegmentDataHeader.DataPosition dataPosition = LogSegmentDataHeader
                    .deserialize(ByteBuffer.wrap(buffer))
                    .getDataPosition(fileType);
            this.currentPos = addExact(currentPos, dataPosition.getPos());
            endPos = addExact(endPos, dataPosition.getPos());

            long actualFileLength = fs.getFileStatus(dataPath).getLen();
            fileLen = (dataPosition.getLength() == Integer.MAX_VALUE) ?
                    Math.min(endPos, actualFileLength) :
                    Math.min(endPos, dataPosition.getPos() + dataPosition.getLength());
            fullFetch = fileLen == actualFileLength;
        }

        @Override
        public int read() throws IOException {
            if (currentPos >= fileLen)
                return -1;
            byte[] data = getCachedData(currentPos);
            return data[(int) ((currentPos++) % cacheLineSize)];
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            int pos = 0;
            if (len > fileLen - currentPos)
                len = (int) (fileLen - currentPos);

            if (len <= 0)
                return -1;

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

        @Override
        public int available() {
            long available = fileLen - currentPos;
            if (available > Integer.MAX_VALUE)
                return Integer.MAX_VALUE;

            return (int) available;
        }

        private byte[] getCachedData(long position) throws IOException {
            long pos = (position / cacheLineSize) * cacheLineSize;

            byte[] data = readCache.get(dataPath.toString(), pos);
            if (data != null)
                return data;

            long dataLength = Math.min(cacheLineSize, fileLen - pos);
            data = new byte[(int) dataLength];
            inputStream.readFully(pos, data);
            // To avoid intermediate results being stored in cache, save only full fetch request results.
            if (fullFetch || dataLength == cacheLineSize) {
                readCache.put(dataPath.toString(), pos, data);
            }
            return data;
        }

        @Override
        public void close() throws IOException {
            Utils.closeAll(inputStream);
            inputStream = null;
        }
    }
}

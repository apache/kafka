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
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.log.remote.RemoteStorageManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static kafka.log.remote.RemoteLogManager.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

public class HDFSRemoteStorageManager implements RemoteStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRemoteStorageManager.class);

    // TODO: Use the utilities in AbstractConfig. Should we support dynamic config?
    public static final String HDFS_URI_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.fs.uri";
    public static final String HDFS_BASE_DIR_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.base.dir";
    public static final String HDFS_REMOTE_INDEX_INTERVAL_BYTES = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.remote.index.interval.bytes";
    public static final String HDFS_REMOTE_INDEX_INTERVAL_BYTES_DEFAULT = "262144";

    private static final String REMOTE_LOG_DIR_FORMAT = "%020d-%020d";
    private static final Pattern REMOTE_SEGMENT_DIR_NAME_PATTERN = Pattern.compile("(\\d{20})-(\\d{20})");
    private static final String LOG_FILE_NAME = "log";
    private static final String OFFSET_INDEX_FILE_NAME = "index";
    private static final String TIME_INDEX_FILE_NAME = "timeindex";
    private static final String REMOTE_INDEX_FILE_NAME = "remotelogindex";
    private static final Pattern RDI_PATTERN = Pattern.compile("(.*):(\\d+)");
    private static final String FILE_PATH = "file_path";

    private static AtomicInteger seqNum = new AtomicInteger(0);

    private long indexIntervalBytes;
    private URI fsURI = null;
    private String baseDir = null;
    private Configuration hadoopConf = null;
    private ThreadLocal<FileSystem> fs = new ThreadLocal<>();

    @Override
    public long earliestLogOffset(TopicPartition tp) throws IOException {
        List<RemoteLogSegmentInfo> remoteLogSegmentInfos = listRemoteSegments(tp);
        //todo better to avoid it seeking from remote storage when it can be cached here especially incase of leader.
        return (remoteLogSegmentInfos.isEmpty()) ? -1L : remoteLogSegmentInfos.get(0).baseOffset();
    }

    @Override
    public List<RemoteLogIndexEntry> copyLogSegment(TopicPartition topicPartition, LogSegment logSegment, int leaderEpoch)
            throws IOException {
        long baseOffset = logSegment.baseOffset();
        long lastOffset = logSegment.readNextOffset() - 1;

        // directly return empty seq if the log segment is empty
        // we don't need to copy empty segment
        if (lastOffset <= baseOffset)
            return Collections.emptyList();

        String desDir = getSegmentRemoteDir(topicPartition, baseOffset, lastOffset);

        File logFile = logSegment.log().file();
        File offsetIdxFile = logSegment.offsetIndex().file();
        File tsIdxFile = logSegment.timeIndex().file();

        FileSystem fs = getFS();

        List<RemoteLogIndexEntry> remoteIndex = buildRemoteIndex(logFile,
                fs.getUri().toString() + desDir);

        // mkdir -p <temporary directory>
        String tmpDir = String.format("%s-%d-%d", desDir, seqNum.incrementAndGet(), System.currentTimeMillis());
        Path tmpDirPath = new Path(tmpDir);
        fs.mkdirs(tmpDirPath);

        // copy local files to remote temporary directory
        Path logFilePath = getPath(tmpDir, LOG_FILE_NAME);
        fs.copyFromLocalFile(new Path(logFile.getAbsolutePath()), logFilePath);
        // keep the original mtime. we will use the mtime to calculate retention time
        fs.setTimes(logFilePath, logFile.lastModified(), System.currentTimeMillis());
        fs.copyFromLocalFile(new Path(offsetIdxFile.getAbsolutePath()), getPath(tmpDir, OFFSET_INDEX_FILE_NAME));
        fs.copyFromLocalFile(new Path(tsIdxFile.getAbsolutePath()), getPath(tmpDir, TIME_INDEX_FILE_NAME));

        // write remote index to remote storage
        try (FSDataOutputStream remoteIndexFile = fs.create(getPath(tmpDir, REMOTE_INDEX_FILE_NAME))) {
            try (WritableByteChannel remoteIndexFileChannel = Channels.newChannel(remoteIndexFile)) {
                for (RemoteLogIndexEntry entry : remoteIndex) {
                    remoteIndexFileChannel.write(entry.asBuffer());
                }
            }
        }

        Path destPath = new Path(desDir);
        // If the destination already exists, this method throws FileAlreadyExistsException
        if (!fs.rename(tmpDirPath, destPath)) {
            // failed to rename the destination dir
            throw new IOException(String.format("Failed to rename <%s> to <%s>", tmpDirPath.toUri(), destPath));
        } else if (fs.exists(new Path(desDir + "/" + tmpDirPath.getName()))) {
            // If the destination directory already exists, HDFS will move the source directory into
            // destination directory.
            fs.delete(new Path(desDir + "/" + tmpDirPath.getName()), true);
            throw new FileAlreadyExistsException(String.format("Directory <%s> already exists on HDFS.", desDir));
        }

        return remoteIndex;
    }

    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition) throws IOException {
        return listRemoteSegments(topicPartition, 0);
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition, long minOffset) throws IOException {

        FileSystem fs = getFS();
        Path path = new Path(getTPRemoteDir(topicPartition));
        if (!fs.exists(path)) {
            // if the path does not exist return an empty list.
            return Collections.emptyList();
        }

        ArrayList<RemoteLogSegmentInfo> segments = new ArrayList<>();
        FileStatus[] files = fs.listStatus(path);

        for (FileStatus file : files) {
            String segmentName = file.getPath().getName();
            Matcher m = REMOTE_SEGMENT_DIR_NAME_PATTERN.matcher(segmentName);
            if (m.matches()) {
                try {
                    long baseOffset = Long.parseLong(m.group(1));
                    long endOffset = Long.parseLong(m.group(2));
                    if (endOffset >= minOffset) {
                        //todo set the right leaderEpoch
                        int leaderEpoch = 0;
                        RemoteLogSegmentInfo segment = new RemoteLogSegmentInfo(baseOffset, endOffset, topicPartition,
                                leaderEpoch, Collections.singletonMap(FILE_PATH, file.getPath()));
                        segments.add(segment);
                    }
                } catch (NumberFormatException e) {
                    LOGGER.warn("Exception occurred while parsing segment file [{}] ", segmentName, e);
                }
            }
        }

        segments.sort(Comparator.comparingLong(RemoteLogSegmentInfo::baseOffset));
        return segments;
    }

    @Override
    public List<RemoteLogIndexEntry> getRemoteLogIndexEntries(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        FileSystem fs = getFS();

        File tmpFile = null;
        try {
            Path hdfsPath = (Path) remoteLogSegment.props().get(FILE_PATH);
            tmpFile = File.createTempFile("kafka-hdfs-rsm-" + hdfsPath.getName(), null);
            tmpFile.deleteOnExit();

            Path remoteIndexPath = getPath(hdfsPath.toString(), REMOTE_INDEX_FILE_NAME);
            try (InputStream is = fs.open(remoteIndexPath)) {
                return seqAsJavaListConverter(RemoteLogIndexEntry.readAll(is)).asJava();
            }
        } finally {
            if (tmpFile != null && tmpFile.exists()) {
                if (!tmpFile.delete()) {
                    // Make find bug happy
                }
            }
        }
    }

    @Override
    public boolean deleteLogSegment(RemoteLogSegmentInfo remoteLogSegmentInfo) throws IOException {
        FileSystem fs = getFS();
        return fs.delete((Path) remoteLogSegmentInfo.props().get(FILE_PATH), true);
    }

    @Override
    public boolean deleteTopicPartition(TopicPartition tp) throws IOException {
        FileSystem fs = getFS();
        Path path = new Path(getTPRemoteDir(tp));
        return fs.delete(path, true);
    }

    @Override
    public long cleanupLogUntil(TopicPartition topicPartition, long cleanUpTillMs) throws IOException {
        FileSystem fs = getFS();
        Path path = new Path(getTPRemoteDir(topicPartition));
        if (!fs.exists(path)) {
            // if there are no log segments available yet then return -1L
            return -1L;
        }

        FileStatus[] files = fs.listStatus(path);

        long minStartOffset = Long.MAX_VALUE;
        for (FileStatus file : files) {
            Path segDirPath = file.getPath();
            String segmentName = segDirPath.getName();
            Matcher m = REMOTE_SEGMENT_DIR_NAME_PATTERN.matcher(segmentName);
            if (m.matches()) {
                Path logFilePath = getPath(segDirPath.toString(), LOG_FILE_NAME);
                if (fs.exists(logFilePath) && fs.getFileStatus(logFilePath).getModificationTime() < cleanUpTillMs) {
                    fs.delete(segDirPath, true);
                } else {
                    minStartOffset = Math.min(minStartOffset, Long.parseLong(m.group(1)));
                }
            }
        }

        return minStartOffset == Long.MAX_VALUE ? -1L : minStartOffset;
    }

    /**
     * Read remote log from startOffset.
     **/
    @Override
    public Records read(RemoteLogIndexEntry remoteLogIndexEntry, int maxBytes, long startOffset, boolean minOneMessage) throws IOException {
        if (startOffset > remoteLogIndexEntry.lastOffset())
            throw new IllegalArgumentException("startOffset > remoteLogIndexEntry.lastOffset()");

        String rdi = new String(remoteLogIndexEntry.rdi(), StandardCharsets.UTF_8);
        Matcher m = RDI_PATTERN.matcher(rdi);

        if (!m.matches()) {
            throw new IllegalArgumentException(String.format("Can't parse RDI <%s>", rdi));
        }

        String path = m.group(1);
        int pos = Integer.parseInt(m.group(2));

        Path logFile = getPath(path, LOG_FILE_NAME);
        FileSystem fs = getFS();

        try (FSDataInputStream is = fs.open(logFile)) {
            // Find out the 1st batch that is not less than startOffset
            Records records = read(is, pos, remoteLogIndexEntry.dataLength());

            int batchPos = pos;
            RecordBatch firstBatch = null;
            for (RecordBatch batch : records.batches()) {
                if (batch.lastOffset() >= startOffset) {
                    firstBatch = batch;
                    break;
                }
                batchPos += batch.sizeInBytes();
            }

            if (firstBatch == null)
                return MemoryRecords.EMPTY;

            int fileLen = (int) fs.getFileStatus(logFile).getLen();
            int adjustedMaxBytes = Math.min(Math.max(maxBytes, firstBatch.sizeInBytes()), fileLen - batchPos);

            if (adjustedMaxBytes <= 0)
                return MemoryRecords.EMPTY;

            return read(is, batchPos, adjustedMaxBytes);
        }
    }

    private Records read(FSDataInputStream is, int pos, int maxBytes) throws IOException {
        byte[] buf = new byte[maxBytes];
        is.readFully(pos, buf);
        return MemoryRecords.readableRecords(ByteBuffer.wrap(buf));
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
        fsURI = URI.create(configs.get(HDFS_URI_PROP).toString());
        baseDir = configs.get(HDFS_BASE_DIR_PROP).toString();

        String intervalBytesStr = null;
        if (configs.containsKey(HDFS_REMOTE_INDEX_INTERVAL_BYTES))
            intervalBytesStr = configs.get(HDFS_REMOTE_INDEX_INTERVAL_BYTES).toString();

        if (intervalBytesStr == null || intervalBytesStr.isEmpty())
            intervalBytesStr = HDFS_REMOTE_INDEX_INTERVAL_BYTES_DEFAULT;
        indexIntervalBytes = Integer.parseInt(intervalBytesStr);

        hadoopConf = new Configuration(); // Load configuration from hadoop configuration files in class path
    }

    private FileSystem getFS() throws IOException {
        if (fs.get() == null) {
            fs.set(FileSystem.newInstance(fsURI, hadoopConf));
        }

        return fs.get();
    }

    private String getTPRemoteDir(TopicPartition tp) {
        return baseDir + "/" + tp.toString();
    }

    private String getSegmentRemoteDir(TopicPartition tp, long baseOffset, long nextOffset) {
        return getTPRemoteDir(tp) + "/" + String.format(REMOTE_LOG_DIR_FORMAT, baseOffset, nextOffset);
    }

    private List<RemoteLogIndexEntry> buildRemoteIndex(File logFile, String remoteLogFileUri) throws IOException {
        ArrayList<RemoteLogIndexEntry> index = new ArrayList<>();

        FileLogInputStream.FileChannelRecordBatch firstBatch = null; // the first Batch in the current index entry
        FileLogInputStream.FileChannelRecordBatch lastBatch = null; // the last Batch in the current index entry

        FileRecords messageSet = FileRecords.open(logFile);
        for (FileLogInputStream.FileChannelRecordBatch batch : messageSet.batches()) {
            if (firstBatch == null) {
                firstBatch = batch;
            }

            lastBatch = batch;

            long nextPos = lastBatch.position() + lastBatch.sizeInBytes();
            if (nextPos - firstBatch.position() > indexIntervalBytes) {
                index.add(makeRemoteLogIndexEntry(remoteLogFileUri, firstBatch, lastBatch));
                // start a new index entry
                firstBatch = null;
            }
        }

        if (firstBatch != null) {
            index.add(makeRemoteLogIndexEntry(remoteLogFileUri, firstBatch, lastBatch));
        }

        return index;
    }

    private RemoteLogIndexEntry makeRemoteLogIndexEntry(String remoteLogFileUri,
                                                        FileLogInputStream.FileChannelRecordBatch firstBatch,
                                                        FileLogInputStream.FileChannelRecordBatch lastBatch) {
        String rdi = remoteLogFileUri + ":" + firstBatch.position();
        return RemoteLogIndexEntry.apply(firstBatch.baseOffset(), lastBatch.lastOffset(), firstBatch.firstTimestamp(),
                lastBatch.maxTimestamp(), lastBatch.position() + lastBatch.sizeInBytes() - firstBatch.position(),
                rdi.getBytes(StandardCharsets.UTF_8));
    }

    private Path getPath(String dirPath, String fileName) {
        return new Path(dirPath + "/" + fileName);
    }
}

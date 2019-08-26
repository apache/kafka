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

import static kafka.log.remote.RemoteLogManager.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
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

import scala.collection.JavaConverters;

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

    private static AtomicInteger seqNum = new AtomicInteger(0);

    private long indexIntervalBytes;
    private URI fsURI = null;
    private String baseDir = null;
    private Configuration hadoopConf = null;
    private ThreadLocal<FileSystem> fs = new ThreadLocal<>();

    @Override
    public List<RemoteLogIndexEntry> copyLogSegment(TopicPartition topicPartition, LogSegment logSegment)
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
        fs.copyFromLocalFile(new Path(logFile.getAbsolutePath()), getPath(tmpDir, LOG_FILE_NAME));
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

    @Override
    public void cancelCopyingLogSegment(TopicPartition topicPartition) {
        // Not implemented
    }

    @Override
    public List<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition, long minBaseOffset) throws IOException {
        ArrayList<RemoteLogSegmentInfo> segments = new ArrayList<>();

        FileSystem fs = getFS();
        Path path = new Path(getTPRemoteDir(topicPartition));
        FileStatus[] files = fs.listStatus(path);

        for (FileStatus file : files) {
            String segmentName = file.getPath().getName();
            Matcher m = REMOTE_SEGMENT_DIR_NAME_PATTERN.matcher(segmentName);
            if (m.matches()) {
                try {
                    long baseOffset = Long.parseLong(m.group(1));
                    if (baseOffset >= minBaseOffset) {
                        long endOffset = Long.parseLong(m.group(2));
                        HDFSRemoteLogSegmentInfo segment = new HDFSRemoteLogSegmentInfo(baseOffset, endOffset, file.getPath());
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
        HDFSRemoteLogSegmentInfo segment = (HDFSRemoteLogSegmentInfo) remoteLogSegment;
        FileSystem fs = getFS();

        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("kafka-hdfs-rsm-" + segment.getPath().getName(), null);
            tmpFile.deleteOnExit();

            Path path = getPath(segment.getPath().toString(), REMOTE_INDEX_FILE_NAME);

            fs.copyToLocalFile(path, new Path(tmpFile.getAbsolutePath()));

            try (RandomAccessFile raFile = new RandomAccessFile(tmpFile, "r")) {
                FileChannel channel = raFile.getChannel();
                return JavaConverters.seqAsJavaList(RemoteLogIndexEntry.readAll(channel));
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
    public boolean deleteLogSegment(RemoteLogSegmentInfo remoteLogSegment) throws IOException {
        HDFSRemoteLogSegmentInfo segment = (HDFSRemoteLogSegmentInfo) remoteLogSegment;
        FileSystem fs = getFS();
        return fs.delete(segment.getPath(), true);
    }

    public boolean deleteTopicPartition(TopicPartition tp) {
        // todo
        return true;
    }

    @Override
    public boolean cleanupLogUntil(TopicPartition topicPartition, long cleanUpTillMs) {
        //todo
        return true;
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
        if (fs != null) {
            try {
                fs.close();
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

package org.apache.kafka.rsm.hdfs;

import kafka.log.remote.RemoteLogSegmentInfo;
import org.apache.hadoop.fs.Path;

public class HDFSRemoteLogSegmentInfo implements RemoteLogSegmentInfo {
    private long baseOffset;
    private long endOffset;
    private Path path;

    HDFSRemoteLogSegmentInfo(long baseOffset, long endOffset, Path path) {
        this.baseOffset = baseOffset;
        this.endOffset = endOffset;
        this.path = path;
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public long endOffset() {
        return endOffset;
    }

    public Path getPath() {
        return path;
    }
}

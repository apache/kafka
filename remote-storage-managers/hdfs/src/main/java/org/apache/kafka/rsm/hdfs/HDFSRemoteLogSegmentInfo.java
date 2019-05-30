package org.apache.kafka.rsm.hdfs;

import kafka.log.remote.RemoteLogSegmentInfo;

public class HDFSRemoteLogSegmentInfo implements RemoteLogSegmentInfo {
    @Override
    public long baseOffset() {
        return 0;
    }

    @Override
    public long endOffset() {
        return 0;
    }
}

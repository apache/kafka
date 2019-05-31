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

import kafka.log.LogReadInfo;
import kafka.log.LogSegment;
import kafka.log.remote.RDI;
import kafka.log.remote.RemoteLogIndexEntry;
import kafka.log.remote.RemoteLogSegmentInfo;
import kafka.log.remote.RemoteStorageManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Records;
import scala.Tuple2;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class HDFSRemoteStorageManager implements RemoteStorageManager {
    private URI baseURI = null;
    private Configuration hadoopConf = null;
    private FileSystem fs = null;

    @Override
    public Tuple2<RDI, Seq<RemoteLogIndexEntry>> copyLogSegment(TopicPartition topicPartition, LogSegment logSegment) {
        return null;
    }

    @Override
    public void cancelCopyingLogSegment(TopicPartition topicPartition) {
    }

    @Override
    public Seq<RemoteLogSegmentInfo> listRemoteSegments(TopicPartition topicPartition) {
        return null;
    }

    @Override
    public Seq<RemoteLogIndexEntry> getRemoteLogIndexEntries(RemoteLogSegmentInfo remoteLogSegment) {
        return null;
    }

    @Override
    public boolean deleteLogSegment(RemoteLogSegmentInfo remoteLogSegment) {
        return false;
    }

    @Override
    public Records read(RDI remoteLocation, int maxBytes, long offset) {
        return null;
    }

    @Override
    public void close() {

    }

    /**
     * Initialize this instance with the given configs
     *
     * @param configs Key-Value pairs of configuration parameters
     */
    @Override
    public void configure(Map<String, ?> configs) {
        baseURI = URI.create("http://localhost:9000/cluster"); // TODO: make this configurable
        hadoopConf = new Configuration(); // Load configuration from hadoop configuration files in class path
        try {
            fs = FileSystem.get(baseURI, hadoopConf);
        }
        catch (IOException e) {
            // TODO:
        }
    }
}

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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static kafka.log.remote.RemoteLogManager.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.*;
import static org.apache.kafka.common.config.ConfigDef.Importance.*;

class HDFSRemoteStorageManagerConfig extends AbstractConfig {
    public static final String HDFS_URI_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.fs.uri";
    public static final String HDFS_URI_DOC = "URI of of the HDFS cluster.";

    public static final String HDFS_BASE_DIR_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.base.dir";
    public static final String HDFS_BASE_DIR_DOC = "The HDFS directory in which the remote data is stored.";

    public static final String HDFS_REMOTE_INDEX_INTERVAL_BYTES_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.remote.index.interval.bytes";
    public static final String HDFS_REMOTE_INDEX_INTERVAL_BYTES_DOC = "The interval with which we add an entry to the remote index.";

    public static final String HDFS_REMOTE_READ_MB_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.remote.read.bytes.mb";
    public static final String HDFS_REMOTE_READ_MB_DOC = "HDFS read buffer size in MB.";

    public static final String HDFS_REMOTE_READ_CACHE_MB_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX() + "hdfs.remote.read.cache.mb";
    public static final String HDFS_REMOTE_READ_CACHE_MB_DOC = "Read cache size in MB. The maximum amount of remote data will be cached in broker's memory. " +
                                                               "This value must be larger than hdfs.remote.read.bytes.mb.";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
            .define(HDFS_URI_PROP, STRING, HIGH, HDFS_URI_DOC)
            .define(HDFS_BASE_DIR_PROP, STRING, HIGH, HDFS_BASE_DIR_DOC)
            .define(HDFS_REMOTE_INDEX_INTERVAL_BYTES_PROP, INT, 262144, atLeast(1), MEDIUM, HDFS_REMOTE_INDEX_INTERVAL_BYTES_DOC)
            .define(HDFS_REMOTE_READ_MB_PROP, INT, 16, atLeast(1), MEDIUM, HDFS_REMOTE_READ_MB_DOC)
            .define(HDFS_REMOTE_READ_CACHE_MB_PROP, INT, 1024, atLeast(1), MEDIUM, HDFS_REMOTE_READ_CACHE_MB_DOC);
    }

    HDFSRemoteStorageManagerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }
}

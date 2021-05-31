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

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

class HDFSRemoteStorageManagerConfig extends AbstractConfig {

    private static final String REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "remote.log.storage.";

    public static final String HDFS_URI_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "hdfs.fs.uri";
    public static final String HDFS_URI_DOC = "URI of of the HDFS cluster.";

    public static final String HDFS_BASE_DIR_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "hdfs.base.dir";
    public static final String HDFS_BASE_DIR_DOC = "The HDFS directory in which the remote data is stored.";

    public static final String HDFS_REMOTE_READ_BYTES_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "hdfs.remote.read.bytes";
    public static final String HDFS_REMOTE_READ_BYTES_DOC = "HDFS read buffer size in bytes.";
    public static final int DEFAULT_HDFS_REMOTE_READ_BYTES = 16 * 1024 * 1024;

    public static final String HDFS_REMOTE_READ_CACHE_BYTES_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "hdfs.remote.read.cache.bytes";
    public static final String HDFS_REMOTE_READ_CACHE_BYTES_DOC = "Read cache size in bytes. " +
            "The maximum amount of remote data will be cached in broker's memory. " +
            "This value must be larger than " + HDFS_REMOTE_READ_BYTES_PROP;
    public static final long DEFAULT_HDFS_REMOTE_READ_CACHE_BYTES = 1024 * 1024 * 1024L;

    public static final String HDFS_USER_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "hdfs.user";
    public static final String HDFS_KEYTAB_PATH_PROP = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "hdfs.keytab.path";

    public static final String HDFS_USER_DOC = "The principal name to load from the keytab. " +
            "This property should be used with " + HDFS_KEYTAB_PATH_PROP;
    public static final String HDFS_KEYTAB_PATH_DOC = "The path to the keytab file. " +
            "This property should be used with " + HDFS_USER_PROP;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
            .define(HDFS_URI_PROP, STRING, HIGH, HDFS_URI_DOC)
            .define(HDFS_BASE_DIR_PROP, STRING, HIGH, HDFS_BASE_DIR_DOC)
            .define(HDFS_USER_PROP, STRING, null, new ConfigDef.NonEmptyString(), MEDIUM, HDFS_USER_DOC)
            .define(HDFS_KEYTAB_PATH_PROP, STRING, null, new ConfigDef.NonEmptyString(), MEDIUM, HDFS_KEYTAB_PATH_DOC)
            .define(HDFS_REMOTE_READ_BYTES_PROP, INT, DEFAULT_HDFS_REMOTE_READ_BYTES, atLeast(1048576), MEDIUM, HDFS_REMOTE_READ_BYTES_DOC)
            .define(HDFS_REMOTE_READ_CACHE_BYTES_PROP, LONG, DEFAULT_HDFS_REMOTE_READ_CACHE_BYTES, atLeast(1048576), MEDIUM, HDFS_REMOTE_READ_CACHE_BYTES_DOC);
    }

    HDFSRemoteStorageManagerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

}

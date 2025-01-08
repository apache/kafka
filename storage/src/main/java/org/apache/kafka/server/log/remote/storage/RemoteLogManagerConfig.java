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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public final class RemoteLogManagerConfig {

    /**
     * Prefix used for properties to be passed to {@link RemoteStorageManager} implementation. Remote log subsystem collects all the properties having
     * this prefix and passes to {@code RemoteStorageManager} using {@link RemoteStorageManager#configure(Map)}.
     */
    public static final String REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP = "remote.log.storage.manager.impl.prefix";
    public static final String REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_DOC = "Prefix used for properties to be passed to RemoteStorageManager " +
            "implementation. For example this value can be `rsm.config.`.";
    public static final String DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "rsm.config.";

    /**
     * Prefix used for properties to be passed to {@link RemoteLogMetadataManager} implementation. Remote log subsystem collects all the properties having
     * this prefix and passed to {@code RemoteLogMetadataManager} using {@link RemoteLogMetadataManager#configure(Map)}.
     */
    public static final String REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP = "remote.log.metadata.manager.impl.prefix";
    public static final String REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_DOC = "Prefix used for properties to be passed to RemoteLogMetadataManager " +
            "implementation. For example this value can be `rlmm.config.`.";
    public static final String DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX = "rlmm.config.";

    public static final String REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP = "remote.log.storage.system.enable";
    public static final String REMOTE_LOG_STORAGE_SYSTEM_ENABLE_DOC = "Whether to enable tiered storage functionality in a broker or not. " +
            "When it is true broker starts all the services required for the tiered storage functionality.";
    public static final boolean DEFAULT_REMOTE_LOG_STORAGE_SYSTEM_ENABLE = false;

    public static final String REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP = "remote.log.storage.manager.class.name";
    public static final String REMOTE_STORAGE_MANAGER_CLASS_NAME_DOC = "Fully qualified class name of `RemoteStorageManager` implementation.";

    public static final String REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP = "remote.log.storage.manager.class.path";
    public static final String REMOTE_STORAGE_MANAGER_CLASS_PATH_DOC = "Class path of the `RemoteStorageManager` implementation. " +
            "If specified, the RemoteStorageManager implementation and its dependent libraries will be loaded by a dedicated " +
            "classloader which searches this class path before the Kafka broker class path. The syntax of this parameter is same " +
            "as the standard Java class path string.";

    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP = "remote.log.metadata.manager.class.name";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_DOC = "Fully qualified class name of `RemoteLogMetadataManager` implementation.";
    public static final String DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME = "org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager";

    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP = "remote.log.metadata.manager.class.path";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_DOC = "Class path of the `RemoteLogMetadataManager` implementation. " +
            "If specified, the RemoteLogMetadataManager implementation and its dependent libraries will be loaded by a dedicated " +
            "classloader which searches this class path before the Kafka broker class path. The syntax of this parameter is same " +
            "as the standard Java class path string.";

    public static final String REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP = "remote.log.metadata.manager.listener.name";
    public static final String REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_DOC = "Listener name of the local broker to which it should get connected if " +
            "needed by RemoteLogMetadataManager implementation.";

    public static final String REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_PROP = "remote.log.metadata.custom.metadata.max.bytes";
    public static final String REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_DOC = "The maximum size of custom metadata in bytes that the broker " +
            "should accept from a remote storage plugin. If custom  metadata exceeds this limit, the updated segment metadata " +
            "will not be stored, the copied data will be attempted to delete, " +
            "and the remote copying task for this topic-partition will stop with an error.";
    public static final int DEFAULT_REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES = 128;

    public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP = "remote.log.index.file.cache.total.size.bytes";
    public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_DOC = "The total size of the space allocated to store index files fetched " +
            "from remote storage in the local storage.";
    public static final long DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES = 1024 * 1024 * 1024L;

    public static final String REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP = "remote.log.manager.thread.pool.size";
    public static final String REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_DOC = "Size of the thread pool used in scheduling follower tasks to read " +
            "the highest-uploaded remote-offset for follower partitions.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE = 2;

    public static final String REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP = "remote.log.manager.copier.thread.pool.size";
    public static final String REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_DOC = "Size of the thread pool used in scheduling tasks " +
            "to copy segments.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE = 10;

    public static final String REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP = "remote.log.manager.expiration.thread.pool.size";
    public static final String REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_DOC = "Size of the thread pool used in scheduling tasks " +
            "to clean up the expired remote log segments.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE = 10;
    
    public static final String REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP = "remote.log.manager.task.interval.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_DOC = "Interval at which remote log manager runs the scheduled tasks like copy " +
            "segments, and clean up remote log segments.";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS = 30 * 1000L;

    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP = "remote.log.manager.task.retry.backoff.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_DOC = "The initial amount of wait in milliseconds before the request is retried again.";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS = 500L;

    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP = "remote.log.manager.task.retry.backoff.max.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_DOC = "The maximum amount of time in milliseconds to wait when the request " +
            "is retried again. The retry duration will increase exponentially for each request failure up to this maximum wait interval.";
    public static final long DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS = 30 * 1000L;

    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP = "remote.log.manager.task.retry.jitter";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_DOC = "The value used in defining the range for computing random jitter factor. " +
            "It is applied to the effective exponential term for computing the resultant retry backoff interval. This will avoid thundering herds " +
            "of requests. The default value is 0.2 and valid value should be between 0(inclusive) and 0.5(inclusive). " +
            "For ex: remote.log.manager.task.retry.jitter = 0.25, then the range to compute random jitter will be [1-0.25, 1+0.25) viz [0.75, 1.25). " +
            "So, jitter factor can be any random value with in that range.";
    public static final double DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_JITTER = 0.2;

    public static final String REMOTE_LOG_READER_THREADS_PROP = "remote.log.reader.threads";
    public static final String REMOTE_LOG_READER_THREADS_DOC = "Size of the thread pool that is allocated for handling remote log reads.";
    public static final int DEFAULT_REMOTE_LOG_READER_THREADS = 10;

    public static final String REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP = "remote.log.reader.max.pending.tasks";
    public static final String REMOTE_LOG_READER_MAX_PENDING_TASKS_DOC = "Maximum remote log reader thread pool task queue size. If the task queue " +
            "is full, fetch requests are served with an error.";
    public static final int DEFAULT_REMOTE_LOG_READER_MAX_PENDING_TASKS = 100;

    public static final String LOG_LOCAL_RETENTION_MS_PROP = "log.local.retention.ms";
    public static final String LOG_LOCAL_RETENTION_MS_DOC = "The number of milliseconds to keep the local log segments before it gets eligible for deletion. " +
            "Default value is -2, it represents `log.retention.ms` value is to be used. The effective value should always be less than or equal " +
            "to `log.retention.ms` value.";
    public static final Long DEFAULT_LOG_LOCAL_RETENTION_MS = -2L;

    public static final String LOG_LOCAL_RETENTION_BYTES_PROP = "log.local.retention.bytes";
    public static final String LOG_LOCAL_RETENTION_BYTES_DOC = "The maximum size of local log segments that can grow for a partition before it gets eligible for deletion. " +
            "Default value is -2, it represents `log.retention.bytes` value to be used. The effective value should always be " +
            "less than or equal to `log.retention.bytes` value.";
    public static final Long DEFAULT_LOG_LOCAL_RETENTION_BYTES = -2L;

    public static final String REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP = "remote.log.manager.copy.max.bytes.per.second";
    public static final String REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_DOC = "The maximum number of bytes that can be copied from local storage to remote storage per second. " +
            "This is a global limit for all the partitions that are being copied from local storage to remote storage. " +
            "The default value is Long.MAX_VALUE, which means there is no limit on the number of bytes that can be copied per second.";
    public static final Long DEFAULT_REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND = Long.MAX_VALUE;

    public static final String REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM_PROP = "remote.log.manager.copy.quota.window.num";
    public static final String REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM_DOC = "The number of samples to retain in memory for remote copy quota management. " +
            "The default value is 11, which means there are 10 whole windows + 1 current window.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM = 11;

    public static final String REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS_PROP = "remote.log.manager.copy.quota.window.size.seconds";
    public static final String REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for remote copy quota management. " +
            "The default value is 1 second.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS = 1;

    public static final String REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP = "remote.log.manager.fetch.max.bytes.per.second";
    public static final String REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_DOC = "The maximum number of bytes that can be fetched from remote storage to local storage per second. " +
            "This is a global limit for all the partitions that are being fetched from remote storage to local storage. " +
            "The default value is Long.MAX_VALUE, which means there is no limit on the number of bytes that can be fetched per second.";
    public static final Long DEFAULT_REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND = Long.MAX_VALUE;

    public static final String REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM_PROP = "remote.log.manager.fetch.quota.window.num";
    public static final String REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM_DOC = "The number of samples to retain in memory for remote fetch quota management. " +
            "The default value is 11, which means there are 10 whole windows + 1 current window.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM = 11;

    public static final String REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP = "remote.log.manager.fetch.quota.window.size.seconds";
    public static final String REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for remote fetch quota management. " +
            "The default value is 1 second.";
    public static final int DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS = 1;

    public static final String REMOTE_FETCH_MAX_WAIT_MS_PROP = "remote.fetch.max.wait.ms";
    public static final String REMOTE_FETCH_MAX_WAIT_MS_DOC = "The maximum amount of time the server will wait before answering the remote fetch request";
    public static final int DEFAULT_REMOTE_FETCH_MAX_WAIT_MS = 500;

    public static final String REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP = "remote.list.offsets.request.timeout.ms";
    public static final String REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_DOC = "The maximum amount of time the server will wait for the remote list offsets request to complete.";
    public static final long DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS = 30000L;

    private final AbstractConfig config;

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP,
                        BOOLEAN,
                        DEFAULT_REMOTE_LOG_STORAGE_SYSTEM_ENABLE,
                        null,
                        MEDIUM,
                        REMOTE_LOG_STORAGE_SYSTEM_ENABLE_DOC)
                .define(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP,
                        STRING,
                        DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX,
                        new ConfigDef.NonEmptyString(),
                        MEDIUM,
                        REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_DOC)
                .define(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP,
                        STRING,
                        DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX,
                        new ConfigDef.NonEmptyString(),
                        MEDIUM,
                        REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_DOC)
                .define(REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        MEDIUM,
                        REMOTE_STORAGE_MANAGER_CLASS_NAME_DOC)
                .define(REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP, STRING,
                        null,
                        null,
                        MEDIUM,
                        REMOTE_STORAGE_MANAGER_CLASS_PATH_DOC)
                .define(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                        STRING,
                        DEFAULT_REMOTE_LOG_METADATA_MANAGER_CLASS_NAME,
                        new ConfigDef.NonEmptyString(),
                        MEDIUM,
                        REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_DOC)
                .define(REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP,
                        STRING,
                        null,
                        null,
                        MEDIUM,
                        REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_DOC)
                .define(REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        MEDIUM,
                        REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_DOC)
                .define(REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES,
                        atLeast(0),
                        LOW,
                        REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_DOC)
                .define(REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES,
                        atLeast(1),
                        LOW,
                        REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_DOC)
                .define(REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_MANAGER_THREAD_POOL_SIZE,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_DOC)
                .define(REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_DOC)
                .define(REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_DOC)
                .define(REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS,
                        atLeast(1),
                        LOW,
                        REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_DOC)
                .defineInternal(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS,
                        atLeast(1),
                        LOW,
                        REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_DOC)
                .defineInternal(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS,
                        atLeast(1), LOW,
                        REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_DOC)
                .defineInternal(REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP,
                        DOUBLE,
                        DEFAULT_REMOTE_LOG_MANAGER_TASK_RETRY_JITTER,
                        between(0, 0.5),
                        LOW,
                        REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_DOC)
                .define(REMOTE_LOG_READER_THREADS_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_READER_THREADS,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_READER_THREADS_DOC)
                .define(REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_READER_MAX_PENDING_TASKS,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_READER_MAX_PENDING_TASKS_DOC)
                .define(LOG_LOCAL_RETENTION_MS_PROP,
                        LONG,
                        DEFAULT_LOG_LOCAL_RETENTION_MS,
                        atLeast(DEFAULT_LOG_LOCAL_RETENTION_MS),
                        MEDIUM,
                        LOG_LOCAL_RETENTION_MS_DOC)
                .define(LOG_LOCAL_RETENTION_BYTES_PROP,
                        LONG,
                        DEFAULT_LOG_LOCAL_RETENTION_BYTES,
                        atLeast(DEFAULT_LOG_LOCAL_RETENTION_BYTES),
                        MEDIUM,
                        LOG_LOCAL_RETENTION_BYTES_DOC)
                .define(REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_DOC)
                .define(REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM_DOC)
                .define(REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS_DOC)
                .define(REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP,
                        LONG,
                        DEFAULT_REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_DOC)
                .define(REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM_DOC)
                .define(REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP,
                        INT,
                        DEFAULT_REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS_DOC)
                .define(REMOTE_FETCH_MAX_WAIT_MS_PROP,
                        INT,
                        DEFAULT_REMOTE_FETCH_MAX_WAIT_MS,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_FETCH_MAX_WAIT_MS_DOC)
                .define(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP,
                        LONG,
                        DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
                        atLeast(1),
                        MEDIUM,
                        REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_DOC);
    }
    
    public RemoteLogManagerConfig(AbstractConfig config) {
        this.config = config;
    }

    public boolean isRemoteStorageSystemEnabled() {
        return config.getBoolean(REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP);
    }

    public String remoteStorageManagerClassName() {
        return config.getString(REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP);
    }

    public String remoteStorageManagerClassPath() {
        return config.getString(REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP);
    }

    public String remoteLogMetadataManagerClassName() {
        return config.getString(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP);
    }

    public String remoteLogMetadataManagerClassPath() {
        return config.getString(REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP);
    }

    public int remoteLogManagerThreadPoolSize() {
        return config.getInt(REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP);
    }

    public int remoteLogManagerCopierThreadPoolSize() {
        int size = config.getInt(REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP);
        return size == -1 ? remoteLogManagerThreadPoolSize() : size;
    }

    public int remoteLogManagerExpirationThreadPoolSize() {
        int size = config.getInt(REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP);
        return size == -1 ? remoteLogManagerThreadPoolSize() : size;
    }

    public long remoteLogManagerTaskIntervalMs() {
        return config.getLong(REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP);
    }

    public long remoteLogManagerTaskRetryBackoffMs() {
        return config.getLong(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP);
    }

    public long remoteLogManagerTaskRetryBackoffMaxMs() {
        return config.getLong(REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP);
    }

    public double remoteLogManagerTaskRetryJitter() {
        return config.getDouble(REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP);
    }

    public int remoteLogReaderThreads() {
        return config.getInt(REMOTE_LOG_READER_THREADS_PROP);
    }

    public int remoteLogReaderMaxPendingTasks() {
        return config.getInt(REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP);
    }

    public String remoteLogMetadataManagerListenerName() {
        return config.getString(REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP);
    }

    public int remoteLogMetadataCustomMetadataMaxBytes() {
        return config.getInt(REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES_PROP);
    }

    public String remoteStorageManagerPrefix() {
        return config.getString(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP);
    }

    public String remoteLogMetadataManagerPrefix() {
        return config.getString(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP);
    }

    public Map<String, Object> remoteStorageManagerProps() {
        return getConfigProps(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP);
    }

    public Map<String, Object> remoteLogMetadataManagerProps() {
        return getConfigProps(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP);
    }

    public Map<String, Object> getConfigProps(String configPrefixProp) {
        String prefixProp = config.getString(configPrefixProp);
        return prefixProp == null ? Collections.emptyMap() : Collections.unmodifiableMap(config.originalsWithPrefix(prefixProp));
    }

    public int remoteLogManagerCopyNumQuotaSamples() {
        return config.getInt(REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_NUM_PROP);
    }

    public int remoteLogManagerCopyQuotaWindowSizeSeconds() {
        return config.getInt(REMOTE_LOG_MANAGER_COPY_QUOTA_WINDOW_SIZE_SECONDS_PROP);
    }

    public int remoteLogManagerFetchNumQuotaSamples() {
        return config.getInt(REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_NUM_PROP);
    }

    public int remoteLogManagerFetchQuotaWindowSizeSeconds() {
        return config.getInt(REMOTE_LOG_MANAGER_FETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP);
    }

    public long remoteLogIndexFileCacheTotalSizeBytes() {
        return config.getLong(REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP);
    }

    public long remoteLogManagerCopyMaxBytesPerSecond() {
        return config.getLong(REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP);
    }

    public long remoteLogManagerFetchMaxBytesPerSecond() {
        return config.getLong(REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP);
    }

    public int remoteFetchMaxWaitMs() {
        return config.getInt(REMOTE_FETCH_MAX_WAIT_MS_PROP);
    }

    public long logLocalRetentionBytes() {
        return config.getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP);
    }

    public long logLocalRetentionMs() {
        return config.getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP);
    }

    public long remoteListOffsetsRequestTimeoutMs() {
        return config.getLong(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP);
    }

    public static void main(String[] args) {
        System.out.println(configDef().toHtml(4, config -> "remote_log_manager_" + config));
    }
}

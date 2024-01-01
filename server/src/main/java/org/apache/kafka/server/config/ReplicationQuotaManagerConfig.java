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
package org.apache.kafka.server.config;

public class ReplicationQuotaManagerConfig {
    public static final long DEFAULT_QUOTA_BYTES_PER_SECOND = Long.MAX_VALUE;
    // Always have 10 whole windows + 1 current window
    public static final int DEFAULT_NUM_QUOTA_SAMPLES = 11;
    public static final int DEFAULT_QUOTA_WINDOW_SIZE_SECONDS = 1;
    // Purge sensors after 1 hour of inactivity
    public static final int INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS = 3600;
    private final long quotaBytesPerSecondDefault;
    private final int numQuotaSamples;
    private final int quotaWindowSizeSeconds;

    /**
     * Configuration settings for replication quota management
     *
     * @param quotaBytesPerSecondDefault The default bytes per second quota allocated to internal replication
     * @param numQuotaSamples            The number of samples to retain in memory
     * @param quotaWindowSizeSeconds     The time span of each sample
     */
    public ReplicationQuotaManagerConfig(long quotaBytesPerSecondDefault, int numQuotaSamples, int quotaWindowSizeSeconds) {
        this.quotaBytesPerSecondDefault = quotaBytesPerSecondDefault;
        this.numQuotaSamples = numQuotaSamples;
        this.quotaWindowSizeSeconds = quotaWindowSizeSeconds;
    }

    public ReplicationQuotaManagerConfig(int numQuotaSamples, int quotaWindowSizeSeconds) {
        this.quotaBytesPerSecondDefault = DEFAULT_QUOTA_BYTES_PER_SECOND;
        this.numQuotaSamples = numQuotaSamples;
        this.quotaWindowSizeSeconds = quotaWindowSizeSeconds;
    }


    public ReplicationQuotaManagerConfig() {
        this.quotaBytesPerSecondDefault = DEFAULT_QUOTA_BYTES_PER_SECOND;
        this.numQuotaSamples = DEFAULT_NUM_QUOTA_SAMPLES;
        this.quotaWindowSizeSeconds = DEFAULT_QUOTA_WINDOW_SIZE_SECONDS;
    }
    public long getQuotaBytesPerSecondDefault() {
        return quotaBytesPerSecondDefault;
    }

    public int getNumQuotaSamples() {
        return numQuotaSamples;
    }

    public int getQuotaWindowSizeSeconds() {
        return quotaWindowSizeSeconds;
    }
}

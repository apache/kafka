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
public class ClientQuotaManagerConfig {
    public final int numQuotaSamples;
    public final int quotaWindowSizeSeconds;

    /**
     * Configuration settings for quota management
     *
     * @param numQuotaSamples         The number of samples to retain in memory
     * @param quotaWindowSizeSeconds  The time span of each sample
     */
    public ClientQuotaManagerConfig(int numQuotaSamples, int quotaWindowSizeSeconds) {
        this.numQuotaSamples = numQuotaSamples;
        this.quotaWindowSizeSeconds = quotaWindowSizeSeconds;
    }

    public ClientQuotaManagerConfig() {
        this(QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT, QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT);
    }

    public ClientQuotaManagerConfig(int numQuotaSamples) {
        this(numQuotaSamples, QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT);
    }
}

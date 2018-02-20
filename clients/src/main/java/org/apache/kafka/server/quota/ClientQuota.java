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
package org.apache.kafka.server.quota;

import java.util.Map;

/**
 * Client quota returned by {@link ClientQuotaCallback} that includes the quota bound
 * as well as the metric tags that indicate which other clients share this quota.
 */
public class ClientQuota {
    private final double quotaLimit;
    private final Map<String, String> metricTags;

    /**
     * Constructs an instance of ClientQuota.
     *
     * @param quotaLimit The quota bound to be applied
     * @param metricTags The tags to be added to the quota metric for this request. All
     *                   entities which have the same `metricTags` share the `quotaLimit`
     */
    public ClientQuota(double quotaLimit, Map<String, String> metricTags) {
        this.quotaLimit = quotaLimit;
        this.metricTags = metricTags;
    }

    /**
     * Returns the quota bound.
     */
    public double quotaLimit() {
        return quotaLimit;
    }

    /**
     * Returns the tags to be added to the quota metric for this request. All
     * entities which have the same `metricTags` share a quota.
     */
    public Map<String, String> metricTags() {
        return metricTags;
    }
}

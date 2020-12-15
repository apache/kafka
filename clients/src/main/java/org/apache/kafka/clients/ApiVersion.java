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
package org.apache.kafka.clients;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.protocol.ApiKeys;

/**
 * Represents the min version and max version of an api key.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public class ApiVersion {
    public final short apiKey;
    public final short minVersion;
    public final short maxVersion;

    public ApiVersion(ApiKeys apiKey) {
        this(apiKey.id, apiKey.oldestVersion(), apiKey.latestVersion());
    }

    public ApiVersion(short apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public ApiVersion(ApiVersionsResponseKey apiVersionsResponseKey) {
        this.apiKey = apiVersionsResponseKey.apiKey();
        this.minVersion = apiVersionsResponseKey.minVersion();
        this.maxVersion = apiVersionsResponseKey.maxVersion();
    }

    @Override
    public String toString() {
        return "ApiVersion(" +
            "apiKey=" + apiKey +
            ", minVersion=" + minVersion +
            ", maxVersion= " + maxVersion +
            ")";
    }
}

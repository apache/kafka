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
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.metrics.Metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsUtils {
    /**
     * Converts the provided time from milliseconds to the requested
     * time unit.
     */
    public static double convert(long timeMs, TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                return timeMs * 1000.0 * 1000.0;
            case MICROSECONDS:
                return timeMs * 1000.0;
            case MILLISECONDS:
                return timeMs;
            case SECONDS:
                return timeMs / 1000.0;
            case MINUTES:
                return timeMs / (60.0 * 1000.0);
            case HOURS:
                return timeMs / (60.0 * 60.0 * 1000.0);
            case DAYS:
                return timeMs / (24.0 * 60.0 * 60.0 * 1000.0);
            default:
                throw new IllegalStateException("Unknown unit: " + unit);
        }
    }

    /**
     * Create a set of tags using the supplied key and value pairs. The order of the tags will be kept.
     *
     * @param keyValue the key and value pairs for the tags; must be an even number
     * @return the map of tags that can be supplied to the {@link Metrics} methods; never null
     */
    public static Map<String, String> getTags(String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Map<String, String> tags = new LinkedHashMap<>(keyValue.length / 2);

        for (int i = 0; i < keyValue.length; i += 2)
            tags.put(keyValue[i], keyValue[i + 1]);
        return tags;
    }
}

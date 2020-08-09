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
}

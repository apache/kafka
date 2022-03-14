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
package org.apache.kafka.clients.telemetry;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;

/**
 * <code>DeltaValueStore</code> is a simple encapsulation that stores a {@link Long} value for a
 * {@link MetricName}. The value at push cycle P(n) is stored in the table. At push cycle P(n+1),
 * the current value is stored and the value at P(n) is returned. Thus, the difference between the
 * values P(n+1)-P(n) can be determined.
 */

public class DeltaValueStore {

    private final Map<MetricName, Long> previousValuesMap;

    public DeltaValueStore() {
        previousValuesMap = new HashMap<>();
    }

    public void remove(MetricName metricName) {
        previousValuesMap.remove(metricName);
    }

    public Long getAndSet(MetricName metricName, Long value) {
        return previousValuesMap.put(metricName, value);
    }

}

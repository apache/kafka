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
package kafka.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ClientMetricsTestUtils {

    public static final String DEFAULT_METRICS =
        "org.apache.kafka.client.producer.partition.queue.,org.apache.kafka.client.producer.partition.latency";
    public static final int DEFAULT_PUSH_INTERVAL_MS = 30 * 1000; // 30 seconds
    public static final List<String> DEFAULT_CLIENT_MATCH_PATTERNS = Collections.unmodifiableList(Arrays.asList(
        ClientMetricsConfigs.CLIENT_SOFTWARE_NAME + "=apache-kafka-java",
        ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION + "=3.5.*"
    ));

    public static Properties defaultProperties() {
        Properties props = new Properties();
        props.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, DEFAULT_METRICS);
        props.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, Integer.toString(DEFAULT_PUSH_INTERVAL_MS));
        props.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, String.join(",", DEFAULT_CLIENT_MATCH_PATTERNS));
        return props;
    }
}

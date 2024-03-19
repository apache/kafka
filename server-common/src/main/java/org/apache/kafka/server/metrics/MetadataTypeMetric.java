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

package org.apache.kafka.server.metrics;

/**
 * This metric is reported by broker and controllers in both KRaft and ZK modes. For brokers,
 * it is included under the KafkaServer mbean. For controllers, it is under the KafkaController
 * mbean. This metric was defined in KIP-866
 */
public final class MetadataTypeMetric {
    public static final String METRIC_NAME = "MetadataType";

    public static final int ZOOKEEPER = 1;
    public static final int KRAFT = 2;
    public static final int DUAL_WRITE = 3;
}

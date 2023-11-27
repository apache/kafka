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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Timer;

/**
 * This extension interface provides a handful of methods to expose internals of the {@link Consumer} for
 * various tests.
 *
 * <p/>
 *
 * <em>Note</em>: this is for internal use only and is not intended for use by end users. Internal users should
 * not attempt to determine the underlying implementation to avoid coding to an unstable interface. Rather, it is
 * the {@link Consumer} API contract that should serve as the caller's interface.
 */
public interface ConsumerDelegate<K, V> extends Consumer<K, V> {

    String clientId();

    Metrics metricsRegistry();

    KafkaConsumerMetrics kafkaConsumerMetrics();

    boolean updateAssignmentMetadataIfNeeded(final Timer timer);
}

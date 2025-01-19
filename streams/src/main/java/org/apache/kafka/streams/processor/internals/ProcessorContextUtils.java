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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Map;

/**
 * This class bridges the gap for components that _should_ be compatible with
 * the public ProcessorContext interface, but have come to depend on features
 * in InternalProcessorContext. In theory, all the features adapted here could
 * migrate to the public interface, so each method in this class should reference
 * the ticket that would ultimately obviate it.
 */
public final class ProcessorContextUtils {

    private ProcessorContextUtils() {}

    /**
     * Should be removed as part of KAFKA-10217
     */
    public static StreamsMetricsImpl metricsImpl(final ProcessorContext context) {
        return (StreamsMetricsImpl) context.metrics();
    }

    /**
     * Should be removed as part of KAFKA-10217
     */
    public static StreamsMetricsImpl metricsImpl(final StateStoreContext context) {
        return (StreamsMetricsImpl) context.metrics();
    }

    public static String changelogFor(final StateStoreContext context, final String storeName, final Boolean newChangelogTopic) {
        final String prefix = topicNamePrefix(context.appConfigs(), context.applicationId());
        if (context instanceof InternalProcessorContext && !newChangelogTopic) {
            final String changelogTopic = ((InternalProcessorContext<?, ?>) context).changelogFor(storeName);
            if (changelogTopic != null)
                return changelogTopic;

        }
        return ProcessorStateManager.storeChangelogTopic(prefix, storeName, context.taskId().topologyName());
    }

    public static String topicNamePrefix(final Map<String, Object> configs, final String applicationId) {
        if (configs == null) {
            return applicationId;
        } else {
            return StreamsConfig.InternalConfig.getString(
                configs,
                StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE,
                applicationId
            );
        }
    }

    public static InternalProcessorContext asInternalProcessorContext(final ProcessorContext context) {
        if (context instanceof InternalProcessorContext) {
            return (InternalProcessorContext) context;
        } else {
            throw new IllegalArgumentException(
                "This component requires internal features of Kafka Streams and must be disabled for unit tests."
            );
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> InternalProcessorContext<K, V> asInternalProcessorContext(final StateStoreContext context) {
        if (context instanceof InternalProcessorContext) {
            return (InternalProcessorContext<K, V>) context;
        } else {
            throw new IllegalArgumentException(
                "This component requires internal features of Kafka Streams and must be disabled for unit tests."
            );
        }
    }
}

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

import com.yammer.metrics.core.MetricsRegistry;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * This class encapsulates the default yammer metrics registry for Kafka server,
 * and configures the set of exported JMX metrics for Yammer metrics.
 *
 * KafkaYammerMetrics.defaultRegistry() should always be used instead of Metrics.defaultRegistry()
 */
public class KafkaYammerMetrics implements Reconfigurable {

    public static final KafkaYammerMetrics INSTANCE = new KafkaYammerMetrics();

    /**
     * convenience method to replace {@link com.yammer.metrics.Metrics#defaultRegistry()}
     */
    public static MetricsRegistry defaultRegistry() {
        return INSTANCE.metricsRegistry;
    }

    private final MetricsRegistry metricsRegistry = new MetricsRegistry();
    private final FilteringJmxReporter jmxReporter = new FilteringJmxReporter(metricsRegistry,
        metricName -> true);

    private KafkaYammerMetrics() {
        jmxReporter.start();
        Runtime.getRuntime().addShutdownHook(new Thread(jmxReporter::shutdown));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        reconfigure(configs);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return JmxReporter.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        JmxReporter.compilePredicate(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        Predicate<String> mBeanPredicate = JmxReporter.compilePredicate(configs);
        jmxReporter.updatePredicate(metricName -> mBeanPredicate.test(metricName.getMBeanName()));
    }
}

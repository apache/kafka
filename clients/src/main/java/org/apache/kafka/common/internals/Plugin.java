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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Plugin<T> implements Supplier<T>, AutoCloseable {

    private final T instance;
    private final Optional<PluginMetricsImpl> pluginMetrics;

    private Plugin(T instance, PluginMetricsImpl pluginMetrics) {
        this.instance = instance;
        this.pluginMetrics = Optional.ofNullable(pluginMetrics);
    }

    public static <T> Plugin<T> wrapInstance(T instance, Metrics metrics, String key) {
        return wrapInstance(instance, metrics, () -> tags(key, instance));
    }

    private static <T> Map<String, String> tags(String key, T instance) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("config", key);
        tags.put("class", instance.getClass().getSimpleName());
        return tags;
    }

    public static <T> List<Plugin<T>> wrapInstances(List<T> instances, Metrics metrics, String key) {
        List<Plugin<T>> plugins = new ArrayList<>();
        for (T instance : instances) {
            plugins.add(wrapInstance(instance, metrics, key));
        }
        return plugins;
    }

    public static <T> Plugin<T> wrapInstance(T instance, Metrics metrics, Supplier<Map<String, String>> tagsSupplier) {
        PluginMetricsImpl pluginMetrics = null;
        if (instance instanceof Monitorable && metrics != null) {
            pluginMetrics = new PluginMetricsImpl(metrics, tagsSupplier.get());
            ((Monitorable) instance).withPluginMetrics(pluginMetrics);
        }
        return new Plugin<>(instance, pluginMetrics);
    }

    @Override
    public T get() {
        return instance;
    }

    @Override
    public void close() throws Exception {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        if (instance instanceof AutoCloseable) {
            Utils.closeQuietly((AutoCloseable) instance, instance.getClass().getSimpleName(), firstException);
        }
        pluginMetrics.ifPresent(metrics -> Utils.closeQuietly(metrics, "pluginMetrics", firstException));
        Throwable throwable = firstException.get();
        if (throwable != null) throw new KafkaException("failed closing plugin", throwable);
    }
}

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

/**
 * Plugins have the following tags:
 * <ul>
 *   <li><code>config</code> set to the name of the configuration to specifying the plugin</li>
 *   <li><code>class</code> set to the name of the instance class</li>
 * </ul>
 */
public class Plugin<T> implements Supplier<T>, AutoCloseable {

    private final T instance;
    private final Optional<PluginMetricsImpl> pluginMetrics;

    private Plugin(T instance, PluginMetricsImpl pluginMetrics) {
        this.instance = instance;
        this.pluginMetrics = Optional.ofNullable(pluginMetrics);
    }

    /**
     * Wrap an instance into a Plugin.
     * @param instance the instance to wrap
     * @param metrics the metrics
     * @param tagsSupplier supplier to retrieve the tags
     * @return the plugin
     */
    public static <T> Plugin<T> wrapInstance(T instance, Metrics metrics, Supplier<Map<String, String>> tagsSupplier) {
        PluginMetricsImpl pluginMetrics = null;
        if (instance instanceof Monitorable && metrics != null) {
            pluginMetrics = new PluginMetricsImpl(metrics, tagsSupplier.get());
            ((Monitorable) instance).withPluginMetrics(pluginMetrics);
        }
        return new Plugin<>(instance, pluginMetrics);
    }

    /**
     * Wrap an instance into a Plugin.
     * @param instance the instance to wrap
     * @param metrics the metrics
     * @param key the value for the <code>config</code> tag
     * @return the plugin
     */
    public static <T> Plugin<T> wrapInstance(T instance, Metrics metrics, String key) {
        return wrapInstance(instance, metrics, () -> tags(key, instance));
    }

    /**
     * Wrap an instance into a Plugin.
     * @param instance the instance to wrap
     * @param metrics the metrics
     * @param name extra tag name to add
     * @param value extra tag value to add
     * @param key the value for the <code>config</code> tag
     * @return the plugin
     */
    public static <T> Plugin<T> wrapInstance(T instance, Metrics metrics, String key, String name, String value) {
        Supplier<Map<String, String>> tagsSupplier = () -> {
            Map<String, String> tags = tags(key, instance);
            tags.put(name, value);
            return tags;
        };
        return wrapInstance(instance, metrics, tagsSupplier);
    }

    private static <T> Map<String, String> tags(String key, T instance) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("config", key);
        tags.put("class", instance.getClass().getSimpleName());
        return tags;
    }

    /**
     * Wrap a list of instances into Plugins.
     * @param instances the instances to wrap
     * @param metrics the metrics
     * @param key the value for the <code>config</code> tag
     * @return the list of plugins
     */
    public static <T> List<Plugin<T>> wrapInstances(List<T> instances, Metrics metrics, String key) {
        List<Plugin<T>> plugins = new ArrayList<>();
        for (T instance : instances) {
            plugins.add(wrapInstance(instance, metrics, key));
        }
        return plugins;
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

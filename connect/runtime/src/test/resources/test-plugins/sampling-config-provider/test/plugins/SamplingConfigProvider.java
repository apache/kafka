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

package test.plugins;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigChangeCallback;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.runtime.isolation.SamplingTestPlugin;
import org.apache.kafka.connect.storage.HeaderConverter;

/**
 * Fake plugin class for testing classloading isolation.
 * See {@link org.apache.kafka.connect.runtime.isolation.TestPlugins}.
 * <p>Samples data about its initialization environment for later analysis.
 */
public final class SamplingConfigProvider implements SamplingTestPlugin, ConfigProvider {

  private static final ClassLoader STATIC_CLASS_LOADER;
  private static List<SamplingTestPlugin> instances;
  private final ClassLoader classloader;
  private Map<String, SamplingTestPlugin> samples;

  static {
    STATIC_CLASS_LOADER = Thread.currentThread().getContextClassLoader();
    instances = Collections.synchronizedList(new ArrayList<>());
  }

  {
    samples = new HashMap<>();
    classloader = Thread.currentThread().getContextClassLoader();
  }

  @Override
  public ConfigData get(String path) {
    logMethodCall(samples);
    return null;
  }

  @Override
  public ConfigData get(String path, Set<String> keys) {
    logMethodCall(samples);
    return null;
  }

  public SamplingConfigProvider() {
    logMethodCall(samples);
    instances.add(this);
  }

  @Override
  public void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
    logMethodCall(samples);
  }

  @Override
  public void unsubscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
    logMethodCall(samples);
  }

  @Override
  public void unsubscribeAll() {
    logMethodCall(samples);
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    logMethodCall(samples);
  }

  @Override
  public void close() {
    logMethodCall(samples);
  }

  @Override
  public ClassLoader staticClassloader() {
    return STATIC_CLASS_LOADER;
  }

  @Override
  public ClassLoader classloader() {
    return classloader;
  }

  @Override
  public Map<String, SamplingTestPlugin> otherSamples() {
    return samples;
  }


  @Override
  public List<SamplingTestPlugin> allInstances() {
    return instances;
  }
}

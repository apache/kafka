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

import java.util.Map;
import java.util.HashMap;
import java.util.ServiceLoader;
import java.util.Iterator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.runtime.isolation.SamplingTestPlugin;

/**
 * Fake plugin class for testing classloading isolation.
 * See {@link org.apache.kafka.connect.runtime.isolation.TestPlugins}.
 * <p>Samples data about its initialization environment for later analysis.
 */
public class ServiceLoaderPlugin implements SamplingTestPlugin, Converter {

  private static final ClassLoader STATIC_CLASS_LOADER;
  private static final Map<String, SamplingTestPlugin> SAMPLES;
  private final ClassLoader classloader;

  static {
    STATIC_CLASS_LOADER = Thread.currentThread().getContextClassLoader();
    SAMPLES = new HashMap<>();
    Iterator<ServiceLoadedClass> it = ServiceLoader.load(ServiceLoadedClass.class).iterator();
    while (it.hasNext()) {
      ServiceLoadedClass loaded = it.next();
      SAMPLES.put(loaded.getClass().getSimpleName() + ".static", loaded);
    }
  }

  {
    classloader = Thread.currentThread().getContextClassLoader();
    Iterator<ServiceLoadedClass> it = ServiceLoader.load(ServiceLoadedClass.class).iterator();
    while (it.hasNext()) {
      ServiceLoadedClass loaded = it.next();
      SAMPLES.put(loaded.getClass().getSimpleName() + ".dynamic", loaded);
    }
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
  }

  @Override
  public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
    return new byte[0];
  }

  @Override
  public SchemaAndValue toConnectData(final String topic, final byte[] value) {
    return null;
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
    return SAMPLES;
  }
}

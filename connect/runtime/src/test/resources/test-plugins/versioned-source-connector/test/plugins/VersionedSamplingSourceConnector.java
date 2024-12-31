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

import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.isolation.SamplingTestPlugin;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;

/**
 * VersionedSamplingSourceConnector is a test connector that extends SamplingConnector and overrides the version method.
 * Any instance of the string PLACEHOLDER_FOR_VERSION will be replaced with the actual version during plugin compilation.
 */
public class VersionedSamplingSourceConnector extends SourceConnector implements SamplingTestPlugin {

    protected static final ClassLoader STATIC_CLASS_LOADER;
    protected static List<SamplingTestPlugin> instances;
    protected final ClassLoader classloader;
    protected Map<String, SamplingTestPlugin> samples;

    static {
        STATIC_CLASS_LOADER = Thread.currentThread().getContextClassLoader();
        instances = Collections.synchronizedList(new ArrayList<>());
    }

    {
        samples = new HashMap<>();
        classloader = Thread.currentThread().getContextClassLoader();
    }

    public VersionedSamplingSourceConnector() {
        logMethodCall(samples);
        instances.add(this);
    }

    @Override
    public void start(Map<String, String> props) {
        logMethodCall(samples);
    }

    @Override
    public Class<? extends Task> taskClass() {
        logMethodCall(samples);
        return VersionedSamplingSourceConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logMethodCall(samples);
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(Collections.singletonMap("task-config-version", "PLACEHOLDER_FOR_VERSION"));
        }
        return configs;
    }

    @Override
    public void stop() {
        logMethodCall(samples);
    }

    @Override
    public ConfigDef config() {
        logMethodCall(samples);
        return new ConfigDef()
                // version specific config will have the defaul value (PLACEHOLDER_FOR_VERSION) replaced with the actual version during plugin compilation
                // this will help with testing differnt configdef for different version of connector
                .define("version-specific-config", ConfigDef.Type.STRING, "PLACEHOLDER_FOR_VERSION", ConfigDef.Importance.HIGH, "version specific docs")
                .define("other-config", ConfigDef.Type.STRING, "defaultVal", ConfigDef.Importance.HIGH, "other docs");
    }

    @Override
    public String version() {
        logMethodCall(samples);
        return "PLACEHOLDER_FOR_VERSION";
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

    public static class VersionedSamplingSourceConnectorTask extends SourceTask {

        @Override
        public String version() {
            return "PLACEHOLDER_FOR_VERSION";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public void put(Collection<SinkRecord> records) {
        }

        @Override
        public void stop() {
        }
    }
}

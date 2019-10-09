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

package org.apache.kafka.connect.runtime.isolation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Mix-in interface for plugins so we can sample information about their initialization
 */
public interface SamplingTestPlugin {

    /**
     * @return the ClassLoader used to statically initialize this plugin class
     */
    ClassLoader staticClassloader();

    /**
     * @return the ClassLoader used to initialize this plugin instance
     */
    ClassLoader classloader();

    /**
     * @return a group of other SamplingTestPlugin instances known by this plugin
     * This should only return direct children, and not reference this instance directly
     */
    default Map<String, SamplingTestPlugin> otherSamples() {
        return Collections.emptyMap();
    }

    /**
     * @return a flattened list of child samples including this entry keyed as "this"
     */
    default Map<String, SamplingTestPlugin> flatten() {
        Map<String, SamplingTestPlugin> out = new HashMap<>();
        Map<String, SamplingTestPlugin> otherSamples = otherSamples();
        if (otherSamples != null) {
            for (Entry<String, SamplingTestPlugin> child : otherSamples.entrySet()) {
                for (Entry<String, SamplingTestPlugin> flattened : child.getValue().flatten().entrySet()) {
                    String key = child.getKey();
                    if (flattened.getKey().length() > 0) {
                        key += "." + flattened.getKey();
                    }
                    out.put(key, flattened.getValue());
                }
            }
        }
        out.put("", this);
        return out;
    }

    /**
     * Log the parent method call as a child sample.
     * Stores only the last invocation of each method if there are multiple invocations.
     * @param samples The collection of samples to which this method call should be added
     */
    default void logMethodCall(Map<String, SamplingTestPlugin> samples) {
        String methodName = "unknown";
        StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
        // 0 is inside getStackTrace
        // 1 is this method
        // 2 is our caller method
        if (stackTraces.length >= 2) {
            StackTraceElement caller = stackTraces[2];
            methodName = caller.getMethodName();
        }

        ClassLoader staticClassloader = getClass().getClassLoader();
        ClassLoader dynamicClassloader = Thread.currentThread().getContextClassLoader();
        samples.put(methodName, new SamplingTestPlugin() {
            @Override
            public ClassLoader staticClassloader() {
                return staticClassloader;
            }

            @Override
            public ClassLoader classloader() {
                return dynamicClassloader;
            }
        });
    }
}

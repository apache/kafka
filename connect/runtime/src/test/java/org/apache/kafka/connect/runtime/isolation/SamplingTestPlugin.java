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
import java.util.Map;

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
     * @return number of instances of this plugin class that have been initialized
     */
    int dynamicInitializations();

    /**
     * @return a group of other SamplingTestPlugin instances known by this plugin
     */
    default Map<String, SamplingTestPlugin> otherSamples() {
        return Collections.emptyMap();
    }
}

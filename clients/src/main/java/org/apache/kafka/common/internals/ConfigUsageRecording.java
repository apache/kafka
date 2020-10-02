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

import java.util.HashMap;
import java.util.Map;

/**
 * AbstractConfig tracks and can log warnings about non-use of config parameters.
 * In places, copies of the results of methods like AbstractConfig.values() get made
 * and then mutated, but use of config parameters from those copies don't record use.
 * This class provides an API for creating copies of the RecordingMaps returned by
 * AbstractConfig.values() which will cause use to be tracked, but without needing
 * a public API.
 */
public class ConfigUsageRecording extends HashMap<String, Object> {

    private final Map<? extends String, ? extends Object> tracking;

    private ConfigUsageRecording(Map<? extends String, ? extends Object> tracking, Map<? extends String, ? extends Object> copy) {
        super(copy);
        this.tracking = tracking;
    }

    public static ConfigUsageRecording copyOf(Map<? extends String, ? extends Object> configs) {
        return new ConfigUsageRecording(configs, configs);
    }

    @Override
    public Object get(Object key) {
        // Intentionally ignore the result; call just to mark the original entry as used
        tracking.get(key);
        return super.get(key);
    }
}

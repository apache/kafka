/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.standalone;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;

import java.util.Map;

public class StandaloneConfig extends WorkerConfig {
    private static final ConfigDef CONFIG;

    /**
     * <code>offset.storage.file.filename</code>
     */
    public static final String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "offset.storage.file.filename";
    private static final String OFFSET_STORAGE_FILE_FILENAME_DOC = "file to store offset data in";

    static {
        CONFIG = baseConfigDef()
                .define(OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        OFFSET_STORAGE_FILE_FILENAME_DOC);
    }

    public StandaloneConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}

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
package org.apache.kafka.streams.internals;

import org.apache.kafka.streams.StreamsConfig;

public class StreamsConfigUtils {

    public enum ProcessingMode {
        AT_LEAST_ONCE("AT_LEAST_ONCE"),

        EXACTLY_ONCE_ALPHA("EXACTLY_ONCE_ALPHA"),

        EXACTLY_ONCE_V2("EXACTLY_ONCE_V2");

        public final String name;

        ProcessingMode(final String name) {
            this.name = name;
        }
    }
    
    @SuppressWarnings("deprecation")
    public static ProcessingMode processingMode(final StreamsConfig config) {
        if (StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
            return ProcessingMode.EXACTLY_ONCE_ALPHA;
        } else if (StreamsConfig.EXACTLY_ONCE_BETA.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
            return ProcessingMode.EXACTLY_ONCE_V2;
        } else if (StreamsConfig.EXACTLY_ONCE_V2.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
            return ProcessingMode.EXACTLY_ONCE_V2;
        } else {
            return ProcessingMode.AT_LEAST_ONCE;
        }
    }

    @SuppressWarnings("deprecation")
    public static String processingModeString(final ProcessingMode processingMode) {
        if (processingMode == ProcessingMode.EXACTLY_ONCE_V2) {
            return StreamsConfig.EXACTLY_ONCE_V2;
        } else if (processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA) {
            return StreamsConfig.EXACTLY_ONCE;
        } else {
            return StreamsConfig.AT_LEAST_ONCE;
        }
    }

    public static boolean eosEnabled(final StreamsConfig config) {
        return eosEnabled(processingMode(config));
    }

    public static boolean eosEnabled(final ProcessingMode processingMode) {
        return processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA ||
            processingMode == ProcessingMode.EXACTLY_ONCE_V2;
    }
}

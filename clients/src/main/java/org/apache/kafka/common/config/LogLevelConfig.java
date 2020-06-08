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

package org.apache.kafka.common.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class holds definitions for log level configurations related to Kafka's application logging. See KIP-412 for additional information
 */
public class LogLevelConfig {
    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * The <code>FATAL</code> level designates a very severe error
     * that will lead the Kafka broker to abort.
     */
    public static final String FATAL_LOG_LEVEL = "FATAL";

    /**
     * The <code>ERROR</code> level designates error events that
     * might still allow the broker to continue running.
     */
    public static final String ERROR_LOG_LEVEL = "ERROR";

    /**
     * The <code>WARN</code> level designates potentially harmful situations.
     */
    public static final String WARN_LOG_LEVEL = "WARN";

    /**
     * The <code>INFO</code> level designates informational messages
     * that highlight normal Kafka events at a coarse-grained level
     */
    public static final String INFO_LOG_LEVEL = "INFO";

    /**
     * The <code>DEBUG</code> level designates fine-grained
     * informational events that are most useful to debug Kafka
     */
    public static final String DEBUG_LOG_LEVEL = "DEBUG";

    /**
     * The <code>TRACE</code> level designates finer-grained
     * informational events than the <code>DEBUG</code> level.
     */
    public static final String TRACE_LOG_LEVEL = "TRACE";

    public static final Set<String> VALID_LOG_LEVELS = new HashSet<>(Arrays.asList(
            FATAL_LOG_LEVEL, ERROR_LOG_LEVEL, WARN_LOG_LEVEL,
            INFO_LOG_LEVEL, DEBUG_LOG_LEVEL, TRACE_LOG_LEVEL
    ));
}

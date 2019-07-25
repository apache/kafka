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
package org.apache.kafka.common.logging;

import java.util.List;

/**
 * An MBean that allows the user to dynamically alter log4j levels at runtime.
 */
public interface Log4jControllerMBean {

    /**
     * @return a list of all registered loggers
     */
    List<String> getLoggers();

    /**
     * get the log level of a logger given its name.
     * @param logger name of the logger whose logging level is desired
     * @return the logging level (the effective level is returned if the immediate level of this logger is not set).
     */
    String getLogLevel(String logger);

    /**
     * set the log level of a logger given its name.
     * @param logger name of the logger
     * @param level the level to be set
     * @return true, if level was successfully set; false otherwise.
     */
    Boolean setLogLevel(String logger, String level);

}

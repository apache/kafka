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
package org.apache.kafka.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple class that sets logIdent appropriately depending on whether the state change logger is being used in the
 * context of the broker (e.g. ReplicaManager and Partition).
 */
public class StateChangeLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger("state.change.logger");

    private final String logIdent;

    public StateChangeLogger(int brokerId) {
        this.logIdent = String.format("[Broker id=%d] ", brokerId);
    }

    public void trace(String message) {
        LOGGER.trace("{}{}", logIdent, message);
    }

    public void info(String message) {
        LOGGER.info("{}{}", logIdent, message);
    }

    public void warn(String message) {
        LOGGER.warn("{}{}", logIdent, message);
    }

    public void error(String message) {
        LOGGER.error("{}{}", logIdent, message);
    }

    public void error(String message, Throwable e) {
        LOGGER.error("{}{}", logIdent, message, e);
    }
}

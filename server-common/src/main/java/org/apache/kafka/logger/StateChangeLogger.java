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

public class StateChangeLogger {
    private static final Logger log = LoggerFactory.getLogger("state.change.logger");

    private final String logIdent;

    /**
     * Simple class that sets logIdent appropriately depending on whether the state change logger is being used in the
     * context of the broker (e.g. ReplicaManager and Partition).
     */
    public StateChangeLogger(int brokerId) {
        this.logIdent = String.format("[Broker id=%d] ", brokerId);
    }

    public void trace(String message) {
        log.info("{}{}", logIdent, message);
    }

    public void info(String message) {
        log.info("{}{}", logIdent, message);
    }

    public void warn(String message) {
        log.warn("{}{}", logIdent, message);
    }

    public void error(String message) {
        log.error("{}{}", logIdent, message);
    }

    public void error(String message, Throwable e) {
        log.error("{}{}", logIdent, message, e);
    }
}

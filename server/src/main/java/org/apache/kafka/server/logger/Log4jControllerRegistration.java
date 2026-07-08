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
package org.apache.kafka.server.logger;

import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers the {@link LoggingController} MBean, which lets users dynamically alter log4j levels
 * at runtime. The MBean is registered only once, on the first call to {@link #register()}.
 */
public final class Log4jControllerRegistration {

    private static final Logger LOGGER = LoggerFactory.getLogger(Log4jControllerRegistration.class);

    private static final AtomicBoolean REGISTERED = new AtomicBoolean(false);

    private static final String MBEAN_TYPE = "kafka.Log4jController";
    private static final String MBEAN_NAME = "kafka:type=" + MBEAN_TYPE;

    private Log4jControllerRegistration() {
    }

    public static void register() {
        if (REGISTERED.compareAndSet(false, true)) {
            if (Utils.registerMBean(new LoggingController(), MBEAN_NAME)) {
                LOGGER.info("Registered `{}` MBean", MBEAN_NAME);
            }
        }
    }
}
